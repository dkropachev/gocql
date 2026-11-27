// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// identityNode is a test node: each connection it accepts is served TLS with
// cert while tls is set, and otherwise answered as a plaintext CQL node
// answers a TLS ClientHello.
type identityNode struct {
	listener net.Listener
	sni      chan string
	cert     tls.Certificate
	accepted atomic.Int32
	tls      atomic.Bool
}

func newIdentityNode(t *testing.T, cert tls.Certificate) *identityNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	n := &identityNode{listener: listener, cert: cert, sni: make(chan string, 16)}
	n.tls.Store(true)
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			n.accepted.Add(1)
			go n.serve(raw)
		}
	}()
	return n
}

// waitAccepted waits until n has accepted want connections and returns how
// many it has: a dial returns before the node's Accept does.
func (n *identityNode) waitAccepted(want int32) int32 {
	for deadline := time.Now().Add(time.Second); n.accepted.Load() < want && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	return n.accepted.Load()
}

func (n *identityNode) serve(raw net.Conn) {
	defer raw.Close()
	raw.SetDeadline(time.Now().Add(5 * time.Second))
	if !n.tls.Load() {
		var b [1]byte
		if _, err := io.ReadFull(raw, b[:]); err != nil || b[0] != 0x16 {
			io.Copy(io.Discard, raw) // a plaintext dial: keep it open
			return
		}
		raw.Write([]byte{0x84, 0, 0, 0, 0, 0, 0, 0, 0})
		return
	}
	server := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{n.cert}, GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		select {
		case n.sni <- hello.ServerName:
		default:
		}
		return nil, nil
	}})
	if server.Handshake() == nil {
		io.Copy(io.Discard, server)
	}
}

// dialer returns a dialer of a TLS session trusting clientTLS, set up by
// configure.
func identityDialer(clientTLS *tls.Config, configure func(*settings.Config)) *transport.InternalV2TLSHostDialer {
	config := settings.DefaultConfig("127.0.0.1")
	config.TLS = clientTLS
	if configure != nil {
		configure(&config)
	}
	return &transport.InternalV2TLSHostDialer{InternalConfig: config.Connectivity()}
}

func dialWith(t *testing.T, d *transport.InternalV2TLSHostDialer, host *implEndpoint.HostInfo) (net.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialed, err := d.DialHost(ctx, host)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { dialed.Conn.Close() })
	return dialed.Conn, nil
}

// TLSIdentity separates the SNI from the verified name, verifies the chain
// alone, and runs its own check, over the session's one TLS configuration.
func TestTLSIdentity(t *testing.T) {
	cert, clientTLS := tlsServerCert(t, []string{"node1.test"}, nil)
	t.Run("SNIAndVerifiedNameDiffer", func(t *testing.T) {
		node := newIdentityNode(t, cert)
		var target endpoint.DialTarget
		d := identityDialer(clientTLS, func(c *settings.Config) {
			c.TLSIdentity = func(dt endpoint.DialTarget) endpoint.TLSIdentity {
				target = dt
				return endpoint.TLSIdentity{ServerName: "route-1.proxy.test", VerifyName: "node1.test"}
			}
		})
		if _, err := dialWith(t, d, tlsTestHost(t, node.listener.Addr(), "")); err != nil {
			t.Fatal(err)
		}
		if sni := <-node.sni; sni != "route-1.proxy.test" {
			t.Fatalf("SNI %q, want route-1.proxy.test", sni)
		}
		if target.Host.HostID != (cql.UUID{15: 1}) || target.Address != node.listener.Addr().String() {
			t.Fatalf("identity target %+v", target)
		}
	})
	t.Run("VerifiedNameMismatch", func(t *testing.T) {
		node := newIdentityNode(t, cert)
		d := identityDialer(clientTLS, func(c *settings.Config) {
			c.TLSIdentity = func(endpoint.DialTarget) endpoint.TLSIdentity {
				return endpoint.TLSIdentity{ServerName: "node1.test", VerifyName: "node2.test"}
			}
		})
		_, err := dialWith(t, d, tlsTestHost(t, node.listener.Addr(), ""))
		var hostnameErr x509.HostnameError
		if !errors.As(err, &hostnameErr) {
			t.Fatalf("dial = %v, want x509.HostnameError", err)
		}
	})
	t.Run("ChainOnly", func(t *testing.T) {
		node := newIdentityNode(t, cert)
		host := tlsTestHost(t, node.listener.Addr(), "") // dialed by an IP the certificate does not name
		if _, err := dialTLS(t, host, clientTLS); err == nil {
			t.Fatal("default verification accepted a certificate without the node's IP")
		}
		d := identityDialer(clientTLS, func(c *settings.Config) {
			c.TLSIdentity = func(endpoint.DialTarget) endpoint.TLSIdentity { return endpoint.TLSIdentity{ChainOnly: true} }
		})
		if _, err := dialWith(t, d, host); err != nil {
			t.Fatal(err)
		}
		untrusted, _ := tlsServerCert(t, []string{"node1.test"}, nil)
		other := newIdentityNode(t, untrusted)
		_, err := dialWith(t, d, tlsTestHost(t, other.listener.Addr(), ""))
		var unknown x509.UnknownAuthorityError
		if !errors.As(err, &unknown) {
			t.Fatalf("chain-only dial to an untrusted node = %v, want x509.UnknownAuthorityError", err)
		}
	})
	t.Run("Verify", func(t *testing.T) {
		node := newIdentityNode(t, cert)
		refuse := errors.New("not this node")
		var chains atomic.Int32
		for _, ok := range []bool{true, false} {
			d := identityDialer(clientTLS, func(c *settings.Config) {
				c.TLSIdentity = func(endpoint.DialTarget) endpoint.TLSIdentity {
					return endpoint.TLSIdentity{ServerName: "node1.test", Verify: func(state tls.ConnectionState) error {
						chains.Store(int32(len(state.VerifiedChains)))
						if ok {
							return nil
						}
						return refuse
					}}
				}
			})
			_, err := dialWith(t, d, tlsTestHost(t, node.listener.Addr(), ""))
			if ok != (err == nil) || !ok && !errors.Is(err, refuse) {
				t.Fatalf("Verify returning ok=%v: dial = %v", ok, err)
			}
			if chains.Load() == 0 {
				t.Fatal("Verify saw no verified chains")
			}
		}
	})
}

// recordingCache records the keys of a session cache.
type recordingCache struct {
	tls.ClientSessionCache
	keys []string
	mu   sync.Mutex
}

func (c *recordingCache) Get(key string) (*tls.ClientSessionState, bool) {
	c.mu.Lock()
	c.keys = append(c.keys, key)
	c.mu.Unlock()
	return c.ClientSessionCache.Get(key)
}

// Nodes that share an SNI do not share session tickets.
func TestTLSSessionCacheKeyedByHost(t *testing.T) {
	cert, clientTLS := tlsServerCert(t, []string{"cluster.test"}, nil)
	cache := &recordingCache{ClientSessionCache: tls.NewLRUClientSessionCache(0)}
	config := clientTLS.Clone()
	config.ServerName = "cluster.test"
	config.ClientSessionCache = cache
	d := identityDialer(config, nil)
	for i := range 2 {
		node := newIdentityNode(t, cert)
		host := tlsTestHost(t, node.listener.Addr(), "")
		implEndpoint.HostInfoData(host).InternalHostId = cql.UUID{15: byte(i + 1)}
		if _, err := dialWith(t, d, host); err != nil {
			t.Fatal(err)
		}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.keys) != 2 || cache.keys[0] == cache.keys[1] || !strings.HasPrefix(cache.keys[0], (cql.UUID{15: 1}).String()) {
		t.Fatalf("session cache keys %q, want one per node", cache.keys)
	}
}

// WithTLSFallback reaches a node that does not speak TLS yet in plaintext,
// until it has once answered TLS; a verification failure never falls back.
func TestTLSFallback(t *testing.T) {
	cert, clientTLS := tlsServerCert(t, nil, []net.IP{net.IPv4(127, 0, 0, 1)})
	isTLS := func(c net.Conn) bool { _, ok := c.(*tls.Conn); return ok }
	t.Run("OneWayPerNode", func(t *testing.T) {
		node := newIdentityNode(t, cert)
		host := tlsTestHost(t, node.listener.Addr(), "")
		d := identityDialer(clientTLS, func(c *settings.Config) { c.TLSFallback = true })
		node.tls.Store(false)
		c, err := dialWith(t, d, host)
		if got := node.waitAccepted(2); err != nil || isTLS(c) || got != 2 {
			t.Fatalf("dial to a plaintext node: %v (%d connections), want a plaintext fallback", err, got)
		}
		node.tls.Store(true)
		if c, err = dialWith(t, d, host); err != nil || !isTLS(c) {
			t.Fatalf("dial to the node with TLS on: %v, want TLS", err)
		}
		node.tls.Store(false)
		before := node.waitAccepted(3)
		if _, err = dialWith(t, d, host); err == nil {
			t.Fatal("a node that answered TLS was dialed in plaintext again")
		}
		time.Sleep(50 * time.Millisecond)
		if n := node.accepted.Load() - before; n != 1 {
			t.Fatalf("%d connections, want only the TLS one", n)
		}
	})
	t.Run("SessionWide", func(t *testing.T) {
		first, second := newIdentityNode(t, cert), newIdentityNode(t, cert)
		d := identityDialer(clientTLS, func(c *settings.Config) { c.TLSFallback = true })
		d.InternalKnownHosts = func() []cql.UUID { return []cql.UUID{{15: 1}} }
		if _, err := dialWith(t, d, tlsTestHost(t, first.listener.Addr(), "")); err != nil {
			t.Fatal(err)
		}
		// Every known node answered TLS: a new one gets no fallback.
		second.tls.Store(false)
		host := tlsTestHost(t, second.listener.Addr(), "")
		implEndpoint.HostInfoData(host).InternalHostId = cql.UUID{15: 2}
		if _, err := dialWith(t, d, host); err == nil {
			t.Fatal("fell back to plaintext after every known node answered TLS")
		}
	})
	t.Run("NotOnVerificationFailure", func(t *testing.T) {
		untrusted, _ := tlsServerCert(t, nil, []net.IP{net.IPv4(127, 0, 0, 1)})
		node := newIdentityNode(t, untrusted)
		d := identityDialer(clientTLS, func(c *settings.Config) { c.TLSFallback = true })
		if _, err := dialWith(t, d, tlsTestHost(t, node.listener.Addr(), "")); err == nil {
			t.Fatal("dial to an untrusted node succeeded")
		}
		time.Sleep(50 * time.Millisecond)
		if n := node.accepted.Load(); n != 1 {
			t.Fatalf("%d connections after a verification failure, want 1", n)
		}
	})
	t.Run("TLSPort", func(t *testing.T) {
		plain := newIdentityNode(t, cert)
		plain.tls.Store(false)
		secure := newIdentityNode(t, cert)
		host := tlsTestHost(t, plain.listener.Addr(), "")
		tlsPort := uint16(secure.listener.Addr().(*net.TCPAddr).Port)
		d := identityDialer(clientTLS, func(c *settings.Config) { c.TLSPort, c.TLSFallback = tlsPort, true })
		if c, err := dialWith(t, d, host); err != nil || !isTLS(c) || secure.waitAccepted(1) != 1 || plain.accepted.Load() != 0 {
			t.Fatalf("dial = %v, want TLS on the TLS port", err)
		}
		// A node whose TLS port is closed is reached in plaintext on its
		// CQL port.
		closed := newIdentityNode(t, cert)
		closedPort := uint16(closed.listener.Addr().(*net.TCPAddr).Port)
		closed.listener.Close()
		other := newIdentityNode(t, cert)
		other.tls.Store(false)
		otherHost := tlsTestHost(t, other.listener.Addr(), "")
		implEndpoint.HostInfoData(otherHost).InternalHostId = cql.UUID{15: 2}
		d = identityDialer(clientTLS, func(c *settings.Config) { c.TLSPort, c.TLSFallback = closedPort, true })
		if c, err := dialWith(t, d, otherHost); err != nil || isTLS(c) {
			t.Fatalf("dial = %v, want a plaintext fallback to the CQL port", err)
		}
	})
}
