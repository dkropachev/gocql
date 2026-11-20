// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
)

const testShards = 4

// sourcePorts returns a source-port range for advanced shard awareness
// above the usual ephemeral range of Linux, where other connections do not
// take ports, and that other test processes are unlikely to use.
func sourcePorts() (start, end uint16) {
	start = uint16(61000 + os.Getpid()%16*256)
	return start, start + 255
}

// connect opens a session to n with a quiet, metadata-free configuration.
func connect(t *testing.T, n *fakeNode, configure func(*settings.Config)) *session.Session {
	t.Helper()
	config := settings.DefaultConfig(n.contact())
	config.Protocol = 4
	config.Metadata.Mode = metadata.SchemaNone
	config.Metadata.InitialDiscovery = false
	config.Metadata.AutomaticAgreement = false
	config.Logger = logging.Nop()
	config.Pool.AdvancedShardAwareness.SourcePortStart, config.Pool.AdvancedShardAwareness.SourcePortEnd = sourcePorts()
	if configure != nil {
		configure(&config)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := session.Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// poolSize returns the number of connections in the session's pools.
func poolSize(s *session.Session) int {
	p := implCluster.SessionData(s.Core()).InternalPool
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	n := 0
	for _, hp := range p.InternalHostConnPools {
		n += hp.Size()
	}
	return n
}

// waitFor polls cond until it returns "" or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		msg := cond()
		if msg == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitFull waits until the session holds one connection to each of the
// node's shards and the node sees exactly those besides the control
// connection, then returns them.
func waitFull(t *testing.T, s *session.Session, n *fakeNode) []nodeConn {
	t.Helper()
	var conns []nodeConn
	waitFor(t, 10*time.Second, func() string {
		conns = n.poolConns()
		if size := poolSize(s); size != n.shards || len(conns) != n.shards {
			return fmt.Sprintf("pool holds %d connections and the node sees %d, want %d", size, len(conns), n.shards)
		}
		var shards []int
		for _, c := range conns {
			shards = append(shards, c.shard)
		}
		slices.Sort(shards)
		for i, shard := range shards {
			if shard != i {
				return fmt.Sprintf("pool connections are on shards %v, want one on each of %d", shards, n.shards)
			}
		}
		return ""
	})
	return conns
}

func TestShardAwarePool(t *testing.T) {
	t.Run("OneConnectionPerShard", func(t *testing.T) {
		n := newFakeNode(t, testShards, nil, shardAwarePort)
		s := connect(t, n, nil)
		conns := waitFull(t, s, n)
		start, end := sourcePorts()
		for _, c := range conns {
			if c.kind != shardAwarePort {
				continue
			}
			if c.sourcePort < int(start) || c.sourcePort > int(end) {
				t.Errorf("shard %d connection from source port %d, outside [%d, %d]", c.shard, c.sourcePort, start, end)
			}
		}
		// The first connection, made before the pool knows the shards,
		// takes the regular port, as does the control connection; every
		// other one is aimed at its shard and lands there the first time.
		if got := n.acceptedOn(regularPort); got != 2 {
			t.Errorf("regular port accepted %d connections, want 2", got)
		}
		if got := n.acceptedOn(shardAwarePort); got != testShards-1 {
			t.Errorf("shard-aware port accepted %d connections, want %d", got, testShards-1)
		}
	})

	t.Run("SourcePortInUse", func(t *testing.T) {
		// Hold the first source port of every shard: each dial must get
		// EADDRINUSE there and move on to a later port of the same shard,
		// not give up on the shard-aware port. A window of its own: ports
		// an earlier test used may still be held by sockets in TIME_WAIT.
		start, end := sourcePorts()
		start += 128
		for port := int(start); port < int(start)+testShards; port++ {
			l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
			if err != nil {
				t.Skipf("cannot hold source port %d: %v", port, err)
			}
			defer l.Close()
		}
		type attempt struct {
			port int
			err  error
		}
		var mu sync.Mutex
		attempts := map[uint32][]attempt{}
		n := newFakeNode(t, testShards, nil, shardAwarePort)
		s := connect(t, n, func(c *settings.Config) {
			c.Pool.AdvancedShardAwareness.SourcePortStart, c.Pool.AdvancedShardAwareness.SourcePortEnd = start, end
			c.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				conn, err := endpoint.DefaultDialer{}.DialContext(ctx, target)
				if target.ShardID.Set {
					mu.Lock()
					attempts[target.ShardID.Value] = append(attempts[target.ShardID.Value], attempt{int(target.LocalAddress.Port()), err})
					mu.Unlock()
				}
				return conn, err
			}
		})
		for _, c := range waitFull(t, s, n) {
			if c.kind != shardAwarePort {
				continue
			}
			mu.Lock()
			tried := attempts[uint32(c.shard)]
			mu.Unlock()
			if len(tried) < 2 || tried[0].port != int(start)+c.shard || !errors.Is(tried[0].err, syscall.EADDRINUSE) {
				t.Errorf("shard %d dials %+v, want the first from held port %d to fail with EADDRINUSE", c.shard, tried, int(start)+c.shard)
				continue
			}
			for i, a := range tried {
				if a.port%testShards != c.shard || i > 0 && a.port <= tried[i-1].port || (a.err == nil) != (i == len(tried)-1) {
					t.Errorf("shard %d dials %+v, want ascending ports of the shard ending in one success", c.shard, tried)
				}
			}
			if last := tried[len(tried)-1]; c.sourcePort != last.port {
				t.Errorf("shard %d connection from source port %d, want %d", c.shard, c.sourcePort, last.port)
			}
		}
		if got := n.acceptedOn(shardAwarePort); got != testShards-1 {
			t.Errorf("shard-aware port accepted %d connections, want %d", got, testShards-1)
		}
	})

	t.Run("Fallback", func(t *testing.T) {
		// Without a usable shard-aware port every connection takes the
		// regular port, where the node picks the shard, and the pool still
		// ends with one connection on each shard.
		for _, tc := range []struct {
			name  string
			serve []listenerKind
		}{
			{"NotAdvertised", nil},
			{"Refused", []listenerKind{shardAwarePort}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				n := newFakeNode(t, testShards, nil, tc.serve...)
				n.closeListener(shardAwarePort)
				var mu sync.Mutex
				targeted := 0
				s := connect(t, n, func(c *settings.Config) {
					c.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
						if target.ShardID.Set {
							mu.Lock()
							targeted++
							mu.Unlock()
						}
						return endpoint.DefaultDialer{}.DialContext(ctx, target)
					}
				})
				for _, c := range waitFull(t, s, n) {
					if c.kind != regularPort {
						t.Errorf("shard %d connection on the %v port", c.shard, c.kind)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				// An advertised port is tried, and every connection still
				// ends on the regular port; one never advertised is not
				// tried at all.
				if refused := tc.serve != nil; refused != (targeted > 0) {
					t.Errorf("%d dials aimed at the shard-aware port", targeted)
				}
			})
		}
	})

	t.Run("TLS", func(t *testing.T) {
		serverTLS, clientTLS := testTLS(t)
		for _, tc := range []struct {
			name  string
			serve []listenerKind
			want  listenerKind
		}{
			// TLS connections take the TLS shard-aware port, never the
			// plaintext one, even when the node advertises both.
			{"ShardAwareTLSPort", []listenerKind{shardAwarePort, shardAwareTLSPort}, shardAwareTLSPort},
			// Without a TLS shard-aware port they take the regular port.
			{"PlaintextPortOnly", []listenerKind{shardAwarePort}, regularPort},
		} {
			t.Run(tc.name, func(t *testing.T) {
				n := newFakeNode(t, testShards, serverTLS, tc.serve...)
				s := connect(t, n, func(c *settings.Config) { c.TLS = clientTLS.Clone() })
				for _, c := range waitFull(t, s, n) {
					if c.kind != tc.want && c.kind != regularPort {
						t.Errorf("shard %d connection on the %v port, want the %v port", c.shard, c.kind, tc.want)
					}
				}
				if got := n.acceptedOn(shardAwarePort); got != 0 {
					t.Errorf("TLS session dialed the plaintext shard-aware port %d times", got)
				}
				if got := n.acceptedOn(tc.want); tc.want == shardAwareTLSPort && got != testShards-1 {
					t.Errorf("TLS shard-aware port accepted %d connections, want %d", got, testShards-1)
				}
			})
		}
	})

	t.Run("AddressTranslator", func(t *testing.T) {
		// The node is known by an address the client cannot reach, nothing
		// listens there; the translator maps it, for the regular and the
		// shard-aware port alike, to where the node listens.
		n := newFakeNode(t, testShards, nil, shardAwarePort)
		n.rpc = net.IPv4(127, 0, 0, 2)
		var mu sync.Mutex
		translated := map[uint16]bool{}
		s := connect(t, n, func(c *settings.Config) {
			c.ContactPoints = []string{fmt.Sprintf("127.0.0.2:%d", n.port(regularPort))}
			c.AddressTranslator = endpoint.TranslatorFunc(func(_ context.Context, _ endpoint.Peer, addr netip.AddrPort) (netip.AddrPort, error) {
				if addr.Addr() != netip.MustParseAddr("127.0.0.2") {
					return addr, nil
				}
				mu.Lock()
				translated[addr.Port()] = true
				mu.Unlock()
				return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), addr.Port()), nil
			})
		})
		waitFull(t, s, n)
		if got := n.acceptedOn(shardAwarePort); got != testShards-1 {
			t.Errorf("shard-aware port accepted %d connections, want %d", got, testShards-1)
		}
		mu.Lock()
		defer mu.Unlock()
		if !translated[uint16(n.port(regularPort))] || !translated[uint16(n.port(shardAwarePort))] {
			t.Errorf("translated ports %v, want the regular port %d and the shard-aware port %d", translated, n.port(regularPort), n.port(shardAwarePort))
		}
	})

	t.Run("ClientRoutes", func(t *testing.T) {
		// The node publishes a client route to a port of its own, as to a
		// proxy that forwards the regular port alone. The pool fills over
		// that route, one connection per shard, and never tries the
		// shard-aware port behind it.
		n := newFakeNode(t, testShards, nil, shardAwarePort, routePort)
		n.route = cql.RandomUUID()
		s := connect(t, n, func(c *settings.Config) {
			c.ClientRoutes = &settings.ClientRoutesOptions{ConnectionIDs: []string{n.route.String()}}
		})
		for _, c := range waitFull(t, s, n) {
			if c.kind != routePort {
				t.Errorf("shard %d connection on the %v port, not through the route", c.shard, c.kind)
			}
		}
		if got := n.acceptedOn(shardAwarePort); got != 0 {
			t.Errorf("shard-aware port behind the route accepted %d connections", got)
		}
	})

	t.Run("SourcePortNAT", func(t *testing.T) {
		// A NAT rewrites source ports, so every connection aimed at a shard
		// lands on another one. The pool drops those and falls back to the
		// regular port, which fills it.
		n := newFakeNode(t, testShards, nil, shardAwarePort)
		n.natShift = 1
		s := connect(t, n, nil)
		for _, c := range waitFull(t, s, n) {
			if c.kind != regularPort {
				t.Errorf("shard %d connection on the %v port after the NAT was detected", c.shard, c.kind)
			}
		}
		if n.acceptedOn(shardAwarePort) == 0 {
			t.Error("the shard-aware port was never dialed, so no NAT was detected")
		}
	})
}

// testTLS returns a server config with a fresh self-signed certificate for
// 127.0.0.1 and a client config that trusts it.
func testTLS(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fake node"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{RootCAs: roots, ServerName: "127.0.0.1"}
}
