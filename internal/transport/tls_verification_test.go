// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// tlsServerCert returns a self-signed server certificate for names and a
// client configuration trusting it, with no ServerName and verification on.
func tlsServerCert(t *testing.T, dnsNames []string, ips []net.IP) (tls.Certificate, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(7), DNSNames: dnsNames, IPAddresses: ips, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

// tlsTestHost describes the node listening at addr, named hostname when it is
// not empty.
func tlsTestHost(t *testing.T, addr net.Addr, hostname string) *implEndpoint.HostInfo {
	t.Helper()
	tcp := addr.(*net.TCPAddr)
	host := new(implEndpoint.HostInfo)
	implEndpoint.HostInfoData(host).InternalConnectAddress = tcp.IP
	implEndpoint.HostInfoData(host).InternalPort = tcp.Port
	implEndpoint.HostInfoData(host).InternalHostId = cql.UUID{15: 1}
	implEndpoint.HostInfoData(host).InternalHostname = hostname
	ap := tcp.AddrPort()
	implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, implEndpoint.InternalTranslatedAddresses{CQL: netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())})
	return host
}

func dialTLS(t *testing.T, host *implEndpoint.HostInfo, clientTLS *tls.Config) (*transport.DialedHost, error) {
	t.Helper()
	config := settings.DefaultConfig("127.0.0.1")
	config.TLS = clientTLS
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return (&transport.InternalV2TLSHostDialer{InternalConfig: config.Connectivity()}).DialHost(ctx, host)
}

// TLS dials verify the node's certificate against the host's logical name
// unless the application turns verification off, and a failed handshake
// never falls back to plaintext.
func TestTLSVerification(t *testing.T) {
	t.Run("HostnameVerifiedByDefault", func(t *testing.T) {
		cert, clientTLS := tlsServerCert(t, []string{"node1.test"}, []net.IP{net.IPv4(127, 0, 0, 1)})
		for _, tc := range []struct {
			name, hostname string
			insecure, ok   bool
		}{
			{name: "matching name", hostname: "node1.test", ok: true},
			{name: "matching address", ok: true},
			{name: "other name", hostname: "node2.test"},
			{name: "other name, verification off", hostname: "node2.test", insecure: true, ok: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				go func() {
					raw, err := listener.Accept()
					if err != nil {
						return
					}
					defer raw.Close()
					server := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
					if server.Handshake() == nil {
						io.Copy(io.Discard, server)
					}
				}()
				config := clientTLS.Clone()
				config.InsecureSkipVerify = tc.insecure
				conn, err := dialTLS(t, tlsTestHost(t, listener.Addr(), tc.hostname), config)
				if tc.ok {
					if err != nil {
						t.Fatal(err)
					}
					conn.Conn.Close()
					return
				}
				if conn != nil {
					conn.Conn.Close()
				}
				var hostnameErr x509.HostnameError
				if !errors.As(err, &hostnameErr) {
					t.Fatalf("dial to a node whose certificate does not name it: %v, want x509.HostnameError", err)
				}
			})
		}
	})

	t.Run("NoPlaintextFallback", func(t *testing.T) {
		untrusted, _ := tlsServerCert(t, nil, []net.IP{net.IPv4(127, 0, 0, 1)})
		for _, tc := range []struct {
			name  string
			serve func(net.Conn)
		}{
			{name: "plaintext server", serve: func(c net.Conn) {
				// A CQL error frame, as a plaintext node would answer.
				c.Write([]byte{0x84, 0, 0, 0, 0, 0, 0, 0, 0})
			}},
			{name: "closed by server", serve: func(net.Conn) {}},
			{name: "untrusted certificate", serve: func(c net.Conn) {
				tls.Server(c, &tls.Config{Certificates: []tls.Certificate{untrusted}}).Handshake()
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, clientTLS := tlsServerCert(t, nil, []net.IP{net.IPv4(127, 0, 0, 1)})
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				var accepted atomic.Int32
				firstByte := make(chan byte, 4)
				go func() {
					for {
						raw, err := listener.Accept()
						if err != nil {
							return
						}
						accepted.Add(1)
						var b [1]byte
						raw.SetReadDeadline(time.Now().Add(5 * time.Second))
						if _, err := io.ReadFull(raw, b[:]); err == nil {
							firstByte <- b[0]
						}
						tc.serve(&prefixConn{Conn: raw, prefix: b[:]})
						raw.Close()
					}
				}()
				conn, err := dialTLS(t, tlsTestHost(t, listener.Addr(), ""), clientTLS)
				if err == nil {
					conn.Conn.Close()
					t.Fatal("dial succeeded without a verified TLS handshake")
				}
				// The only bytes the node saw open a TLS handshake record.
				select {
				case b := <-firstByte:
					if b != 0x16 {
						t.Fatalf("first byte sent 0x%02x, want a TLS handshake record (0x16)", b)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("the node received nothing")
				}
				// Give a fallback dial the time to arrive, then count.
				time.Sleep(50 * time.Millisecond)
				if n := accepted.Load(); n != 1 {
					t.Fatalf("%d connections after a failed TLS dial, want 1", n)
				}
			})
		}
	})
}

// A dial takes the TLS source's configuration at that time: after a
// TLSFileSource reloads a rotated CA file, the next dial trusts the new CA
// and no longer the old one.
func TestTLSFileSourceReloadReachesDials(t *testing.T) {
	ip := []net.IP{net.IPv4(127, 0, 0, 1)}
	oldCert, _ := tlsServerCert(t, nil, ip)
	newCert, _ := tlsServerCert(t, nil, ip)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	writeCA := func(cert tls.Certificate) {
		t.Helper()
		if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeCA(oldCert)
	source, err := config.NewTLSFileSource(nil, config.TLSFiles{CAFile: caFile, Interval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	dial := func(cert tls.Certificate) error {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			defer raw.Close()
			server := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
			if server.Handshake() == nil {
				io.Copy(io.Discard, server)
			}
		}()
		cfg := settings.DefaultConfig("127.0.0.1")
		cfg.TLSSource = source
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := (&transport.InternalV2TLSHostDialer{InternalConfig: cfg.Connectivity()}).DialHost(ctx, tlsTestHost(t, listener.Addr(), ""))
		if err == nil {
			conn.Conn.Close()
		}
		return err
	}
	var unknownCA x509.UnknownAuthorityError
	if err := dial(oldCert); err != nil {
		t.Fatal("old CA before rotation:", err)
	}
	if err := dial(newCert); !errors.As(err, &unknownCA) {
		t.Fatalf("new CA before rotation: %v, want x509.UnknownAuthorityError", err)
	}
	writeCA(newCert)
	if err := source.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := dial(newCert); err != nil {
		t.Fatal("new CA after rotation:", err)
	}
	if err := dial(oldCert); !errors.As(err, &unknownCA) {
		t.Fatalf("old CA after rotation: %v, want x509.UnknownAuthorityError", err)
	}
}

// nilTLSSource is a TLS source that has no configuration to give.
type nilTLSSource struct{}

func (nilTLSSource) TLSConfig() *tls.Config { return nil }

// A TLS source that returns no configuration fails the dial before any
// socket opens: it never means plaintext.
func TestTLSSourceWithoutConfigDoesNotDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var accepted atomic.Int32
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			raw.Close()
		}
	}()
	config := settings.DefaultConfig("127.0.0.1")
	config.TLSSource = nilTLSSource{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := (&transport.InternalV2TLSHostDialer{InternalConfig: config.Connectivity()}).DialHost(ctx, tlsTestHost(t, listener.Addr(), ""))
	if err == nil {
		conn.Conn.Close()
		t.Fatal("dial succeeded without a TLS configuration")
	}
	time.Sleep(50 * time.Millisecond)
	if n := accepted.Load(); n != 0 {
		t.Fatalf("%d connections opened without a TLS configuration", n)
	}
}

// prefixConn replays prefix before reading from Conn.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}
