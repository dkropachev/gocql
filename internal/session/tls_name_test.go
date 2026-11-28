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
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// nameOnlyTLS returns a server config whose certificate names only name, no
// IP address, and a client config that trusts it and names no server.
func nameOnlyTLS(t *testing.T, name string) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
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
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, &tls.Config{RootCAs: roots}
}

// loopbackResolver resolves every name to 127.0.0.1.
type loopbackResolver struct{}

func (loopbackResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
}

// A contact point dialed by a name its certificate carries passes, but the
// node it discovers is dialed by IP, which the certificate does not name:
// Connect fails with the pool's error, which says what to set. An address
// translator that returns the name makes the name the TLS identity.
func TestTLSNameOfDiscoveredNode(t *testing.T) {
	const name = "node.test"
	server, client := nameOnlyTLS(t, name)
	n := newFakeNode(t, 1, server)
	configure := func(c *settings.Config) {
		c.ContactPoints = []string{fmt.Sprintf("%s:%d", name, n.port(regularPort))}
		c.Port = uint16(n.port(regularPort))
		c.DNSResolver = loopbackResolver{}
		c.TLS = client
		c.Pool.AdvancedShardAwareness.Enabled = false
		c.Metadata.InitialDiscovery = true
		c.Logger = logging.Nop()
	}
	t.Run("ByIP", func(t *testing.T) {
		config := settings.DefaultConfig()
		config.Protocol = 4
		config.Metadata.AutomaticAgreement = false
		configure(&config)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s, err := session.Connect(ctx, config)
		if err == nil {
			_ = s.Close()
			t.Fatal("Connect succeeded, want the pool's TLS name error")
		}
		var connectErr *cqlerr.ConnectError
		var hostnameErr x509.HostnameError
		if !errors.As(err, &connectErr) || !errors.Is(err, cqlerr.ErrNoConnectionsStarted) || !errors.As(err, &hostnameErr) {
			t.Fatalf("Connect = %v, want a cqlerr.ConnectError wrapping an x509.HostnameError", err)
		}
		if msg := err.Error(); !strings.Contains(msg, "ServerName") || !strings.Contains(msg, "WithTLSIdentity") {
			t.Fatalf("Connect = %v, want it to name ServerName and WithTLSIdentity", err)
		}
	})
	t.Run("TranslatedName", func(t *testing.T) {
		connect(t, n, func(c *settings.Config) {
			configure(c)
			c.AddressTranslator = endpoint.TranslatorFunc(func(_ context.Context, _ endpoint.Peer, addr netip.AddrPort) (endpoint.Address, error) {
				return endpoint.Address{Host: name, Port: addr.Port()}, nil
			})
		})
	})
}
