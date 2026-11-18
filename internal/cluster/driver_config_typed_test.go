// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// TestDriverConfigTypedReport covers the parts of the report of a session
// built from a config.Config that the fake server of
// TestDriverConfigReportFromBuilder cannot reach: TLS, the ScyllaDB server
// timeout of system queries, and the absence of secrets.
func TestDriverConfigTypedReport(t *testing.T) {
	t.Run("TLS", func(t *testing.T) {
		verify := func(v bool) *bool { return &v }
		for _, tc := range []struct {
			name   string
			config *tls.Config
			want   *tlsReport
		}{
			{name: "Plaintext"},
			{name: "Verifies", config: &tls.Config{}, want: &tlsReport{HostnameVerification: verify(true)}},
			{name: "ServerName", config: &tls.Config{ServerName: "node"}, want: &tlsReport{HostnameVerification: verify(true)}},
			{name: "Skips", config: &tls.Config{InsecureSkipVerify: true}, want: &tlsReport{HostnameVerification: verify(false)}},
			{name: "SkipsWithCallback", config: &tls.Config{InsecureSkipVerify: true, VerifyConnection: func(tls.ConnectionState) error { return nil }}, want: &tlsReport{}},
			{name: "SkipsWithPeerCallback", config: &tls.Config{InsecureSkipVerify: true, VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error { return nil }}, want: &tlsReport{}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cfg := InternalNewClusterSettings("127.0.0.1")
				typed := settings.DefaultConfig("127.0.0.1")
				typed.TLS = tc.config
				// The typed dialer applies TLS itself, so the legacy
				// HostDialer must not hide it.
				cfg.HostDialer = &transport.InternalV2TLSHostDialer{InternalConfig: typed.Connectivity()}
				conn := typed.Connectivity()
				got := buildConnectionReport(cfg, &conn).TLS
				if (got == nil) != (tc.want == nil) || got != nil && (got.HostnameVerification == nil) != (tc.want.HostnameVerification == nil) ||
					got != nil && got.HostnameVerification != nil && *got.HostnameVerification != *tc.want.HostnameVerification {
					t.Fatalf("tls = %+v, want %+v", got, tc.want)
				}
			})
		}
	})

	t.Run("ServerSideTimeout", func(t *testing.T) {
		ms := func(v int64) *int64 { return &v }
		for _, tc := range []struct {
			name     string
			server   cql.Option[time.Duration]
			isScylla bool
			want     *int64
		}{
			{name: "CassandraDefault"},
			{name: "CassandraSet", server: cql.Some(time.Second)},
			{name: "ScyllaDefault", isScylla: true, want: ms(30000)},
			{name: "ScyllaSet", server: cql.Some(1500 * time.Millisecond), isScylla: true, want: ms(1500)},
			{name: "ScyllaDisabled", server: cql.Some(time.Duration(0)), isScylla: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cfg := InternalNewClusterSettings("127.0.0.1")
				cfg.MetadataSchemaRequestTimeout = 5 * time.Second
				typed := settings.DefaultConfig("127.0.0.1")
				typed.Metadata.ServerTimeout = tc.server
				conn := typed.Connectivity()
				got := buildControlPlaneReport(cfg, &conn, tc.isScylla).Queries.System.Timeout
				if got.ClientSideMs == nil || *got.ClientSideMs != 5000 {
					t.Errorf("client-side-ms = %v, want 5000", got.ClientSideMs)
				}
				if (got.ServerSideMs == nil) != (tc.want == nil) || got.ServerSideMs != nil && *got.ServerSideMs != *tc.want {
					t.Errorf("server-side-ms = %v, want %v", got.ServerSideMs, tc.want)
				}
			})
		}
	})

	t.Run("UnixSocketHasNoTCPOptions", func(t *testing.T) {
		typed := settings.DefaultConfig("/var/lib/scylla/cql.m")
		typed.SocketKeepalive, typed.Linger, typed.ReceiveBuffer = time.Minute, cql.Some(time.Second), cql.Some(4096)
		conn := typed.Connectivity()
		if got := buildConnectionReport(InternalNewClusterSettings("127.0.0.1"), &conn).Socket; got != (socketReport{}) {
			t.Fatalf("socket = %+v, want no TCP options", got)
		}
	})

	t.Run("OrphanBound", func(t *testing.T) {
		cfg := InternalNewClusterSettings("127.0.0.1")
		if got := buildConnectionReport(cfg, nil).Requests.Orphaned; got == nil || got.Max != transport.InternalDefaultOrphanBudget {
			t.Errorf("legacy orphaned = %+v, want %d", got, transport.InternalDefaultOrphanBudget)
		}
		cfg.MaxRequestsPerConn = 100
		typed := settings.DefaultConfig("127.0.0.1")
		typed.Pool.MaxOrphanedStreams = 1000
		conn := typed.Connectivity()
		if got := buildConnectionReport(cfg, &conn).Requests.Orphaned; got == nil || got.Max != 99 {
			t.Errorf("orphaned = %+v, want 99, capped below the stream count", got)
		}
	})

	t.Run("NoSecrets", func(t *testing.T) {
		secrets := []string{"secret-cert-bytes", "secret-key", "secret-server-name", "secret-route-id", "secret-proxy.example", "secret-contact"}
		typed := settings.DefaultConfig("secret-contact")
		typed.TLS = &tls.Config{
			ServerName:   "secret-server-name",
			Certificates: []tls.Certificate{{Certificate: [][]byte{[]byte("secret-cert-bytes")}, PrivateKey: "secret-key"}},
		}
		typed.ClientRoutes = &connectivity.ClientRoutesOptions{ConnectionIDs: []string{"secret-route-id"}, HostnameOverrides: map[string]string{"secret-route-id": "secret-proxy.example"}}
		cfg := InternalNewClusterSettings("secret-contact")
		cfg.HostDialer = &transport.InternalV2TLSHostDialer{InternalConfig: typed.Connectivity()}
		conn := typed.Connectivity()
		report := InternalDriverConfigReport{
			Version:      InternalDriverConfigVersion,
			Connection:   buildConnectionReport(cfg, &conn),
			ControlPlane: buildControlPlaneReport(cfg, &conn, true),
		}
		data, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		if report.Connection.TLS == nil {
			t.Fatal("TLS was not reported, so its absence of secrets proves nothing")
		}
		for _, secret := range secrets {
			if strings.Contains(string(data), secret) {
				t.Errorf("report leaks %q: %s", secret, data)
			}
		}
	})
}
