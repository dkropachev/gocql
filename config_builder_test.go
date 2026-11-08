// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"crypto/tls"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/routing"
)

func mustBuildConfig(t *testing.T, b *ConfigBuilder) Config {
	t.Helper()
	c, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return c
}

// comparable drops the fields that hold fresh closures or policies on every
// DefaultConfig call, so the remaining settings compare with DeepEqual.
func comparable(c implSettings.Config) implSettings.Config {
	profile := *c.DefaultProfile
	profile.Routing = nil
	c.DefaultProfile = &profile
	if c.TimestampGenerator == nil {
		panic("missing timestamp generator")
	}
	c.TimestampGenerator = nil
	return c
}

func TestConfigBuilder(t *testing.T) {
	t.Run("DefaultsMatchDriverDefaults", func(t *testing.T) {
		got := *mustBuildConfig(t, NewConfig("10.0.0.1")).settings
		want := implSettings.DefaultConfig("10.0.0.1")
		if !reflect.DeepEqual(comparable(got), comparable(want)) {
			t.Fatalf("NewConfig defaults differ from DefaultConfig:\n got %+v\nwant %+v", comparable(got), comparable(want))
		}
		if got.DefaultProfile.Routing == nil {
			t.Fatal("default profile has no routing policy")
		}
	})

	t.Run("ZeroConfigRejected", func(t *testing.T) {
		_, err := Connect(context.Background(), Config{})
		var invalid *cqlerr.ValidationError
		if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "NewConfig") {
			t.Fatalf("Connect(Config{}) = %v, want a validation error naming NewConfig", err)
		}
	})

	t.Run("BuildReportsEveryInvalidCall", func(t *testing.T) {
		_, err := NewConfig("h").ConnectionsPerShard(0).Port(0).Heartbeat(-time.Second, time.Second).Build()
		if err == nil {
			t.Fatal("Build accepted invalid settings")
		}
		for _, call := range []string{"ConnectionsPerShard(0)", "Port(0)", "Heartbeat(-1s, 1s)"} {
			if !strings.Contains(err.Error(), call) {
				t.Errorf("error %q does not name %s", err, call)
			}
		}
		var invalid *cqlerr.ValidationError
		if !errors.As(err, &invalid) {
			t.Fatalf("error %v is not a ValidationError", err)
		}
	})

	t.Run("NoContactPoints", func(t *testing.T) {
		if _, err := NewConfig().Build(); err == nil || !strings.Contains(err.Error(), "NewConfig()") {
			t.Fatalf("Build without contact points = %v", err)
		}
	})

	t.Run("LastCallWins", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").ConnectionsPerShard(3).ConnectionsPerShard(4).
			DefaultProfile(func(p *ProfileBuilder) { p.Consistency(cql.One) }).
			DefaultProfile(func(p *ProfileBuilder) { p.Consistency(cql.All) }).
			ClientRoutes(NewClientRoutes("c1").HostnameOverride("c1", "a").HostnameOverride("c1", "b")).
			NoAdvancedShardAwareness()).settings
		if c.Pool.ConnectionsPerShard != 4 || c.DefaultProfile.Consistency != cql.All || c.ClientRoutes.HostnameOverrides["c1"] != "b" {
			t.Fatalf("pool %+v consistency %v overrides %v", c.Pool, c.DefaultProfile.Consistency, c.ClientRoutes.HostnameOverrides)
		}
	})

	t.Run("ExclusiveOptionsReplace", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").
			Heartbeat(time.Second, time.Second).NoHeartbeat().
			ConnectionsPerHost(4).ConnectionsPerShard(2).
			AdvancedShardAwareness(50000, 60000).NoAdvancedShardAwareness().
			MetadataRefresh(time.Second).NoMetadataRefresh().
			PreparedCacheSize(10).NoPreparedCache().
			TimestampGenerator(func() int64 { return 1 }).ServerTimestamps()).settings
		if c.KeepaliveInterval != 0 || c.Pool.ConnectionsPerHost != 0 || c.Pool.ConnectionsPerShard != 2 || c.Pool.ShardAwarePort ||
			c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.TimestampGenerator != nil {
			t.Fatalf("a later call did not replace an earlier one: %+v", c)
		}
		c = mustBuildConfig(t, NewConfig("h").NoHeartbeat().Heartbeat(time.Second, 2*time.Second).ConnectionsPerShard(2).ConnectionsPerHost(4)).settings
		if c.KeepaliveInterval != time.Second || c.KeepaliveTimeout != 2*time.Second || c.Pool.ConnectionsPerHost != 4 {
			t.Fatalf("reverse order: %+v", c)
		}
	})

	t.Run("InvalidArgumentKeepsPreviousValue", func(t *testing.T) {
		b := NewConfig("h").ConnectionsPerShard(3).ConnectionsPerShard(-1)
		if got := b.settings.Pool.ConnectionsPerShard; got != 3 {
			t.Fatalf("ConnectionsPerShard = %d, want 3", got)
		}
		if _, err := b.Build(); err == nil || !strings.Contains(err.Error(), "ConnectionsPerShard(-1)") {
			t.Fatalf("Build = %v", err)
		}
	})

	t.Run("TLSCopiedOnSet", func(t *testing.T) {
		config := &tls.Config{ServerName: "before"}
		b := NewConfig("h").TLS(config)
		config.ServerName = "after"
		if got := mustBuildConfig(t, b).settings.TLS.ServerName; got != "before" {
			t.Fatalf("ServerName = %q, want the value at TLS()", got)
		}
	})

	t.Run("TLSFiles", func(t *testing.T) {
		pki := TLSFiles{CertFile: "testdata/pki/gocql.crt", KeyFile: "testdata/pki/gocql.key", CAFile: "testdata/pki/ca.crt"}
		for _, tc := range []struct {
			name  string
			files TLSFiles
			want  string
		}{
			{"CertWithoutKey", TLSFiles{CertFile: pki.CertFile}, "set together"},
			{"Empty", TLSFiles{}, "pair or a CA file is required"},
			{"NegativeInterval", TLSFiles{CAFile: pki.CAFile, Interval: -time.Second}, "must not be negative"},
			{"MissingFile", TLSFiles{CAFile: "testdata/pki/missing.crt"}, "missing.crt"},
			{"MismatchedPair", TLSFiles{CertFile: pki.CertFile, KeyFile: "testdata/pki/cassandra.key"}, "cert and key"},
			{"NotPEM", TLSFiles{CAFile: "testdata/pki/ca.cnf"}, "no PEM certificates"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := NewConfig("h").TLSFiles(tc.files).Build(); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Build = %v, want an error containing %q", err, tc.want)
				}
			})
		}

		t.Run("AppliedOverTLS", func(t *testing.T) {
			base := &tls.Config{ServerName: "node", MinVersion: tls.VersionTLS13}
			c := mustBuildConfig(t, NewConfig("h").TLSFiles(pki).TLS(base)).settings
			if c.TLS.ServerName != "node" || c.TLS.MinVersion != tls.VersionTLS13 || len(c.TLS.Certificates) != 1 || c.TLS.RootCAs == nil {
				t.Fatal("files were not applied over the TLS settings")
			}
			if c.TLSFilesBase == nil || c.TLSFilesBase.ServerName != "node" || len(c.TLSFilesBase.Certificates) != 0 {
				t.Fatal("the base for reloads is not the TLS config")
			}
		})

		t.Run("DefaultsWithoutTLS", func(t *testing.T) {
			c := mustBuildConfig(t, NewConfig("h").TLSFiles(TLSFiles{CAFile: pki.CAFile})).settings
			if c.TLS == nil || c.TLS.MinVersion != tls.VersionTLS12 || c.TLS.RootCAs == nil || len(c.TLS.Certificates) != 0 {
				t.Fatal("CA-only files without TLS did not build a TLS 1.2+ config")
			}
		})

		t.Run("GetClientCertificateConflicts", func(t *testing.T) {
			base := &tls.Config{GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return nil, nil }}
			if _, err := NewConfig("h").TLS(base).TLSFiles(pki).Build(); err == nil || !strings.Contains(err.Error(), "GetClientCertificate") {
				t.Fatalf("Build = %v", err)
			}
		})

		t.Run("UnixSocketRejected", func(t *testing.T) {
			if _, err := NewConfig("/var/lib/scylla/cql.m").TLSFiles(TLSFiles{CAFile: pki.CAFile}).Build(); err == nil {
				t.Fatal("TLS files on a Unix socket were accepted")
			}
		})
	})

	t.Run("UnixSocketInfersProtocolAndDiscovery", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("/var/lib/scylla/cql.m")).settings
		if c.Protocol != 3 || c.Metadata.InitialDiscovery {
			t.Fatalf("protocol %d discovery %t, want 3 and false", c.Protocol, c.Metadata.InitialDiscovery)
		}
		if _, err := NewConfig("/var/lib/scylla/cql.m").ProtocolVersion(Protocol4).Build(); err == nil {
			t.Fatal("explicit protocol 4 on a Unix socket was accepted")
		}
		if _, err := NewConfig("/var/lib/scylla/cql.m").TLS(&tls.Config{}).Build(); err == nil {
			t.Fatal("TLS on a Unix socket was accepted")
		}
	})

	t.Run("ClientRoutesInfersNoAdvancedShardAwareness", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").ClientRoutes(NewClientRoutes("c1"))).settings
		if c.Pool.ShardAwarePort {
			t.Fatal("advanced shard awareness stayed on under client routes")
		}
		if _, err := NewConfig("h").AdvancedShardAwareness(49152, 65535).ClientRoutes(NewClientRoutes("c1")).Build(); err == nil {
			t.Fatal("explicit advanced shard awareness with client routes was accepted")
		}
	})

	t.Run("BuildSnapshotsBuilder", func(t *testing.T) {
		b := NewConfig("h").MetadataKeyspaces("a")
		first := mustBuildConfig(t, b)
		b.MetadataKeyspaces("b", "c").Partitioner("X", routing.Murmur3TokenPartitioner)
		second := mustBuildConfig(t, b)
		if len(first.settings.Metadata.Keyspaces) != 1 || first.settings.Partitioners["X"] != nil {
			t.Fatal("changing the builder changed an already built Config")
		}
		if len(second.settings.Metadata.Keyspaces) != 2 || second.settings.Partitioners["X"] == nil {
			t.Fatal("changes missing from the second Build")
		}
	})

	t.Run("DefaultProfileChoicesAccumulate", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").
			DefaultProfile(func(p *ProfileBuilder) { p.Consistency(cql.Quorum) }).
			DefaultProfile(func(p *ProfileBuilder) { p.PageSize(7) })).settings
		if c.DefaultProfile.Consistency != cql.Quorum || c.DefaultProfile.PageSize != 7 {
			t.Fatalf("profile = %+v", *c.DefaultProfile)
		}
		_, err := NewConfig("h").DefaultProfile(func(p *ProfileBuilder) { p.PageSize(0) }).Build()
		if err == nil || !strings.Contains(err.Error(), "DefaultProfile") || !strings.Contains(err.Error(), "PageSize(0)") {
			t.Fatalf("invalid profile error = %v", err)
		}
	})

	t.Run("PartitionerAddsToBuiltins", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").Partitioner("Custom", routing.Murmur3TokenPartitioner)).settings
		if c.Partitioners["Custom"] == nil || c.Partitioners["Murmur3Partitioner"] == nil {
			t.Fatalf("partitioners = %v", c.Partitioners)
		}
	})

	t.Run("IgnoreEvents", func(t *testing.T) {
		m := mustBuildConfig(t, NewConfig("h").IgnoreEvents(SchemaEvent, StatusEvent)).settings.Metadata
		if m.SchemaEvents || m.StatusEvents || !m.TopologyEvents || !m.ClientRouteEvents {
			t.Fatalf("events = %+v", m)
		}
		if _, err := NewConfig("h").IgnoreEvents(OverflowEvent).Build(); err == nil {
			t.Fatal("IgnoreEvents accepted a client-side event")
		}
	})

	t.Run("DisableMethods", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").NoHeartbeat().NoMetadataRefresh().NoPreparedCache().ServerTimestamps().
			NoConfigReport().NoDiscovery().ManualSchemaAgreement().NoRefreshAfterAgreement().NoAdvancedShardAwareness()).settings
		if c.KeepaliveInterval != 0 || c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.TimestampGenerator != nil ||
			c.ReportConfiguration || c.Metadata.InitialDiscovery || c.Metadata.AutomaticAgreement || c.Metadata.RefreshAfterAgreement || c.Pool.ShardAwarePort {
			t.Fatalf("a No* method left its feature on: %+v", c)
		}
	})

	t.Run("StringHidesCredentials", func(t *testing.T) {
		s := mustBuildConfig(t, NewConfig("h").Auth(auth.Password("user", "secret-password"))).String()
		if strings.Contains(s, "secret-password") || !strings.Contains(s, "auth=true") {
			t.Fatalf("String() = %q", s)
		}
		if (Config{}).String() != "gocql.Config{}" {
			t.Fatal("zero Config String")
		}
	})

	t.Run("ProfileBuild", func(t *testing.T) {
		p, err := NewProfile().Consistency(cql.One).NoSerialConsistency().NoPrefetch().Build()
		if err != nil {
			t.Fatal(err)
		}
		if p.Consistency != cql.One || p.SerialConsistency.Set || p.Prefetch != 0 || p.Routing == nil {
			t.Fatalf("profile = %+v", p)
		}
		if _, err := NewProfile().SerialConsistency(cql.Quorum).Build(); err == nil {
			t.Fatal("non-serial serial consistency accepted")
		}
	})
}
