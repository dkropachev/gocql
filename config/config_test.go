// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/event"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
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

// comparable drops the fields that hold fresh policies on every
// DefaultConfig call, and the reconnect probe Build adds, so the remaining
// settings compare with DeepEqual.
func comparable(c implSettings.Config) implSettings.Config {
	c.ReconnectFirst = implSettings.ReconnectProbe{}
	profile := *c.DefaultProfile
	profile.Routing = nil
	c.DefaultProfile = &profile
	return c
}

func TestConfigBuilder(t *testing.T) {
	t.Run("DefaultsMatchDriverDefaults", func(t *testing.T) {
		got := *mustBuildConfig(t, New(ScyllaV4, "10.0.0.1")).settings
		want := implSettings.DefaultConfig("10.0.0.1")
		want.Server = implSettings.ServerScylla // the dialect's, which DefaultConfig leaves open
		if !reflect.DeepEqual(comparable(got), comparable(want)) {
			t.Fatalf("New defaults differ from DefaultConfig:\n got %+v\nwant %+v", comparable(got), comparable(want))
		}
		if got.DefaultProfile.Routing == nil {
			t.Fatal("default profile has no routing policy")
		}
	})

	t.Run("ZeroConfigRejected", func(t *testing.T) {
		_, err := implSettings.InternalResolvePublic(Config{})
		if _, ok := errors.AsType[*cql.ValidationError](err); !ok || !strings.Contains(err.Error(), "config.New") {
			t.Fatalf("resolving Config{} = %v, want a validation error naming config.New", err)
		}
	})

	t.Run("BuildValidatesFinalSettings", func(t *testing.T) {
		b := New(ScyllaV4, "h").WithPort(0).WithConnectionsPerShard(-1)
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted an invalid port")
		} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithPort" || !strings.Contains(err.Error(), "1 to 65535") {
			t.Fatalf("Build = %v, want a WithPort ValidationError naming the limit", err)
		}
		b.WithPort(9042)
		if _, err := b.Build(); err == nil {
			t.Fatal("correcting the port hid the invalid pool size")
		}
		b.WithConnectionsPerShard(2)
		mustBuildConfig(t, b)
	})

	t.Run("ApplicationVersionRequiresName", func(t *testing.T) {
		b := New(ScyllaV4, "h").WithApplication("", "1.0.0")
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted an application version without a name")
		} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithApplication" {
			t.Fatalf("Build = %v, want a WithApplication ValidationError", err)
		}
		mustBuildConfig(t, b.WithApplication("app", "1.0.0"))
		mustBuildConfig(t, b.WithApplication("", ""))
	})

	t.Run("ErrorStatementText", func(t *testing.T) {
		if _, err := New(ScyllaV4, "h").WithErrorStatementText(-1).Build(); err == nil {
			t.Fatal("Build accepted a negative error statement limit")
		} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithErrorStatementText" {
			t.Fatalf("Build = %v, want a WithErrorStatementText ValidationError", err)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithErrorStatementText(200)).settings; c.ErrorStatementBytes != 200 {
			t.Fatalf("ErrorStatementBytes = %d, want 200", c.ErrorStatementBytes)
		}
	})

	t.Run("LocalDatacenter", func(t *testing.T) {
		if _, err := New(ScyllaV4, "h").WithLocalDatacenter("", "r1").Build(); err == nil {
			t.Fatal("Build accepted a local rack without a datacenter")
		} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithLocalDatacenter" {
			t.Fatalf("Build = %v, want a WithLocalDatacenter ValidationError", err)
		}
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithLocalDatacenter("dc1", "r1")).settings
		if c.LocalDatacenter != "dc1" || c.LocalRack != "r1" {
			t.Fatalf("locality %q/%q", c.LocalDatacenter, c.LocalRack)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithLocalDatacenter("dc1", "r1").WithLocalDatacenter("", "")).settings; c.LocalDatacenter != "" || c.LocalRack != "" {
			t.Fatalf("cleared locality %q/%q", c.LocalDatacenter, c.LocalRack)
		}
	})

	t.Run("NoContactPoints", func(t *testing.T) {
		if _, err := New(ScyllaV4).Build(); err == nil || !strings.Contains(err.Error(), "WithContactPoints") {
			t.Fatalf("Build without contact points = %v", err)
		}
	})

	t.Run("LastCallWins", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithConnectionsPerShard(3).WithConnectionsPerShard(4).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) }).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.All) }).
			WithClientRoutes(ClientRoute{ConnectionID: "c1", Hostname: "a"}, ClientRoute{ConnectionID: "c2", Hostname: "x"}).
			WithClientRoutes(ClientRoute{ConnectionID: "c1", Hostname: "b"}).
			WithAdvancedShardAwareness(false)).settings
		if c.Pool.ConnectionsPerShard != 4 || c.DefaultProfile.Consistency != cql.All {
			t.Fatalf("pool %+v consistency %v", c.Pool, c.DefaultProfile.Consistency)
		}
		// The last WithClientRoutes replaces every route and override.
		if r := c.ClientRoutes; !reflect.DeepEqual(r.ConnectionIDs, []string{"c1"}) || !reflect.DeepEqual(r.HostnameOverrides, map[string]string{"c1": "b"}) {
			t.Fatalf("client routes = %+v", *r)
		}
	})

	t.Run("ExclusiveOptionsReplace", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").
			WithHeartbeat(time.Second, time.Second).WithHeartbeat(0, 0).
			WithConnectionsPerHost(4).WithConnectionsPerShard(2).
			WithAdvancedShardAwareness(true).WithAdvancedShardAwareness(false).
			WithMetadataRefresh(time.Second).WithMetadataRefresh(0).
			WithPreparedCacheSize(10).WithPreparedCacheSize(0).
			WithTimestampGenerator(func() int64 { return 1 }).WithTimestampGenerator(nil)).settings
		if c.KeepaliveInterval != 0 || c.Pool.ConnectionsPerHost != 0 || c.Pool.ConnectionsPerShard != 2 || c.Pool.AdvancedShardAwareness.Enabled ||
			c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.Timestamps.Mode() != implSettings.ServerTimestampMode {
			t.Fatalf("a later call did not replace an earlier one: %+v", c)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h")).settings; c.Timestamps.Mode() != implSettings.DriverTimestampMode {
			t.Fatalf("default timestamps: mode %d", c.Timestamps.Mode())
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithTimestampGenerator(func() int64 { return 7 })).settings; c.Timestamps.Mode() != implSettings.GeneratedTimestampMode || c.Timestamps.Generator()() != 7 {
			t.Fatalf("a user timestamp generator: mode %d", c.Timestamps.Mode())
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithTimestampGenerator(nil)).settings; c.Timestamps.Mode() != implSettings.ServerTimestampMode {
			t.Fatalf("a nil timestamp generator: mode %d", c.Timestamps.Mode())
		}
		c = mustBuildConfig(t, New(ScyllaV4, "h").WithHeartbeat(0, 0).WithHeartbeat(time.Second, 2*time.Second).WithConnectionsPerShard(2).WithConnectionsPerHost(4)).settings
		if c.KeepaliveInterval != time.Second || c.KeepaliveTimeout != 2*time.Second || c.Pool.ConnectionsPerHost != 4 {
			t.Fatalf("reverse order: %+v", c)
		}
	})

	// DocumentedDefaults pins the default each setter's doc names, so a
	// changed default fails here until its doc is updated too.
	t.Run("DocumentedDefaults", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h")).settings
		p := c.DefaultProfile
		for name, ok := range map[string]bool{
			"WithPort 9042":                         c.Port == 9042,
			"WithKeyspace none":                     c.Keyspace == "",
			"WithAuth none":                         c.Auth == nil,
			"WithTLS plaintext":                     c.TLS == nil && c.TLSSource == nil,
			"WithCompression none":                  c.Compression == cql.NoCompression,
			"WithCQLVersion 3.0.0":                  c.CQLVersion == "3.0.0",
			"WithMaxFrameSize 256 MiB":              c.Limits.MaxFrameBytes == 256<<20,
			"WithMaxDecompressedSize 256 MiB":       c.Limits.MaxDecompressedBytes == 256<<20,
			"WithMaxMetadataDepth 64":               c.Limits.MaxMetadataDepth == 64,
			"WithMaxPagingStates 65536":             c.Limits.MaxPagingStates == 65536,
			"WithMaxPagingStateSize 16 MiB":         c.Limits.MaxPagingStateBytes == 16<<20,
			"WithDialer net.Dialer":                 c.Dialer == nil,
			"WithResolver net.DefaultResolver":      c.DNSResolver == nil,
			"WithLocalAddress any":                  !c.LocalAddress.IsValid(),
			"WithConnectTimeout 5s":                 c.ConnectTimeout == 5*time.Second,
			"WithDNSTimeout 5s":                     c.DNSTimeout == 5*time.Second,
			"WithNagle off":                         c.NoDelay,
			"WithTCPKeepalive off":                  c.SocketKeepalive == 0,
			"WithReceiveBuffer the OS's":            c.ReceiveBuffer == 0,
			"WithSendBuffer the OS's":               c.SendBuffer == 0,
			"WithLinger the OS's":                   !c.Linger.Set,
			"WithReadTimeout none":                  c.ReadTimeout == 0,
			"WithWriteTimeout none":                 c.WriteTimeout == 0,
			"WithHeartbeat 30s and 30s":             c.KeepaliveInterval == 30*time.Second && c.KeepaliveTimeout == 30*time.Second,
			"WithConnectionsPerShard 1":             c.Pool.ConnectionsPerShard == 1 && c.Pool.ConnectionsPerHost == 0,
			"WithMaxExcessPerShard 2":               c.Pool.MaxExcessPerShard == 2,
			"WithMaxRequestsPerConnection 32768":    c.Pool.MaxRequestsPerConnection == 32768,
			"WithMaxOrphanedStreams 1024":           c.Pool.MaxOrphanedStreams == 1024,
			"WithAdvancedShardAwareness true":       c.Pool.AdvancedShardAwareness.Enabled,
			"WithShardAwareSourcePorts 49152-65535": c.Pool.AdvancedShardAwareness.SourcePortStart == 49152 && c.Pool.AdvancedShardAwareness.SourcePortEnd == 65535,
			"WithDiscovery true":                    c.Metadata.InitialDiscovery,
			"WithHostFilter all nodes":              c.HostFilter == nil,
			"WithAddressTranslator identity":        c.AddressTranslator == nil,
			"WithClientRoutes none":                 c.ClientRoutes == nil,
			"WithReconnect jittered backoff":        implSettings.IsDefaultReconnect(c.Reconnect),
			"WithConviction SimpleConvictionPolicy": c.Conviction == nil,
			"WithMetadataMode ModeFull":             c.Metadata.Mode == metadata.ModeFull,
			"WithMetadataKeyspaces all":             len(c.Metadata.Keyspaces) == 0,
			"WithPartitioner built-ins only":        len(c.Partitioners) == 4,
			"WithMetadataRefresh 60s":               c.Metadata.PeriodicRefresh == 60*time.Second,
			"WithMetadataTimeout 31s":               c.Metadata.RequestTimeout == 31*time.Second,
			"WithMetadataServerTimeout not chosen (30s on ScyllaDB, none on Cassandra)": !c.Metadata.ServerTimeout.Set,
			"WithIgnoreEvents none ignored":                                             c.Metadata.TopologyEvents && c.Metadata.StatusEvents && c.Metadata.SchemaEvents && c.Metadata.ClientRouteEvents,
			"WithSchemaAgreement 200ms and 60s":                                         c.Metadata.AgreementTimeout == 60*time.Second && c.Metadata.AgreementPoll == 200*time.Millisecond,
			"WithAutomaticSchemaAgreement true":                                         c.Metadata.AutomaticAgreement,
			"WithRefreshAfterAgreement true":                                            c.Metadata.RefreshAfterAgreement,
			"WithPreparedCacheSize 1000":                                                c.PreparedCacheSize == 1000,
			"WithTimestampGenerator driver stamps":                                      c.Timestamps.Mode() == implSettings.DriverTimestampMode,
			"WithApplication none":                                                      c.Identity.ApplicationName == "" && c.Identity.ApplicationVersion == "",
			"WithClientID none":                                                         c.Identity.ClientID == "",
			"WithLogger slog.Default":                                                   c.Logger == nil,
			"WithObserver none":                                                         c.Observer == nil,
			"WithLifecycleObserver none":                                                c.LifecycleObserver == nil,
			"WithConfigReport true":                                                     c.ReportConfiguration,
			"WithErrorStatementText off":                                                c.ErrorStatementBytes == 0,
			"Profile WithConsistency LOCAL_QUORUM":                                      p.Consistency == cql.LocalQuorum,
			"Profile WithSerialConsistency LOCAL_SERIAL":                                p.SerialConsistency == cql.Some(cql.LocalSerial),
			"Profile WithTimeout 30s":                                                   p.Timeout == 30*time.Second,
			"Profile WithPageTimeout none":                                              p.PageTimeout == 0,
			"Profile WithPageSize 5000":                                                 p.PageSize == 5000,
			"Profile WithPrefetch one":                                                  p.Prefetch == 1,
			"Profile WithRetry retry.Safe{MaxAttempts: 3}":                              p.Retry == retry.Safe{MaxAttempts: 3},
			"Profile WithRouting token-aware with shuffling":                            p.Routing != nil,
			"Profile WithSpeculation none":                                              p.Speculation == nil,
		} {
			if !ok {
				t.Errorf("%s: the built default differs from the documented one", name)
			}
		}
	})

	t.Run("ProfileExclusiveOptionsReplace", func(t *testing.T) {
		p := codecTestMust(NewProfile().NoSerialConsistency().WithSerialConsistency(cql.Serial).WithTimeout(0).WithTimeout(time.Second).WithPrefetch(0).WithPrefetch(1).Build())
		if p.SerialConsistency != cql.Some(cql.Serial) || p.Timeout != time.Second || p.Prefetch != 1 {
			t.Fatalf("a later call did not replace an earlier one: %+v", p)
		}
		p = codecTestMust(NewProfile().WithSerialConsistency(cql.Serial).NoSerialConsistency().WithTimeout(time.Second).WithTimeout(0).WithPrefetch(1).WithPrefetch(0).Build())
		if p.SerialConsistency.Set || p.Timeout != 0 || p.Prefetch != 0 {
			t.Fatalf("a later clearing call did not replace an earlier one: %+v", p)
		}
	})

	t.Run("ClientRoutesRejectAddressTranslator", func(t *testing.T) {
		routes := ClientRoute{ConnectionID: "c1"}
		translator := endpoint.TranslatorFunc(func(_ context.Context, _ endpoint.Peer, addr netip.AddrPort) (netip.AddrPort, error) {
			return addr, nil
		})
		for name, b := range map[string]*ConfigBuilder{
			"RoutesFirst":     New(ScyllaV4, "h").WithClientRoutes(routes).WithAddressTranslator(translator),
			"TranslatorFirst": New(ScyllaV4, "h").WithAddressTranslator(translator).WithClientRoutes(routes),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted client routes with an address translator")
				} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithClientRoutes" {
					t.Fatalf("Build = %v, want a WithClientRoutes ValidationError", err)
				}
				mustBuildConfig(t, b.WithClientRoutes())
			})
		}
	})

	t.Run("InvalidLastValueReplacesPreviousValue", func(t *testing.T) {
		b := New(ScyllaV4, "h").WithConnectionsPerShard(3).WithConnectionsPerShard(-1)
		if got := b.settings.Pool.ConnectionsPerShard; got != -1 {
			t.Fatalf("ConnectionsPerShard = %d, want the last value -1", got)
		}
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted the invalid last value")
		}
		b.WithConnectionsPerHost(4)
		mustBuildConfig(t, b)
	})

	t.Run("CorrectionsAndResets", func(t *testing.T) {
		b := New(ScyllaV4).WithContactPoints("h").
			WithHeartbeat(-time.Second, -time.Second).WithHeartbeat(0, 0).
			WithIgnoreEvents(event.Overflow).WithIgnoreEvents(event.Status).
			WithTLSSource(staticTLS{}).WithTLS(nil).
			WithClientRoutes(ClientRoute{ConnectionID: "c1", Hostname: "a"}).WithClientRoutes().
			WithMetadataKeyspaces("").WithMetadataKeyspaces().
			WithDiscovery(false).WithDiscovery(true).
			WithConfigReport(false).WithConfigReport(true).
			WithAutomaticSchemaAgreement(false).WithAutomaticSchemaAgreement(true).
			WithRefreshAfterAgreement(false).WithRefreshAfterAgreement(true).
			ConfigureDefaultProfile(func(p *ProfileBuilder) {
				p.WithPageSize(0).WithPageSize(42).WithPrefetch(0).WithPrefetch(1)
			})
		c := mustBuildConfig(t, b).settings
		if c.TLS != nil || c.ClientRoutes != nil || !c.Metadata.InitialDiscovery || !c.ReportConfiguration ||
			!c.Metadata.AutomaticAgreement || !c.Metadata.RefreshAfterAgreement ||
			!c.Metadata.SchemaEvents || c.Metadata.StatusEvents || c.DefaultProfile.PageSize != 42 || c.DefaultProfile.Prefetch != 1 {
			t.Fatalf("later settings did not replace previous choices: %+v", c)
		}
		b.WithIgnoreEvents().ConfigureDefaultProfile(nil)
		reset := mustBuildConfig(t, b).settings
		if !reset.Metadata.StatusEvents || reset.DefaultProfile.PageSize != 5000 {
			t.Fatal("reset did not restore defaults")
		}
		// Already-built configurations are snapshots, including event choices.
		if c.Metadata.StatusEvents || c.DefaultProfile.PageSize != 42 {
			t.Fatal("later builder changes reached a built configuration")
		}
	})

	t.Run("TLSCopiedOnSet", func(t *testing.T) {
		config := &tls.Config{ServerName: "before"}
		b := New(ScyllaV4, "h").WithTLS(config)
		config.ServerName = "after"
		if got := mustBuildConfig(t, b).settings.TLS.ServerName; got != "before" {
			t.Fatalf("ServerName = %q, want the value at WithTLS()", got)
		}
	})

	t.Run("TLS", func(t *testing.T) {
		source := staticTLS{config: &tls.Config{ServerName: "node"}}
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithTLS(&tls.Config{ServerName: "first"}).WithTLSSource(source)).settings
		if c.TLSSource != source || c.TLS == nil || c.TLS.ServerName != "node" {
			t.Fatal("the last WithTLSSource did not win or its config was not validated")
		}
		c = mustBuildConfig(t, New(ScyllaV4, "h").WithTLSSource(source).WithTLS(&tls.Config{ServerName: "last"})).settings
		if c.TLS == nil || c.TLS.ServerName != "last" {
			t.Fatal("WithTLS did not replace WithTLSSource")
		}
		for name, tc := range map[string]struct {
			b    *ConfigBuilder
			want string
		}{
			"NoConfig":   {New(ScyllaV4, "h").WithTLSSource(staticTLS{}), "returned no config"},
			"UnixSocket": {New(ScyllaV4, "/var/lib/scylla/cql.m").WithTLSSource(source), "Unix TLS"},
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := tc.b.Build(); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Build = %v, want an error containing %q", err, tc.want)
				}
			})
		}
	})

	t.Run("UnixSocketInfersNoDiscovery", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "/var/lib/scylla/cql.m")).settings
		if c.Metadata.InitialDiscovery {
			t.Fatal("a Unix socket kept host discovery")
		}
		if _, err := New(ScyllaV4, "/var/lib/scylla/cql.m").WithTLS(&tls.Config{}).Build(); err == nil {
			t.Fatal("TLS on a Unix socket was accepted")
		}
	})

	t.Run("ClientRoutes", func(t *testing.T) {
		routes := []ClientRoute{{ConnectionID: "c1", Hostname: "a"}, {ConnectionID: "c2"}}
		b := New(ScyllaV4, "h").WithClientRoutes(routes...)
		routes[0].Hostname = "changed"
		r := mustBuildConfig(t, b).settings.ClientRoutes
		if !reflect.DeepEqual(r.ConnectionIDs, []string{"c1", "c2"}) || !reflect.DeepEqual(r.HostnameOverrides, map[string]string{"c1": "a"}) {
			t.Fatalf("client routes settings = %+v, want the routes at WithClientRoutes", *r)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithClientRoutes()).settings; c.ClientRoutes != nil {
			t.Fatal("WithClientRoutes() kept client routes")
		}
		for name, b := range map[string]*ConfigBuilder{
			"EmptyID":    New(ScyllaV4, "h").WithClientRoutes(ClientRoute{Hostname: "a"}),
			"RepeatedID": New(ScyllaV4, "h").WithClientRoutes(ClientRoute{ConnectionID: "c1"}, ClientRoute{ConnectionID: "c1", Hostname: "b"}),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted the routes")
				} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithClientRoutes" {
					t.Fatalf("Build = %v, want a WithClientRoutes ValidationError", err)
				}
			})
		}
		if _, err := New(CassandraV4, "h").WithClientRoutes(ClientRoute{ConnectionID: "c1"}).Build(); err == nil {
			t.Fatal("Build accepted client routes on Cassandra")
		}
	})

	t.Run("ClientRoutesAdvancedShardAwareness", func(t *testing.T) {
		route := ClientRoute{ConnectionID: "c1"}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithClientRoutes(route)).settings; c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("advanced shard awareness stayed on under client routes")
		}
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithAdvancedShardAwareness(true).WithShardAwareSourcePorts(50000, 60000).WithClientRoutes(route)).settings
		if !c.Pool.AdvancedShardAwareness.Enabled || c.Pool.AdvancedShardAwareness.SourcePortStart != 50000 || c.Pool.AdvancedShardAwareness.SourcePortEnd != 60000 {
			t.Fatalf("explicit advanced shard awareness under client routes: %+v", c.Pool)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithShardAwareSourcePorts(50000, 60000).WithClientRoutes(route)).settings; c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("WithShardAwareSourcePorts turned advanced shard awareness on under client routes")
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithAdvancedShardAwareness(true).WithAdvancedShardAwareness(false).WithClientRoutes(route)).settings; c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("the last WithAdvancedShardAwareness did not win")
		}
	})

	t.Run("ShardAwareSourcePorts", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithAdvancedShardAwareness(false).WithShardAwareSourcePorts(50000, 60000).WithAdvancedShardAwareness(true)).settings
		if sa := c.Pool.AdvancedShardAwareness; !sa.Enabled || sa.SourcePortStart != 50000 || sa.SourcePortEnd != 60000 {
			t.Fatalf("source ports: %+v", sa)
		}
		for name, b := range map[string]*ConfigBuilder{
			"BelowPrivileged": New(ScyllaV4, "h").WithShardAwareSourcePorts(80, 60000),
			"Reversed":        New(ScyllaV4, "h").WithShardAwareSourcePorts(60000, 50000),
			"WhileOff":        New(ScyllaV4, "h").WithAdvancedShardAwareness(false).WithShardAwareSourcePorts(60000, 50000),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted the source ports")
				} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithShardAwareSourcePorts" {
					t.Fatalf("Build = %v, want a WithShardAwareSourcePorts ValidationError", err)
				}
			})
		}
	})

	t.Run("OnlyHost", func(t *testing.T) {
		id := cql.MustParseUUID("00112233-4455-4677-8899-aabbccddeeff")
		other := cql.MustParseUUID("10112233-4455-4677-8899-aabbccddeeff")
		keep := mustBuildConfig(t, New(ScyllaV4, "h").WithOnlyHost(id)).settings.HostFilter
		if keep == nil || !keep(metadata.HostSnapshot{ID: id}) || keep(metadata.HostSnapshot{ID: other}) {
			t.Fatal("WithOnlyHost did not keep exactly its host")
		}
		keep = mustBuildConfig(t, New(ScyllaV4, "h").WithOnlyHost(id).
			WithHostFilter(func(h metadata.HostSnapshot) bool { return h.Datacenter == "dc1" })).settings.HostFilter
		if !keep(metadata.HostSnapshot{ID: id, Datacenter: "dc1"}) || keep(metadata.HostSnapshot{ID: id, Datacenter: "dc2"}) ||
			keep(metadata.HostSnapshot{ID: other, Datacenter: "dc1"}) {
			t.Fatal("WithOnlyHost and WithHostFilter must both pass")
		}
		if _, err := New(ScyllaV4, "h").WithOnlyHost(cql.UUID{}).Build(); err == nil {
			t.Fatal("Build accepted an empty host ID")
		} else if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
			t.Fatalf("Build = %v, want a ValidationError", err)
		}
	})

	t.Run("PoolSize", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithConnectionsPerHost(3)).settings
		if c.Pool.ConnectionsPerHost != 3 || c.Pool.ConnectionsPerShard != 0 || c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatalf("WithConnectionsPerHost(3) pool = %+v, want 3 per host without the shard-aware port", c.Pool)
		}
		c = mustBuildConfig(t, New(ScyllaV4, "h").WithConnectionsPerHost(3).WithAdvancedShardAwareness(false)).settings
		if c.Pool.ConnectionsPerHost != 3 || c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatalf("PerHost with WithAdvancedShardAwareness(false): %+v", c.Pool)
		}
		c = mustBuildConfig(t, New(ScyllaV4, "h").WithConnectionsPerHost(3).WithConnectionsPerShard(2).
			WithAdvancedShardAwareness(true).WithMaxExcessPerShard(1)).settings
		if c.Pool.ConnectionsPerShard != 2 || c.Pool.ConnectionsPerHost != 0 || !c.Pool.AdvancedShardAwareness.Enabled || c.Pool.MaxExcessPerShard != 1 {
			t.Fatalf("PerShard after PerHost: %+v", c.Pool)
		}
		for name, b := range map[string]*ConfigBuilder{
			"AdvancedShardAwareness": New(ScyllaV4, "h").WithAdvancedShardAwareness(true).WithConnectionsPerHost(2),
			"MaxExcessPerShard":      New(ScyllaV4, "h").WithConnectionsPerHost(2).WithMaxExcessPerShard(2),
			"Zero":                   New(ScyllaV4, "h").WithConnectionsPerHost(0),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted the pool size")
				} else if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
					t.Fatalf("Build = %v, want a ValidationError", err)
				}
			})
		}
	})

	t.Run("BuildSnapshotsBuilder", func(t *testing.T) {
		b := New(ScyllaV4, "h").WithMetadataKeyspaces("a")
		first := mustBuildConfig(t, b)
		b.WithMetadataKeyspaces("b", "c").WithPartitioner("X", routing.Murmur3TokenPartitioner)
		second := mustBuildConfig(t, b)
		if len(first.settings.Metadata.Keyspaces) != 1 || first.settings.Partitioners["X"] != nil {
			t.Fatal("changing the builder changed an already built Config")
		}
		if len(second.settings.Metadata.Keyspaces) != 2 || second.settings.Partitioners["X"] == nil {
			t.Fatal("changes missing from the second Build")
		}
	})

	t.Run("DefaultProfileMustBeBuilt", func(t *testing.T) {
		_, err := New(ScyllaV4, "h").WithDefaultProfile(Profile{PageSize: 1000}).Build()
		if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "Profile" || !strings.Contains(err.Error(), "NewProfile") {
			t.Fatalf("Build with a Profile literal = %v, want a profile ValidationError naming NewProfile", err)
		}
		_, err = New(ScyllaV4, "h").WithDefaultProfile(Profile{PageSize: 1000}).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) }).Build()
		if err == nil {
			t.Fatal("ConfigureDefaultProfile made a Profile literal acceptable")
		}
		built := codecTestMust(NewProfile().Build())
		edited := built
		edited.PageSize = 1000
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithDefaultProfile(edited).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) })).settings
		if c.DefaultProfile.PageSize != 1000 || c.DefaultProfile.Consistency != cql.One || c.DefaultProfile.Retry == nil {
			t.Fatalf("edited copy of a built profile: %+v", *c.DefaultProfile)
		}
	})

	t.Run("DefaultProfileSnapshotsAndReplaces", func(t *testing.T) {
		profile := codecTestMust(NewProfile().WithConsistency(cql.Quorum).WithPageSize(7).WithTimeout(0).Build())
		b := New(ScyllaV4, "h").WithDefaultProfile(profile)
		other := New(ScyllaV4, "h").WithDefaultProfile(profile)
		profile.PageSize = 99
		first := mustBuildConfig(t, b).settings
		if first.DefaultProfile.PageSize != 7 || first.DefaultProfile.Timeout != 0 {
			t.Fatal("WithDefaultProfile did not snapshot the complete profile")
		}
		b.ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(11) })
		if got := mustBuildConfig(t, other).settings.DefaultProfile.PageSize; got != 7 {
			t.Fatalf("configuring another builder changed the shared profile: %d", got)
		}
		replacement := codecTestMust(NewProfile().WithConsistency(cql.One).Build())
		replaced := mustBuildConfig(t, b.WithDefaultProfile(replacement)).settings.DefaultProfile
		if replaced.Consistency != cql.One || replaced.PageSize != 5000 || replaced.Timeout == 0 {
			t.Fatalf("WithDefaultProfile merged instead of replacing: %+v", replaced)
		}
		invalid := replacement
		invalid.PageSize = 0
		if _, err := b.WithDefaultProfile(invalid).Build(); err == nil {
			t.Fatal("invalid profile accepted")
		}
		mustBuildConfig(t, b.WithDefaultProfile(replacement))
		if first.DefaultProfile.PageSize != 7 {
			t.Fatal("later changes reached the built configuration")
		}
	})

	t.Run("DefaultProfileChoicesAccumulate", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.Quorum) }).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(7) })).settings
		if c.DefaultProfile.Consistency != cql.Quorum || c.DefaultProfile.PageSize != 7 {
			t.Fatalf("profile = %+v", *c.DefaultProfile)
		}
		_, err := New(ScyllaV4, "h").ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(0) }).Build()
		if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithPageSize" {
			t.Fatalf("invalid profile error = %v", err)
		}
	})

	t.Run("PartitionerAddsToBuiltins", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithPartitioner("Custom", routing.Murmur3TokenPartitioner)).settings
		if c.Partitioners["Custom"] == nil || c.Partitioners["Murmur3Partitioner"] == nil {
			t.Fatalf("partitioners = %v", c.Partitioners)
		}
	})

	t.Run("IgnoreEvents", func(t *testing.T) {
		m := mustBuildConfig(t, New(ScyllaV4, "h").WithIgnoreEvents(event.Schema, event.Status)).settings.Metadata
		if m.SchemaEvents || m.StatusEvents || !m.TopologyEvents || !m.ClientRouteEvents {
			t.Fatalf("events = %+v", m)
		}
		if _, err := New(ScyllaV4, "h").WithIgnoreEvents(event.Overflow).Build(); err == nil {
			t.Fatal("IgnoreEvents accepted a client-side event")
		}
	})

	t.Run("DisableMethods", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithHeartbeat(0, 0).WithMetadataRefresh(0).WithPreparedCacheSize(0).WithTimestampGenerator(nil).
			WithConfigReport(false).WithDiscovery(false).WithAutomaticSchemaAgreement(false).WithRefreshAfterAgreement(false).WithAdvancedShardAwareness(false).WithNagle(true)).settings
		if c.KeepaliveInterval != 0 || c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.Timestamps.Mode() != implSettings.ServerTimestampMode ||
			c.ReportConfiguration || c.Metadata.InitialDiscovery || c.Metadata.AutomaticAgreement || c.Metadata.RefreshAfterAgreement || c.Pool.AdvancedShardAwareness.Enabled || c.NoDelay {
			t.Fatalf("a disabling setting left its feature on: %+v", c)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithNagle(true).WithNagle(false)).settings; !c.NoDelay {
			t.Fatal("WithNagle(false) did not set TCP_NODELAY")
		}
	})

	t.Run("StringHidesCredentials", func(t *testing.T) {
		s := mustBuildConfig(t, New(ScyllaV4, "h").WithAuth(auth.Password("user", "secret-password"))).String()
		if strings.Contains(s, "secret-password") || !strings.Contains(s, "auth=true") {
			t.Fatalf("String() = %q", s)
		}
		secrets := []string{"secret-password", "secret-user", "secret-cert-bytes", "secret-key", "secret-server-name", "secret-route-id", "secret-proxy", "secret-dialer-token", "secret-translator"}
		tlsConfig := &tls.Config{ServerName: "secret-server-name", Certificates: []tls.Certificate{{Certificate: [][]byte{[]byte("secret-cert-bytes")}, PrivateKey: "secret-key"}}}
		dialer := secretDialer{token: "secret-dialer-token"}
		for name, b := range map[string]*ConfigBuilder{
			"TLS":       New(ScyllaV4, "h").WithTLS(tlsConfig),
			"TLSSource": New(ScyllaV4, "h").WithTLSSource(staticTLS{config: tlsConfig}),
			"Everything": New(ScyllaV4, "h").WithAuth(auth.Password("secret-user", "secret-password")).WithTLS(tlsConfig).WithDialer(dialer).
				WithClientRoutes(ClientRoute{ConnectionID: "secret-route-id", Hostname: "secret-proxy"}).WithApplication("app", "1"),
			"Translator": New(ScyllaV4, "h").WithDialer(dialer).WithAddressTranslator(secretTranslator("secret-translator")),
		} {
			t.Run(name, func(t *testing.T) {
				c := mustBuildConfig(t, b)
				for _, out := range []string{c.String(), fmt.Sprint(c), fmt.Sprintf("%v %+v %#v %s", c, c, c, c)} {
					for _, secret := range secrets {
						if strings.Contains(out, secret) {
							t.Errorf("formatted Config leaks %q: %s", secret, out)
						}
					}
				}
				if s := c.String(); name != "Translator" && !strings.Contains(s, "tls=true") {
					t.Errorf("String() = %q, want tls=true", s)
				}
			})
		}
		if (Config{}).String() != "config.Config{}" {
			t.Fatal("zero Config String")
		}
	})

	t.Run("SettersStoreValues", func(t *testing.T) {
		lifecycle := &lifecycleRecorder{}
		addr := netip.MustParseAddr("192.0.2.7")
		c := mustBuildConfig(t, New(ScyllaV4, "h").
			WithMaxFrameSize(1<<20).
			WithMaxDecompressedSize(2<<20).
			WithMaxMetadataDepth(32).
			WithMaxPagingStates(100).
			WithMaxPagingStateSize(4096).
			WithLocalAddress(addr).
			WithDNSTimeout(3*time.Second).
			WithTCPKeepalive(15*time.Second).
			WithReceiveBuffer(1024).
			WithSendBuffer(2048).
			WithLinger(2*time.Second).
			WithMaxRequestsPerConnection(128).
			WithMaxOrphanedStreams(0).
			WithLifecycleObserver(lifecycle)).settings
		for name, ok := range map[string]bool{
			"WithMaxFrameSize":             c.Limits.MaxFrameBytes == 1<<20,
			"WithMaxDecompressedSize":      c.Limits.MaxDecompressedBytes == 2<<20,
			"WithMaxMetadataDepth":         c.Limits.MaxMetadataDepth == 32,
			"WithMaxPagingStates":          c.Limits.MaxPagingStates == 100,
			"WithMaxPagingStateSize":       c.Limits.MaxPagingStateBytes == 4096,
			"WithLocalAddress":             c.LocalAddress == addr,
			"WithDNSTimeout":               c.DNSTimeout == 3*time.Second,
			"WithTCPKeepalive":             c.SocketKeepalive == 15*time.Second,
			"WithReceiveBuffer":            c.ReceiveBuffer == 1024,
			"WithSendBuffer":               c.SendBuffer == 2048,
			"WithLinger":                   c.Linger == cql.Some(2*time.Second),
			"WithMaxRequestsPerConnection": c.Pool.MaxRequestsPerConnection == 128,
			"WithMaxOrphanedStreams":       c.Pool.MaxOrphanedStreams == 0,
			"WithLifecycleObserver":        c.LifecycleObserver == observe.LifecycleObserver(lifecycle),
		} {
			if !ok {
				t.Errorf("%s: the built configuration does not carry the chosen value", name)
			}
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithLinger(0)).settings; c.Linger != cql.Some(time.Duration(0)) {
			t.Errorf("WithLinger(0) = %+v, want a chosen zero linger", c.Linger)
		}
	})

	// SettersRejectInvalidValues checks that Build names the setter that
	// chose an invalid value, and the limit it broke.
	t.Run("SettersRejectInvalidValues", func(t *testing.T) {
		for name, tc := range map[string]struct {
			b      *ConfigBuilder
			setter string
			limit  string
		}{
			"MaxFrameSizeZero":                 {New(ScyllaV4, "h").WithMaxFrameSize(0), "WithMaxFrameSize", "1 to 2147483647 bytes"},
			"MaxFrameSizeOver2GiB":             {New(ScyllaV4, "h").WithMaxFrameSize(1 << 31), "WithMaxFrameSize", "1 to 2147483647 bytes"},
			"MaxDecompressedSizeNegative":      {New(ScyllaV4, "h").WithMaxDecompressedSize(-1), "WithMaxDecompressedSize", "1 to 2147483647 bytes"},
			"MaxDecompressedSizeOver2GiB":      {New(ScyllaV4, "h").WithMaxDecompressedSize(1 << 31), "WithMaxDecompressedSize", "1 to 2147483647 bytes"},
			"MaxMetadataDepthZero":             {New(ScyllaV4, "h").WithMaxMetadataDepth(0), "WithMaxMetadataDepth", "1 to 128"},
			"MaxMetadataDepthOver128":          {New(ScyllaV4, "h").WithMaxMetadataDepth(129), "WithMaxMetadataDepth", "1 to 128"},
			"MaxPagingStatesZero":              {New(ScyllaV4, "h").WithMaxPagingStates(0), "WithMaxPagingStates", "positive"},
			"MaxPagingStateSizeZero":           {New(ScyllaV4, "h").WithMaxPagingStateSize(0), "WithMaxPagingStateSize", "positive"},
			"DNSTimeoutNegative":               {New(ScyllaV4, "h").WithDNSTimeout(-time.Second), "WithDNSTimeout", "negative"},
			"ConnectTimeoutNegative":           {New(ScyllaV4, "h").WithConnectTimeout(-time.Second), "WithConnectTimeout", "negative"},
			"TCPKeepaliveNegative":             {New(ScyllaV4, "h").WithTCPKeepalive(-time.Second), "WithTCPKeepalive", "negative"},
			"ReceiveBufferNegative":            {New(ScyllaV4, "h").WithReceiveBuffer(-1), "WithReceiveBuffer", "negative"},
			"SendBufferNegative":               {New(ScyllaV4, "h").WithSendBuffer(-1), "WithSendBuffer", "negative"},
			"LingerNegative":                   {New(ScyllaV4, "h").WithLinger(-time.Second), "WithLinger", "whole seconds"},
			"LingerFractionalSeconds":          {New(ScyllaV4, "h").WithLinger(1500 * time.Millisecond), "WithLinger", "whole seconds"},
			"ConnectionsPerShardNegative":      {New(ScyllaV4, "h").WithConnectionsPerShard(-1), "WithConnectionsPerShard", "positive"},
			"ConnectionsPerHostZero":           {New(ScyllaV4, "h").WithConnectionsPerHost(0), "WithConnectionsPerShard", "positive"},
			"MaxExcessPerShardNegative":        {New(ScyllaV4, "h").WithMaxExcessPerShard(-1), "WithMaxExcessPerShard", "negative"},
			"MaxRequestsPerConnectionZero":     {New(ScyllaV4, "h").WithMaxRequestsPerConnection(0), "WithMaxRequestsPerConnection", "1 to 32768"},
			"MaxRequestsPerConnectionOver32Ki": {New(ScyllaV4, "h").WithMaxRequestsPerConnection(32769), "WithMaxRequestsPerConnection", "1 to 32768"},
			"MaxOrphanedStreamsNegative":       {New(ScyllaV4, "h").WithMaxOrphanedStreams(-1), "WithMaxOrphanedStreams", "negative"},
			"PreparedCacheSizeNegative":        {New(ScyllaV4, "h").WithPreparedCacheSize(-1), "WithPreparedCacheSize", "negative"},
			"MetadataKeyspacesRepeated":        {New(ScyllaV4, "h").WithMetadataKeyspaces("ks", "ks"), "WithMetadataKeyspaces", "repeated"},
			"SchemaAgreementZero":              {New(ScyllaV4, "h").WithSchemaAgreement(0, time.Second), "WithSchemaAgreement", "positive"},
			"HeartbeatWithoutTimeout":          {New(ScyllaV4, "h").WithHeartbeat(time.Second, 0), "WithHeartbeat", "positive"},
			"ProfilePageSize":                  {New(ScyllaV4, "h").ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(-1) }), "WithPageSize", "positive"},
			"ProfileTimeout":                   {New(ScyllaV4, "h").ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithTimeout(-1) }), "WithTimeout", "negative"},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := tc.b.Build()
				if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != tc.setter || !strings.Contains(err.Error(), tc.limit) {
					t.Fatalf("Build = %v, want a ValidationError for %s saying %q", err, tc.setter, tc.limit)
				}
			})
		}
		mustBuildConfig(t, New(ScyllaV4, "h").WithMaxFrameSize(1<<31-1).WithMaxDecompressedSize(1<<31-1).WithMaxMetadataDepth(128).WithMaxRequestsPerConnection(32768))
	})

	// NilRestoresDefault checks that a nil policy means the driver's default
	// in every setter but WithTimestampGenerator.
	t.Run("NilRestoresDefault", func(t *testing.T) {
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithReconnect(declineReconnect{}).WithReconnect(nil).
			WithConviction(endpoint.SimpleConvictionPolicy{}).WithConviction(nil).
			WithDialer(secretDialer{}).WithDialer(nil).
			ConfigureDefaultProfile(func(p *ProfileBuilder) {
				p.WithRetry(retry.Never{}).WithRetry(nil).WithRouting(routing.SingleTarget(metadata.Target{})).WithRouting(nil)
			})).settings
		if !implSettings.IsDefaultReconnect(c.Reconnect) || c.Conviction != nil || c.Dialer != nil {
			t.Fatalf("nil did not restore the default: reconnect %T, conviction %T, dialer %T", c.Reconnect, c.Conviction, c.Dialer)
		}
		if p := c.DefaultProfile; p.Retry != (retry.Safe{MaxAttempts: 3}) || p.Routing == nil {
			t.Fatalf("nil did not restore the default profile policies: retry %v, routing %v", p.Retry, p.Routing)
		}
		if p := codecTestMust(NewProfile().WithRouting(nil).Build()); p.Routing == nil {
			t.Fatal("WithRouting(nil) built a profile without a routing policy")
		}
		// A built profile copied with nil policies converts to the defaults.
		edited := codecTestMust(NewProfile().WithRetry(retry.Never{}).Build())
		edited.Retry, edited.Routing = nil, nil
		if p := internalProfile(edited); p.Retry != (retry.Safe{MaxAttempts: 3}) || p.Routing == nil {
			t.Fatalf("a nil Profile policy did not convert to the default: retry %v, routing %v", p.Retry, p.Routing)
		}
	})

	t.Run("ReconnectProbedOnceInBuild", func(t *testing.T) {
		policy := &countingReconnect{delay: 2 * time.Second, again: true}
		c := mustBuildConfig(t, New(ScyllaV4, "h").WithReconnect(policy)).settings
		if policy.calls != 1 || c.ReconnectFirst != (implSettings.ReconnectProbe{Delay: 2 * time.Second, Again: true, Set: true}) {
			t.Fatalf("Delay calls %d, probe %+v; want one call and its answer kept", policy.calls, c.ReconnectFirst)
		}
		if c := mustBuildConfig(t, New(ScyllaV4, "h").WithReconnect(declineReconnect{})).settings; c.ReconnectFirst.Again {
			t.Fatal("a declining reconnect policy reconnects")
		}
		_, err := New(ScyllaV4, "h").WithReconnect(&countingReconnect{delay: -time.Second}).Build()
		if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "WithReconnect" {
			t.Fatalf("Build with a negative first delay = %v, want a WithReconnect ValidationError", err)
		}
	})

	t.Run("ContactPoints", func(t *testing.T) {
		for name, b := range map[string]*ConfigBuilder{
			"HostNames":          New(ScyllaV4, "a.example", "b.example:9042"),
			"IPv6":               New(ScyllaV4, "::1", "[::2]:9042", "fe80::1%eth0"),
			"MixedPorts":         New(ScyllaV4, "a:19042", "b:19043"),
			"SomeNamePorts":      New(ScyllaV4, "a:19042", "b"),
			"PortMatches":        New(ScyllaV4, "a:19042", "b:19042").WithPort(19042),
			"NoDiscovery":        New(ScyllaV4, "a:19042").WithDiscovery(false),
			"ClientRoutes":       New(ScyllaV4, "a:19042").WithClientRoutes(ClientRoute{ConnectionID: "c1"}),
			"UnixSocket":         New(ScyllaV4, "unix:/var/lib/scylla/cql.m"),
			"DefaultPortMatches": New(ScyllaV4, "a:9042"),
		} {
			t.Run(name, func(t *testing.T) { mustBuildConfig(t, b) })
		}
		for name, tc := range map[string]struct {
			b      *ConfigBuilder
			setter string
		}{
			"PortNotNumber":   {New(ScyllaV4, "h:abc"), "WithContactPoints"},
			"PortZero":        {New(ScyllaV4, "h:0"), "WithContactPoints"},
			"PortTooLarge":    {New(ScyllaV4, "h:65536"), "WithContactPoints"},
			"NoHost":          {New(ScyllaV4, ":9042"), "WithContactPoints"},
			"Empty":           {New(ScyllaV4, ""), "WithContactPoints"},
			"BracketsNoPort":  {New(ScyllaV4, "[::1]"), "WithContactPoints"},
			"OtherCommonPort": {New(ScyllaV4, "a:19042", "b:19042"), "WithPort"},
			"OtherPortOnce":   {New(ScyllaV4).WithContactPoints("a:19042"), "WithPort"},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := tc.b.Build()
				if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != tc.setter {
					t.Fatalf("Build = %v, want a %s ValidationError", err, tc.setter)
				}
			})
		}
	})

	t.Run("ProfileSettersStoreValues", func(t *testing.T) {
		speculation := fixedSpeculation{delay: 50 * time.Millisecond}
		p := codecTestMust(NewProfile().WithPageTimeout(2 * time.Second).WithSpeculation(speculation).Build())
		if p.PageTimeout != 2*time.Second || p.Speculation != retry.SpeculationPolicy(speculation) {
			t.Fatalf("profile = %+v", p)
		}
		if d, ok := p.Speculation.Delay(1); !ok || d != 50*time.Millisecond {
			t.Fatalf("Speculation.Delay = %v, %v", d, ok)
		}
		if p := codecTestMust(NewProfile().WithSpeculation(speculation).WithSpeculation(nil).Build()); p.Speculation != nil {
			t.Fatalf("WithSpeculation(nil) kept %v", p.Speculation)
		}
		if _, err := NewProfile().WithPageTimeout(-time.Second).Build(); err == nil {
			t.Fatal("negative page timeout accepted")
		}
		c := mustBuildConfig(t, New(ScyllaV4, "h").ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageTimeout(time.Second) })).settings
		if c.DefaultProfile.PageTimeout != time.Second {
			t.Fatalf("default profile PageTimeout = %v", c.DefaultProfile.PageTimeout)
		}
	})

	t.Run("ProfileBuild", func(t *testing.T) {
		p, err := NewProfile().WithConsistency(cql.One).NoSerialConsistency().WithPrefetch(0).Build()
		if err != nil {
			t.Fatal(err)
		}
		if p.Consistency != cql.One || p.SerialConsistency.Set || p.Prefetch != 0 || p.Routing == nil {
			t.Fatalf("profile = %+v", p)
		}
		if _, err := NewProfile().WithSerialConsistency(cql.Quorum).Build(); err == nil {
			t.Fatal("non-serial serial consistency accepted")
		}
	})
}

type secretDialer struct{ token string }

func (secretDialer) DialContext(context.Context, endpoint.DialTarget) (net.Conn, error) {
	return nil, errors.New("not dialed")
}

func secretTranslator(secret string) endpoint.Translator {
	return endpoint.TranslatorFunc(func(_ context.Context, _ endpoint.Peer, addr netip.AddrPort) (netip.AddrPort, error) {
		_ = secret
		return addr, nil
	})
}

type lifecycleRecorder struct{ events []observe.LifecycleEvent }

func (r *lifecycleRecorder) ObserveLifecycle(e observe.LifecycleEvent) {
	r.events = append(r.events, e)
}

type countingReconnect struct {
	calls int
	delay time.Duration
	again bool
}

func (p *countingReconnect) Delay(int) (time.Duration, bool) {
	p.calls++
	return p.delay, p.again
}

// declineReconnect never reconnects, the documented way to turn
// reconnection off.
type declineReconnect struct{}

func (declineReconnect) Delay(int) (time.Duration, bool) { return 0, false }

type fixedSpeculation struct{ delay time.Duration }

func (s fixedSpeculation) Delay(int) (time.Duration, bool) { return s.delay, true }

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
