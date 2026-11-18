// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"context"
	"crypto/tls"
	"errors"
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
// DefaultConfig call, so the remaining settings compare with DeepEqual.
func comparable(c implSettings.Config) implSettings.Config {
	profile := *c.DefaultProfile
	profile.Routing = nil
	c.DefaultProfile = &profile
	return c
}

func TestConfigBuilder(t *testing.T) {
	t.Run("DefaultsMatchDriverDefaults", func(t *testing.T) {
		got := *mustBuildConfig(t, New("10.0.0.1")).settings
		want := implSettings.DefaultConfig("10.0.0.1")
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
		b := New("h").WithPort(0).WithConnectionsPerShard(-1)
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted an invalid port")
		} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "port" {
			t.Fatalf("Build = %v, want a port ValidationError", err)
		}
		b.WithPort(9042)
		if _, err := b.Build(); err == nil {
			t.Fatal("correcting the port hid the invalid pool size")
		}
		b.WithConnectionsPerShard(2)
		mustBuildConfig(t, b)
	})

	t.Run("ApplicationVersionRequiresName", func(t *testing.T) {
		b := New("h").WithApplication("", "1.0.0")
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted an application version without a name")
		} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "application" {
			t.Fatalf("Build = %v, want an application ValidationError", err)
		}
		mustBuildConfig(t, b.WithApplication("app", "1.0.0"))
		mustBuildConfig(t, b.WithApplication("", ""))
	})

	t.Run("NoContactPoints", func(t *testing.T) {
		if _, err := New().Build(); err == nil || !strings.Contains(err.Error(), "contact points") {
			t.Fatalf("Build without contact points = %v", err)
		}
	})

	t.Run("LastCallWins", func(t *testing.T) {
		c := mustBuildConfig(t, New("h").WithConnectionsPerShard(3).WithConnectionsPerShard(4).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) }).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.All) }).
			WithClientRoutes(mustRoutes(NewClientRoutes().WithRoute("c1", "a").WithRoute("c2", "x"))).
			WithClientRoutes(mustRoutes(NewClientRoutes().WithRoute("c1", "a").WithRoute("c1", "b"))).
			NoAdvancedShardAwareness()).settings
		if c.Pool.ConnectionsPerShard != 4 || c.DefaultProfile.Consistency != cql.All {
			t.Fatalf("pool %+v consistency %v", c.Pool, c.DefaultProfile.Consistency)
		}
		// The last WithClientRoutes replaces every route and override; the
		// last WithRoute for an ID replaces its hostname.
		if r := c.ClientRoutes; !reflect.DeepEqual(r.ConnectionIDs, []string{"c1"}) || !reflect.DeepEqual(r.HostnameOverrides, map[string]string{"c1": "b"}) {
			t.Fatalf("client routes = %+v", *r)
		}
	})

	t.Run("ExclusiveOptionsReplace", func(t *testing.T) {
		c := mustBuildConfig(t, New("h").
			WithHeartbeat(time.Second, time.Second).WithHeartbeat(0, 0).
			WithConnectionsPerHost(4).WithConnectionsPerShard(2).
			WithAdvancedShardAwareness(50000, 60000).NoAdvancedShardAwareness().
			WithMetadataRefresh(time.Second).WithMetadataRefresh(0).
			WithPreparedCacheSize(10).WithPreparedCacheSize(0).
			WithTimestampGenerator(func() int64 { return 1 }).WithServerTimestamps()).settings
		if c.KeepaliveInterval != 0 || c.Pool.ConnectionsPerHost != 0 || c.Pool.ConnectionsPerShard != 2 || c.Pool.AdvancedShardAwareness.Enabled ||
			c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.Timestamps.Mode() != implSettings.ServerTimestampMode {
			t.Fatalf("a later call did not replace an earlier one: %+v", c)
		}
		if c := mustBuildConfig(t, New("h")).settings; c.Timestamps.Mode() != implSettings.DriverTimestampMode {
			t.Fatalf("default timestamps: mode %d", c.Timestamps.Mode())
		}
		if c := mustBuildConfig(t, New("h").WithTimestampGenerator(func() int64 { return 7 })).settings; c.Timestamps.Mode() != implSettings.GeneratedTimestampMode || c.Timestamps.Generator()() != 7 {
			t.Fatalf("a user timestamp generator: mode %d", c.Timestamps.Mode())
		}
		if c := mustBuildConfig(t, New("h").WithTimestampGenerator(nil)).settings; c.Timestamps.Mode() != implSettings.ServerTimestampMode {
			t.Fatalf("a nil timestamp generator: mode %d", c.Timestamps.Mode())
		}
		c = mustBuildConfig(t, New("h").WithHeartbeat(0, 0).WithHeartbeat(time.Second, 2*time.Second).WithConnectionsPerShard(2).WithConnectionsPerHost(4)).settings
		if c.KeepaliveInterval != time.Second || c.KeepaliveTimeout != 2*time.Second || c.Pool.ConnectionsPerHost != 4 {
			t.Fatalf("reverse order: %+v", c)
		}
	})

	// DocumentedDefaults pins the default each setter's doc names, so a
	// changed default fails here until its doc is updated too.
	t.Run("DocumentedDefaults", func(t *testing.T) {
		c := mustBuildConfig(t, New("h")).settings
		p := c.DefaultProfile
		for name, ok := range map[string]bool{
			"WithPort 9042":                                  c.Port == 9042,
			"WithKeyspace none":                              c.Keyspace == "",
			"WithAuth none":                                  c.Auth == nil,
			"WithTLS plaintext":                              c.TLS == nil && c.TLSSource == nil,
			"WithCompression none":                           c.Compression == cql.NoCompression,
			"WithCQLVersion 3.0.0":                           c.CQLVersion == "3.0.0",
			"WithMaxFrameSize 256 MiB":                       c.Limits.MaxFrameBytes == 256<<20,
			"WithMaxDecompressedSize 256 MiB":                c.Limits.MaxDecompressedBytes == 256<<20,
			"WithMaxMetadataDepth 64":                        c.Limits.MaxMetadataDepth == 64,
			"WithMaxPagingStates 65536":                      c.Limits.MaxPagingStates == 65536,
			"WithMaxPagingStateSize 16 MiB":                  c.Limits.MaxPagingStateBytes == 16<<20,
			"WithDialer net.Dialer":                          c.Dialer == nil,
			"WithResolver net.DefaultResolver":               c.DNSResolver == nil,
			"WithLocalAddress any":                           !c.LocalAddress.IsValid(),
			"WithConnectTimeout 5s":                          c.ConnectTimeout == 5*time.Second,
			"WithDNSTimeout 5s":                              c.DNSTimeout == 5*time.Second,
			"WithNagle off":                                  c.NoDelay,
			"WithTCPKeepalive off":                           c.SocketKeepalive == 0,
			"WithSocketBuffers the OS's":                     !c.ReceiveBuffer.Set && !c.SendBuffer.Set,
			"WithLinger the OS's":                            !c.Linger.Set,
			"WithReadTimeout none":                           c.ReadTimeout == 0,
			"WithWriteTimeout none":                          c.WriteTimeout == 0,
			"WithHeartbeat 30s and 30s":                      c.KeepaliveInterval == 30*time.Second && c.KeepaliveTimeout == 30*time.Second,
			"WithConnectionsPerShard 1":                      c.Pool.ConnectionsPerShard == 1 && c.Pool.ConnectionsPerHost == 0,
			"WithMaxExcessPerShard 2":                        c.Pool.MaxExcessPerShard == 2,
			"WithMaxRequestsPerConnection 32768":             c.Pool.MaxRequestsPerConnection == 32768,
			"WithMaxOrphanedStreams 1024":                    c.Pool.MaxOrphanedStreams == 1024,
			"WithAdvancedShardAwareness on 49152-65535":      c.Pool.AdvancedShardAwareness == implSettings.AdvancedShardAwarenessOptions{Enabled: true, SourcePortStart: 49152, SourcePortEnd: 65535},
			"WithDiscovery true":                             c.Metadata.InitialDiscovery,
			"WithHostFilter all nodes":                       c.HostFilter == nil,
			"WithAddressTranslator identity":                 c.AddressTranslator == nil,
			"WithClientRoutes none":                          c.ClientRoutes == nil,
			"WithReconnect jittered backoff":                 c.Reconnect != nil,
			"WithConviction SimpleConvictionPolicy":          c.Conviction == nil,
			"WithMetadataMode SchemaFull":                    c.Metadata.Mode == metadata.SchemaFull,
			"WithMetadataKeyspaces all":                      len(c.Metadata.Keyspaces) == 0,
			"WithPartitioner built-ins only":                 len(c.Partitioners) == 4,
			"WithMetadataRefresh 60s":                        c.Metadata.PeriodicRefresh == 60*time.Second,
			"WithMetadataTimeout 31s":                        c.Metadata.RequestTimeout == 31*time.Second,
			"WithMetadataServerTimeout unset":                !c.Metadata.ServerTimeout.Set,
			"WithIgnoreEvents none ignored":                  c.Metadata.TopologyEvents && c.Metadata.StatusEvents && c.Metadata.SchemaEvents && c.Metadata.ClientRouteEvents,
			"WithSchemaAgreement 60s and 200ms":              c.Metadata.AgreementTimeout == 60*time.Second && c.Metadata.AgreementPoll == 200*time.Millisecond,
			"WithAutomaticSchemaAgreement true":              c.Metadata.AutomaticAgreement,
			"WithRefreshAfterAgreement true":                 c.Metadata.RefreshAfterAgreement,
			"WithPreparedCacheSize 1000":                     c.PreparedCacheSize == 1000,
			"WithTimestampGenerator driver stamps":           c.Timestamps.Mode() == implSettings.DriverTimestampMode,
			"WithApplication none":                           c.Identity.ApplicationName == "" && c.Identity.ApplicationVersion == "",
			"WithClientID none":                              c.Identity.ClientID == "",
			"WithLogger slog.Default":                        c.Logger == nil,
			"WithObserver none":                              c.Observer == nil,
			"WithLifecycleObserver none":                     c.LifecycleObserver == nil,
			"WithConfigReport true":                          c.ReportConfiguration,
			"Profile WithConsistency LOCAL_QUORUM":           p.Consistency == cql.LocalQuorum,
			"Profile WithSerialConsistency LOCAL_SERIAL":     p.SerialConsistency == cql.Some(cql.LocalSerial),
			"Profile WithTimeout 30s":                        p.Timeout == 30*time.Second,
			"Profile WithPageTimeout none":                   p.PageTimeout == 0,
			"Profile WithPageSize 5000":                      p.PageSize == 5000,
			"Profile WithPrefetch one":                       p.Prefetch == 1,
			"Profile WithRetry retry.Safe{MaxAttempts: 3}":   p.Retry == retry.Safe{MaxAttempts: 3},
			"Profile WithRouting token-aware with shuffling": p.Routing != nil,
			"Profile WithSpeculation none":                   p.Speculation == nil,
		} {
			if !ok {
				t.Errorf("%s: the built default differs from the documented one", name)
			}
		}
	})

	t.Run("ProfileExclusiveOptionsReplace", func(t *testing.T) {
		p := codecTestMust(NewProfile().NoSerialConsistency().WithSerialConsistency(cql.Serial).NoTimeout().WithTimeout(time.Second).NoPrefetch().WithPrefetch(1).Build())
		if p.SerialConsistency != cql.Some(cql.Serial) || p.Timeout != time.Second || p.Prefetch != 1 {
			t.Fatalf("a later With call did not replace a No call: %+v", p)
		}
		p = codecTestMust(NewProfile().WithSerialConsistency(cql.Serial).NoSerialConsistency().WithTimeout(time.Second).NoTimeout().WithPrefetch(1).NoPrefetch().Build())
		if p.SerialConsistency.Set || p.Timeout != 0 || p.Prefetch != 0 {
			t.Fatalf("a later No call did not replace a With call: %+v", p)
		}
	})

	t.Run("ClientRoutesRejectAddressTranslator", func(t *testing.T) {
		routes := mustRoutes(NewClientRoutes().WithRoute("c1", ""))
		translator := endpoint.TranslatorFunc(func(_ context.Context, _ endpoint.Peer, addr netip.AddrPort) (netip.AddrPort, error) {
			return addr, nil
		})
		for name, b := range map[string]*ConfigBuilder{
			"RoutesFirst":     New("h").WithClientRoutes(routes).WithAddressTranslator(translator),
			"TranslatorFirst": New("h").WithAddressTranslator(translator).WithClientRoutes(routes),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted client routes with an address translator")
				} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "client routes" {
					t.Fatalf("Build = %v, want a client routes ValidationError", err)
				}
				mustBuildConfig(t, b.WithClientRoutes(ClientRoutes{}))
			})
		}
	})

	t.Run("InvalidLastValueReplacesPreviousValue", func(t *testing.T) {
		b := New("h").WithConnectionsPerShard(3).WithConnectionsPerShard(-1)
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
		b := New().WithContactPoints("h").
			WithHeartbeat(-time.Second, -time.Second).WithHeartbeat(0, 0).
			WithIgnoreEvents(event.Overflow).WithIgnoreEvents(event.Status).
			WithTLSSource(staticTLS{}).WithTLS(nil).
			WithClientRoutes(mustRoutes(NewClientRoutes().WithRoute("c1", "a"))).WithClientRoutes(ClientRoutes{}).
			WithMetadataKeyspaces("").WithMetadataKeyspaces().
			WithDiscovery(false).WithDiscovery(true).
			WithConfigReport(false).WithConfigReport(true).
			WithAutomaticSchemaAgreement(false).WithAutomaticSchemaAgreement(true).
			WithRefreshAfterAgreement(false).WithRefreshAfterAgreement(true).
			ConfigureDefaultProfile(func(p *ProfileBuilder) {
				p.WithPageSize(0).WithPageSize(42).NoPrefetch().WithPrefetch(1)
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
		b := New("h").WithTLS(config)
		config.ServerName = "after"
		if got := mustBuildConfig(t, b).settings.TLS.ServerName; got != "before" {
			t.Fatalf("ServerName = %q, want the value at WithTLS()", got)
		}
	})

	t.Run("TLS", func(t *testing.T) {
		source := staticTLS{config: &tls.Config{ServerName: "node"}}
		c := mustBuildConfig(t, New("h").WithTLS(&tls.Config{ServerName: "first"}).WithTLSSource(source)).settings
		if c.TLSSource != source || c.TLS == nil || c.TLS.ServerName != "node" {
			t.Fatal("the last WithTLSSource did not win or its config was not validated")
		}
		c = mustBuildConfig(t, New("h").WithTLSSource(source).WithTLS(&tls.Config{ServerName: "last"})).settings
		if c.TLS == nil || c.TLS.ServerName != "last" {
			t.Fatal("WithTLS did not replace WithTLSSource")
		}
		for name, tc := range map[string]struct {
			b    *ConfigBuilder
			want string
		}{
			"NoConfig":   {New("h").WithTLSSource(staticTLS{}), "returned no config"},
			"UnixSocket": {New("/var/lib/scylla/cql.m").WithTLSSource(source), "Unix TLS"},
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := tc.b.Build(); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Build = %v, want an error containing %q", err, tc.want)
				}
			})
		}
	})

	t.Run("UnixSocketInfersNoDiscovery", func(t *testing.T) {
		c := mustBuildConfig(t, New("/var/lib/scylla/cql.m")).settings
		if c.Metadata.InitialDiscovery {
			t.Fatal("a Unix socket kept host discovery")
		}
		if _, err := New("/var/lib/scylla/cql.m").WithTLS(&tls.Config{}).Build(); err == nil {
			t.Fatal("TLS on a Unix socket was accepted")
		}
	})

	t.Run("ClientRoutes", func(t *testing.T) {
		b := NewClientRoutes().WithRoute("c1", "a").WithRoute("c2", "")
		routes := mustRoutes(b)
		b.WithRoute("c1", "changed").WithRoute("c3", "")
		got := routes.Routes()
		if want := []ClientRoute{{ConnectionID: "c1", Hostname: "a"}, {ConnectionID: "c2"}}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Routes() = %+v, want %+v unaffected by later builder calls", got, want)
		}
		got[0].Hostname = "mutated"
		if routes.Routes()[0].Hostname != "a" {
			t.Fatal("Routes() exposed the internal slice")
		}
		r := mustBuildConfig(t, New("h").WithClientRoutes(routes)).settings.ClientRoutes
		if !reflect.DeepEqual(r.ConnectionIDs, []string{"c1", "c2"}) || !reflect.DeepEqual(r.HostnameOverrides, map[string]string{"c1": "a"}) {
			t.Fatalf("client routes settings = %+v", *r)
		}
		if len((ClientRoutes{}).Routes()) != 0 {
			t.Fatal("zero ClientRoutes has routes")
		}
		for name, b := range map[string]*ClientRoutesBuilder{
			"NoRoutes": NewClientRoutes(),
			"EmptyID":  NewClientRoutes().WithRoute("", "a"),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted the routes")
				} else if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "client routes" {
					t.Fatalf("Build = %v, want a client routes ValidationError", err)
				}
			})
		}
	})

	t.Run("ClientRoutesAdvancedShardAwareness", func(t *testing.T) {
		routes := mustRoutes(NewClientRoutes().WithRoute("c1", ""))
		if c := mustBuildConfig(t, New("h").WithClientRoutes(routes)).settings; c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("advanced shard awareness stayed on under client routes")
		}
		c := mustBuildConfig(t, New("h").WithAdvancedShardAwareness(50000, 60000).WithClientRoutes(routes)).settings
		if !c.Pool.AdvancedShardAwareness.Enabled || c.Pool.AdvancedShardAwareness.SourcePortStart != 50000 || c.Pool.AdvancedShardAwareness.SourcePortEnd != 60000 {
			t.Fatalf("explicit advanced shard awareness under client routes: %+v", c.Pool)
		}
		if c := mustBuildConfig(t, New("h").WithDefaultAdvancedShardAwareness().WithClientRoutes(routes)).settings; !c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("WithDefaultAdvancedShardAwareness did not turn it on under client routes")
		}
		if c := mustBuildConfig(t, New("h").WithDefaultAdvancedShardAwareness().NoAdvancedShardAwareness().WithClientRoutes(routes)).settings; c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("NoAdvancedShardAwareness did not win")
		}
	})

	t.Run("DefaultAdvancedShardAwareness", func(t *testing.T) {
		c := mustBuildConfig(t, New("h").WithAdvancedShardAwareness(50000, 60000).WithDefaultAdvancedShardAwareness()).settings
		if !c.Pool.AdvancedShardAwareness.Enabled || c.Pool.AdvancedShardAwareness.SourcePortStart != 49152 || c.Pool.AdvancedShardAwareness.SourcePortEnd != 65535 {
			t.Fatalf("WithDefaultAdvancedShardAwareness: %+v", c.Pool)
		}
		if c := mustBuildConfig(t, New("h").NoAdvancedShardAwareness().WithDefaultAdvancedShardAwareness()).settings; !c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatal("WithDefaultAdvancedShardAwareness did not replace NoAdvancedShardAwareness")
		}
	})

	t.Run("OnlyHost", func(t *testing.T) {
		id := cql.MustParseUUID("00112233-4455-4677-8899-aabbccddeeff")
		other := cql.MustParseUUID("10112233-4455-4677-8899-aabbccddeeff")
		keep := mustBuildConfig(t, New("h").WithOnlyHost(id)).settings.HostFilter
		if keep == nil || !keep(metadata.HostSnapshot{ID: id}) || keep(metadata.HostSnapshot{ID: other}) {
			t.Fatal("WithOnlyHost did not keep exactly its host")
		}
		keep = mustBuildConfig(t, New("h").WithOnlyHost(id).
			WithHostFilter(func(h metadata.HostSnapshot) bool { return h.Datacenter == "dc1" })).settings.HostFilter
		if !keep(metadata.HostSnapshot{ID: id, Datacenter: "dc1"}) || keep(metadata.HostSnapshot{ID: id, Datacenter: "dc2"}) ||
			keep(metadata.HostSnapshot{ID: other, Datacenter: "dc1"}) {
			t.Fatal("WithOnlyHost and WithHostFilter must both pass")
		}
		if _, err := New("h").WithOnlyHost(cql.UUID{}).Build(); err == nil {
			t.Fatal("Build accepted an empty host ID")
		} else if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
			t.Fatalf("Build = %v, want a ValidationError", err)
		}
	})

	t.Run("PoolSize", func(t *testing.T) {
		c := mustBuildConfig(t, New("h").WithConnectionsPerHost(3)).settings
		if c.Pool.ConnectionsPerHost != 3 || c.Pool.ConnectionsPerShard != 0 || c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatalf("WithConnectionsPerHost(3) pool = %+v, want 3 per host without the shard-aware port", c.Pool)
		}
		c = mustBuildConfig(t, New("h").WithConnectionsPerHost(3).NoAdvancedShardAwareness()).settings
		if c.Pool.ConnectionsPerHost != 3 || c.Pool.AdvancedShardAwareness.Enabled {
			t.Fatalf("PerHost with NoAdvancedShardAwareness: %+v", c.Pool)
		}
		c = mustBuildConfig(t, New("h").WithConnectionsPerHost(3).WithConnectionsPerShard(2).
			WithAdvancedShardAwareness(50000, 60000).WithMaxExcessPerShard(1)).settings
		if c.Pool.ConnectionsPerShard != 2 || c.Pool.ConnectionsPerHost != 0 || !c.Pool.AdvancedShardAwareness.Enabled || c.Pool.MaxExcessPerShard != 1 {
			t.Fatalf("PerShard after PerHost: %+v", c.Pool)
		}
		for name, b := range map[string]*ConfigBuilder{
			"AdvancedShardAwareness": New("h").WithAdvancedShardAwareness(50000, 60000).WithConnectionsPerHost(2),
			"MaxExcessPerShard":      New("h").WithConnectionsPerHost(2).WithMaxExcessPerShard(2),
			"Zero":                   New("h").WithConnectionsPerHost(0),
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
		b := New("h").WithMetadataKeyspaces("a")
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
		_, err := New("h").WithDefaultProfile(Profile{PageSize: 1000}).Build()
		if v, ok := errors.AsType[*cql.ValidationError](err); !ok || v.Field != "profile" || !strings.Contains(err.Error(), "NewProfile") {
			t.Fatalf("Build with a Profile literal = %v, want a profile ValidationError naming NewProfile", err)
		}
		_, err = New("h").WithDefaultProfile(Profile{PageSize: 1000}).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) }).Build()
		if err == nil {
			t.Fatal("ConfigureDefaultProfile made a Profile literal acceptable")
		}
		built := codecTestMust(NewProfile().Build())
		edited := built
		edited.PageSize = 1000
		c := mustBuildConfig(t, New("h").WithDefaultProfile(edited).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) })).settings
		if c.DefaultProfile.PageSize != 1000 || c.DefaultProfile.Consistency != cql.One || c.DefaultProfile.Retry == nil {
			t.Fatalf("edited copy of a built profile: %+v", *c.DefaultProfile)
		}
	})

	t.Run("DefaultProfileSnapshotsAndReplaces", func(t *testing.T) {
		profile := codecTestMust(NewProfile().WithConsistency(cql.Quorum).WithPageSize(7).NoTimeout().Build())
		b := New("h").WithDefaultProfile(profile)
		other := New("h").WithDefaultProfile(profile)
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
		c := mustBuildConfig(t, New("h").
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.Quorum) }).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(7) })).settings
		if c.DefaultProfile.Consistency != cql.Quorum || c.DefaultProfile.PageSize != 7 {
			t.Fatalf("profile = %+v", *c.DefaultProfile)
		}
		_, err := New("h").ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(0) }).Build()
		if err == nil || !strings.Contains(err.Error(), "page size") {
			t.Fatalf("invalid profile error = %v", err)
		}
	})

	t.Run("PartitionerAddsToBuiltins", func(t *testing.T) {
		c := mustBuildConfig(t, New("h").WithPartitioner("Custom", routing.Murmur3TokenPartitioner)).settings
		if c.Partitioners["Custom"] == nil || c.Partitioners["Murmur3Partitioner"] == nil {
			t.Fatalf("partitioners = %v", c.Partitioners)
		}
	})

	t.Run("IgnoreEvents", func(t *testing.T) {
		m := mustBuildConfig(t, New("h").WithIgnoreEvents(event.Schema, event.Status)).settings.Metadata
		if m.SchemaEvents || m.StatusEvents || !m.TopologyEvents || !m.ClientRouteEvents {
			t.Fatalf("events = %+v", m)
		}
		if _, err := New("h").WithIgnoreEvents(event.Overflow).Build(); err == nil {
			t.Fatal("IgnoreEvents accepted a client-side event")
		}
	})

	t.Run("DisableMethods", func(t *testing.T) {
		c := mustBuildConfig(t, New("h").WithHeartbeat(0, 0).WithMetadataRefresh(0).WithPreparedCacheSize(0).WithServerTimestamps().
			WithConfigReport(false).WithDiscovery(false).WithAutomaticSchemaAgreement(false).WithRefreshAfterAgreement(false).NoAdvancedShardAwareness().WithNagle(true)).settings
		if c.KeepaliveInterval != 0 || c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.Timestamps.Mode() != implSettings.ServerTimestampMode ||
			c.ReportConfiguration || c.Metadata.InitialDiscovery || c.Metadata.AutomaticAgreement || c.Metadata.RefreshAfterAgreement || c.Pool.AdvancedShardAwareness.Enabled || c.NoDelay {
			t.Fatalf("a disabling setting left its feature on: %+v", c)
		}
		if c := mustBuildConfig(t, New("h").WithNagle(true).WithNagle(false)).settings; !c.NoDelay {
			t.Fatal("WithNagle(false) did not set TCP_NODELAY")
		}
	})

	t.Run("StringHidesCredentials", func(t *testing.T) {
		s := mustBuildConfig(t, New("h").WithAuth(auth.Password("user", "secret-password"))).String()
		if strings.Contains(s, "secret-password") || !strings.Contains(s, "auth=true") {
			t.Fatalf("String() = %q", s)
		}
		if (Config{}).String() != "config.Config{}" {
			t.Fatal("zero Config String")
		}
	})

	t.Run("ProfileBuild", func(t *testing.T) {
		p, err := NewProfile().WithConsistency(cql.One).NoSerialConsistency().NoPrefetch().Build()
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

func mustRoutes(b *ClientRoutesBuilder) ClientRoutes { return codecTestMust(b.Build()) }

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
