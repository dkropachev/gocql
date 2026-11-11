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
		_, err := Connect[ScyllaV4](context.Background(), Config{})
		if _, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || !strings.Contains(err.Error(), "NewConfig") {
			t.Fatalf("Connect(Config{}) = %v, want a validation error naming NewConfig", err)
		}
	})

	t.Run("BuildValidatesFinalSettings", func(t *testing.T) {
		b := NewConfig("h").WithPort(0).WithPoolSize(PerShard(-1))
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted an invalid port")
		} else if v, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || v.Field != "port" {
			t.Fatalf("Build = %v, want a port ValidationError", err)
		}
		b.WithPort(9042)
		if _, err := b.Build(); err == nil {
			t.Fatal("correcting the port hid the invalid pool size")
		}
		b.WithPoolSize(PerShard(2))
		mustBuildConfig(t, b)
	})

	t.Run("NoContactPoints", func(t *testing.T) {
		if _, err := NewConfig().Build(); err == nil || !strings.Contains(err.Error(), "contact points") {
			t.Fatalf("Build without contact points = %v", err)
		}
	})

	t.Run("LastCallWins", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").WithPoolSize(PerShard(3)).WithPoolSize(PerShard(4)).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) }).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.All) }).
			WithClientRoutes(NewClientRoutes("c1").WithHostnameOverride("c1", "a").WithHostnameOverride("c1", "b")).
			NoAdvancedShardAwareness()).settings
		if c.Pool.ConnectionsPerShard != 4 || c.DefaultProfile.Consistency != cql.All || c.ClientRoutes.HostnameOverrides["c1"] != "b" {
			t.Fatalf("pool %+v consistency %v overrides %v", c.Pool, c.DefaultProfile.Consistency, c.ClientRoutes.HostnameOverrides)
		}
	})

	t.Run("ExclusiveOptionsReplace", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").
			WithHeartbeat(time.Second, time.Second).WithHeartbeat(0, 0).
			WithPoolSize(PerHost(4)).WithPoolSize(PerShard(2)).
			WithAdvancedShardAwareness(50000, 60000).NoAdvancedShardAwareness().
			WithMetadataRefresh(time.Second).WithMetadataRefresh(0).
			WithPreparedCacheSize(10).WithPreparedCacheSize(0).
			WithTimestampGenerator(func() int64 { return 1 }).WithServerTimestamps()).settings
		if c.KeepaliveInterval != 0 || c.Pool.ConnectionsPerHost != 0 || c.Pool.ConnectionsPerShard != 2 || c.Pool.ShardAwarePort ||
			c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.TimestampGenerator != nil {
			t.Fatalf("a later call did not replace an earlier one: %+v", c)
		}
		c = mustBuildConfig(t, NewConfig("h").WithHeartbeat(0, 0).WithHeartbeat(time.Second, 2*time.Second).WithPoolSize(PerShard(2)).WithPoolSize(PerHost(4))).settings
		if c.KeepaliveInterval != time.Second || c.KeepaliveTimeout != 2*time.Second || c.Pool.ConnectionsPerHost != 4 {
			t.Fatalf("reverse order: %+v", c)
		}
	})

	t.Run("InvalidLastValueReplacesPreviousValue", func(t *testing.T) {
		b := NewConfig("h").WithPoolSize(PerShard(3)).WithPoolSize(PerShard(-1))
		if got := b.settings.Pool.ConnectionsPerShard; got != -1 {
			t.Fatalf("ConnectionsPerShard = %d, want the last value -1", got)
		}
		if _, err := b.Build(); err == nil {
			t.Fatal("Build accepted the invalid last value")
		}
		b.WithPoolSize(PerHost(4))
		mustBuildConfig(t, b)
	})

	t.Run("CorrectionsAndResets", func(t *testing.T) {
		b := NewConfig().WithContactPoints("h").
			WithHeartbeat(-time.Second, -time.Second).WithHeartbeat(0, 0).
			WithIgnoreEvents(OverflowEvent).WithIgnoreEvents(StatusEvent).
			WithTLS(StaticTLS(nil)).WithTLS(nil).
			WithClientRoutes(NewClientRoutes()).WithClientRoutes(nil).
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
		b := NewConfig("h").WithTLS(StaticTLS(config))
		config.ServerName = "after"
		if got := mustBuildConfig(t, b).settings.TLS.ServerName; got != "before" {
			t.Fatalf("ServerName = %q, want the value at StaticTLS()", got)
		}
	})

	t.Run("TLS", func(t *testing.T) {
		source := StaticTLS(&tls.Config{ServerName: "node"})
		c := mustBuildConfig(t, NewConfig("h").WithTLS(StaticTLS(&tls.Config{ServerName: "first"})).WithTLS(source)).settings
		if c.TLSSource != source || c.TLS == nil || c.TLS.ServerName != "node" {
			t.Fatal("the last WithTLS did not win or its config was not validated")
		}
		for name, tc := range map[string]struct {
			b    *ConfigBuilder
			want string
		}{
			"NoConfig":         {NewConfig("h").WithTLS(StaticTLS(nil)), "returned no config"},
			"UnixSocket":       {NewConfig("/var/lib/scylla/cql.m").WithTLS(source), "Unix TLS"},
			"UnverifiedRoutes": {NewConfig("h").WithClientRoutes(NewClientRoutes("c").WithTLS()).WithTLS(StaticTLS(&tls.Config{InsecureSkipVerify: true})), "client routes TLS"},
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := tc.b.Build(); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Build = %v, want an error containing %q", err, tc.want)
				}
			})
		}
	})

	t.Run("UnixSocketInfersNoDiscovery", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("/var/lib/scylla/cql.m")).settings
		if c.Metadata.InitialDiscovery {
			t.Fatal("a Unix socket kept host discovery")
		}
		if _, err := NewConfig("/var/lib/scylla/cql.m").WithTLS(StaticTLS(&tls.Config{})).Build(); err == nil {
			t.Fatal("TLS on a Unix socket was accepted")
		}
	})

	t.Run("ClientRoutesInfersNoAdvancedShardAwareness", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").WithClientRoutes(NewClientRoutes("c1"))).settings
		if c.Pool.ShardAwarePort {
			t.Fatal("advanced shard awareness stayed on under client routes")
		}
		if _, err := NewConfig("h").WithAdvancedShardAwareness(49152, 65535).WithClientRoutes(NewClientRoutes("c1")).Build(); err == nil {
			t.Fatal("explicit advanced shard awareness with client routes was accepted")
		}
	})

	t.Run("OnlyHost", func(t *testing.T) {
		id := cql.MustParseUUID("00112233-4455-4677-8899-aabbccddeeff")
		other := cql.MustParseUUID("10112233-4455-4677-8899-aabbccddeeff")
		keep := mustBuildConfig(t, NewConfig("h").WithOnlyHost(id)).settings.HostFilter
		if keep == nil || !keep(routing.HostSnapshot{ID: id}) || keep(routing.HostSnapshot{ID: other}) {
			t.Fatal("WithOnlyHost did not keep exactly its host")
		}
		keep = mustBuildConfig(t, NewConfig("h").WithOnlyHost(id).
			WithHostFilter(func(h routing.HostSnapshot) bool { return h.Datacenter == "dc1" })).settings.HostFilter
		if !keep(routing.HostSnapshot{ID: id, Datacenter: "dc1"}) || keep(routing.HostSnapshot{ID: id, Datacenter: "dc2"}) ||
			keep(routing.HostSnapshot{ID: other, Datacenter: "dc1"}) {
			t.Fatal("WithOnlyHost and WithHostFilter must both pass")
		}
		if _, err := NewConfig("h").WithOnlyHost(cql.UUID{}).Build(); err == nil {
			t.Fatal("Build accepted an empty host ID")
		} else if _, ok := errors.AsType[*cqlerr.ValidationError](err); !ok {
			t.Fatalf("Build = %v, want a ValidationError", err)
		}
	})

	t.Run("PoolSize", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").WithPoolSize(PerHost(3))).settings
		if c.Pool.ConnectionsPerHost != 3 || c.Pool.ConnectionsPerShard != 0 || c.Pool.ShardAwarePort {
			t.Fatalf("PerHost(3) pool = %+v, want 3 per host without the shard-aware port", c.Pool)
		}
		c = mustBuildConfig(t, NewConfig("h").WithPoolSize(PerHost(3)).NoAdvancedShardAwareness()).settings
		if c.Pool.ConnectionsPerHost != 3 || c.Pool.ShardAwarePort {
			t.Fatalf("PerHost with NoAdvancedShardAwareness: %+v", c.Pool)
		}
		c = mustBuildConfig(t, NewConfig("h").WithPoolSize(PerHost(3)).WithPoolSize(PerShard(2)).
			WithAdvancedShardAwareness(50000, 60000).WithMaxExcessPerShard(1)).settings
		if c.Pool.ConnectionsPerShard != 2 || c.Pool.ConnectionsPerHost != 0 || !c.Pool.ShardAwarePort || c.Pool.MaxExcessPerShard != 1 {
			t.Fatalf("PerShard after PerHost: %+v", c.Pool)
		}
		for name, b := range map[string]*ConfigBuilder{
			"AdvancedShardAwareness": NewConfig("h").WithAdvancedShardAwareness(50000, 60000).WithPoolSize(PerHost(2)),
			"MaxExcessPerShard":      NewConfig("h").WithPoolSize(PerHost(2)).WithMaxExcessPerShard(2),
			"Zero":                   NewConfig("h").WithPoolSize(PerHost(0)),
			"ZeroValue":              NewConfig("h").WithPoolSize(PoolSize{}),
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := b.Build(); err == nil {
					t.Fatal("Build accepted the pool size")
				} else if _, ok := errors.AsType[*cqlerr.ValidationError](err); !ok {
					t.Fatalf("Build = %v, want a ValidationError", err)
				}
			})
		}
	})

	t.Run("BuildSnapshotsBuilder", func(t *testing.T) {
		b := NewConfig("h").WithMetadataKeyspaces("a")
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
		_, err := NewConfig("h").WithDefaultProfile(Profile{PageSize: 1000}).Build()
		if v, ok := errors.AsType[*cqlerr.ValidationError](err); !ok || v.Field != "profile" || !strings.Contains(err.Error(), "NewProfile") {
			t.Fatalf("Build with a Profile literal = %v, want a profile ValidationError naming NewProfile", err)
		}
		_, err = NewConfig("h").WithDefaultProfile(Profile{PageSize: 1000}).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) }).Build()
		if err == nil {
			t.Fatal("ConfigureDefaultProfile made a Profile literal acceptable")
		}
		built := codecTestMust(NewProfile().Build())
		edited := built
		edited.PageSize = 1000
		c := mustBuildConfig(t, NewConfig("h").WithDefaultProfile(edited).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.One) })).settings
		if c.DefaultProfile.PageSize != 1000 || c.DefaultProfile.Consistency != cql.One || c.DefaultProfile.Retry == nil {
			t.Fatalf("edited copy of a built profile: %+v", *c.DefaultProfile)
		}
	})

	t.Run("DefaultProfileSnapshotsAndReplaces", func(t *testing.T) {
		profile := codecTestMust(NewProfile().WithConsistency(cql.Quorum).WithPageSize(7).NoTimeout().Build())
		b := NewConfig("h").WithDefaultProfile(profile)
		other := NewConfig("h").WithDefaultProfile(profile)
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
		c := mustBuildConfig(t, NewConfig("h").
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithConsistency(cql.Quorum) }).
			ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(7) })).settings
		if c.DefaultProfile.Consistency != cql.Quorum || c.DefaultProfile.PageSize != 7 {
			t.Fatalf("profile = %+v", *c.DefaultProfile)
		}
		_, err := NewConfig("h").ConfigureDefaultProfile(func(p *ProfileBuilder) { p.WithPageSize(0) }).Build()
		if err == nil || !strings.Contains(err.Error(), "page size") {
			t.Fatalf("invalid profile error = %v", err)
		}
	})

	t.Run("PartitionerAddsToBuiltins", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").WithPartitioner("Custom", routing.Murmur3TokenPartitioner)).settings
		if c.Partitioners["Custom"] == nil || c.Partitioners["Murmur3Partitioner"] == nil {
			t.Fatalf("partitioners = %v", c.Partitioners)
		}
	})

	t.Run("IgnoreEvents", func(t *testing.T) {
		m := mustBuildConfig(t, NewConfig("h").WithIgnoreEvents(SchemaEvent, StatusEvent)).settings.Metadata
		if m.SchemaEvents || m.StatusEvents || !m.TopologyEvents || !m.ClientRouteEvents {
			t.Fatalf("events = %+v", m)
		}
		if _, err := NewConfig("h").WithIgnoreEvents(OverflowEvent).Build(); err == nil {
			t.Fatal("IgnoreEvents accepted a client-side event")
		}
	})

	t.Run("DisableMethods", func(t *testing.T) {
		c := mustBuildConfig(t, NewConfig("h").WithHeartbeat(0, 0).WithMetadataRefresh(0).WithPreparedCacheSize(0).WithServerTimestamps().
			WithConfigReport(false).WithDiscovery(false).WithAutomaticSchemaAgreement(false).WithRefreshAfterAgreement(false).NoAdvancedShardAwareness().WithNagle(true)).settings
		if c.KeepaliveInterval != 0 || c.Metadata.PeriodicRefresh != 0 || c.PreparedCacheSize != 0 || c.TimestampGenerator != nil ||
			c.ReportConfiguration || c.Metadata.InitialDiscovery || c.Metadata.AutomaticAgreement || c.Metadata.RefreshAfterAgreement || c.Pool.ShardAwarePort || c.NoDelay {
			t.Fatalf("a disabling setting left its feature on: %+v", c)
		}
		if c := mustBuildConfig(t, NewConfig("h").WithNagle(true).WithNagle(false)).settings; !c.NoDelay {
			t.Fatal("WithNagle(false) did not set TCP_NODELAY")
		}
	})

	t.Run("StringHidesCredentials", func(t *testing.T) {
		s := mustBuildConfig(t, NewConfig("h").WithAuth(auth.Password("user", "secret-password"))).String()
		if strings.Contains(s, "secret-password") || !strings.Contains(s, "auth=true") {
			t.Fatalf("String() = %q", s)
		}
		if (Config{}).String() != "gocql.Config{}" {
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
