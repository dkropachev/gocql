// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// Each dialect fixes the protocol version and the server the session expects.
func TestDialect(t *testing.T) {
	for d, want := range map[Dialect]struct {
		protocol int
		server   implSettings.Server
	}{
		ScyllaV4:    {4, implSettings.ServerScylla},
		CassandraV4: {4, implSettings.ServerCassandra},
		CassandraV5: {5, implSettings.ServerCassandra},
	} {
		t.Run(d.String(), func(t *testing.T) {
			s := mustBuildConfig(t, New(d, "127.0.0.1")).settings
			if s.Protocol != want.protocol || s.Server != want.server {
				t.Fatalf("protocol %d server %v, want %d %v", s.Protocol, s.Server, want.protocol, want.server)
			}
		})
	}
	for _, d := range []Dialect{0, 9} {
		if _, err := New(d, "127.0.0.1").Build(); err == nil {
			t.Errorf("%v: Build accepted it", d)
		} else if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
			t.Errorf("%v: Build = %v, want a cql.ValidationError", d, err)
		}
	}
	// Snappy compresses frames, which protocol 5 does not have: the dialect
	// reveals the conflict, so Build reports it.
	_, err := New(CassandraV5, "127.0.0.1").WithCompression(cql.SnappyCompression).Build()
	if _, ok := errors.AsType[*cqlerr.UnsupportedSettingError](err); !ok {
		t.Fatalf("snappy on protocol 5: %v", err)
	}
}

// String names every dialect, and DialectOf inverts DialectSpec.
func TestDialectString(t *testing.T) {
	for d, want := range map[Dialect]string{0: "Dialect(0)", ScyllaV4: "ScyllaV4", CassandraV4: "CassandraV4", CassandraV5: "CassandraV5", 9: "Dialect(9)"} {
		if got := d.String(); got != want {
			t.Errorf("%d: %q, want %q", uint8(d), got, want)
		}
		if server, protocol, ok := implSettings.DialectSpec(uint8(d)); ok {
			if got := Dialect(implSettings.DialectOf(server, protocol)); got != d {
				t.Errorf("DialectOf(%v) = %v", d, got)
			}
		}
	}
	if s := mustBuildConfig(t, New(CassandraV5, "127.0.0.1")).String(); !strings.Contains(s, "dialect=CassandraV5") {
		t.Errorf("Config.String() = %q, want the dialect", s)
	}
}

// A setting only ScyllaDB supports fails Build with a Cassandra dialect with
// an UnsupportedSettingError naming the setting and the dialect, and builds
// with ScyllaV4.
func TestScyllaOnlySettings(t *testing.T) {
	route := ClientRoute{ConnectionID: "c1", Hostname: "a.example"}
	for name, tc := range map[string]struct {
		setting string
		choose  func(*ConfigBuilder) *ConfigBuilder
	}{
		"ClientRoutes": {"WithClientRoutes", func(b *ConfigBuilder) *ConfigBuilder { return b.WithClientRoutes(route) }},
		"MetadataServerTimeout": {"WithMetadataServerTimeout", func(b *ConfigBuilder) *ConfigBuilder {
			return b.WithMetadataServerTimeout(time.Second)
		}},
		"ShardAwareSourcePorts": {"WithShardAwareSourcePorts", func(b *ConfigBuilder) *ConfigBuilder {
			return b.WithShardAwareSourcePorts(50000, 60000)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			for _, d := range []Dialect{CassandraV4, CassandraV5} {
				_, err := tc.choose(New(d, "127.0.0.1")).Build()
				unsupported, ok := errors.AsType[*cqlerr.UnsupportedSettingError](err)
				if !ok || unsupported.Setting != tc.setting || !strings.Contains(err.Error(), "ScyllaV4") || !strings.Contains(err.Error(), d.String()) {
					t.Fatalf("%v: Build = %v, want an UnsupportedSettingError for %q naming ScyllaV4 and %v", d, err, tc.setting, d)
				}
				if cqlerr.KindOf(err) != cqlerr.KindInvalid {
					t.Fatalf("%v: kind %v", d, cqlerr.KindOf(err))
				}
			}
			mustBuildConfig(t, tc.choose(New(ScyllaV4, "127.0.0.1")))
		})
	}
	// A zero server timeout sends none, so Cassandra accepts it.
	mustBuildConfig(t, New(CassandraV4, "127.0.0.1").WithMetadataServerTimeout(0))
}
