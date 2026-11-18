// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// Each dialect fixes the protocol version and the server the session expects.
func TestDialect(t *testing.T) {
	cfg := mustBuildConfig(t, config.New("127.0.0.1"))
	check := func(t *testing.T, settings implSettings.Config, err error, protocol int, server implSettings.Server) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if settings.Protocol != protocol || settings.Server != server {
			t.Fatalf("protocol %d server %v, want %d %v", settings.Protocol, settings.Server, protocol, server)
		}
		if err := settings.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("ScyllaV4", func(t *testing.T) {
		s, err := dialectSettings(cfg, ScyllaV4)
		check(t, s, err, 4, implSettings.ServerScylla)
	})
	t.Run("CassandraV4", func(t *testing.T) {
		s, err := dialectSettings(cfg, CassandraV4)
		check(t, s, err, 4, implSettings.ServerCassandra)
	})
	t.Run("CassandraV5", func(t *testing.T) {
		s, err := dialectSettings(cfg, CassandraV5)
		check(t, s, err, 5, implSettings.ServerCassandra)
	})
	t.Run("Zero", func(t *testing.T) {
		_, err := dialectSettings(cfg, 0)
		if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
			t.Fatalf("zero dialect: %v", err)
		}
	})
	// Snappy compresses frames, which protocol 5 does not have: only the
	// dialect reveals the conflict, so Connect reports it.
	t.Run("CompressionConflict", func(t *testing.T) {
		snappy := mustBuildConfig(t, config.New("127.0.0.1").WithCompression(cql.SnappyCompression))
		s, err := dialectSettings(snappy, CassandraV5)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := errors.AsType[*cqlerr.UnsupportedSettingError](s.Validate()); !ok {
			t.Fatalf("snappy on protocol 5: %v", s.Validate())
		}
	})
}

// String names every dialect, and dialectOf inverts spec.
func TestDialectString(t *testing.T) {
	for d, want := range map[Dialect]string{0: "Dialect(0)", ScyllaV4: "ScyllaV4", CassandraV4: "CassandraV4", CassandraV5: "CassandraV5", 9: "Dialect(9)"} {
		if got := d.String(); got != want {
			t.Errorf("%d: %q, want %q", uint8(d), got, want)
		}
		if spec, ok := d.spec(); ok {
			if got := dialectOf(spec.protocol, spec.server); got != d {
				t.Errorf("dialectOf(%v) = %v", d, got)
			}
		}
	}
}

func mustBuildConfig(t *testing.T, b *config.ConfigBuilder) config.Config {
	t.Helper()
	c, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return c
}
