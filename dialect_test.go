// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/config"
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
		s, err := dialectSettings[ScyllaV4](cfg)
		check(t, s, err, 4, implSettings.ServerScylla)
	})
	t.Run("CassandraV4", func(t *testing.T) {
		s, err := dialectSettings[CassandraV4](cfg)
		check(t, s, err, 4, implSettings.ServerCassandra)
	})
	t.Run("CassandraV5", func(t *testing.T) {
		s, err := dialectSettings[CassandraV5](cfg)
		check(t, s, err, 5, implSettings.ServerCassandra)
	})
	// Snappy compresses frames, which protocol 5 does not have: only the
	// dialect reveals the conflict, so Connect reports it.
	t.Run("CompressionConflict", func(t *testing.T) {
		snappy := mustBuildConfig(t, config.New("127.0.0.1").WithCompression(config.SnappyCompression))
		s, err := dialectSettings[CassandraV5](snappy)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := errors.AsType[*cqlerr.UnsupportedSettingError](s.Validate()); !ok {
			t.Fatalf("snappy on protocol 5: %v", s.Validate())
		}
	})
}

func mustBuildConfig(t *testing.T, b *config.ConfigBuilder) config.Config {
	t.Helper()
	c, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return c
}
