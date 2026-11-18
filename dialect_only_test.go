// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// A setting only ScyllaDB supports fails Connect with a Cassandra dialect
// with an UnsupportedSettingError naming the setting and the dialect, before
// anything is dialed, and passes validation with ScyllaV4.
func TestScyllaOnlySettings(t *testing.T) {
	routes, err := config.NewClientRoutes().WithRoute("c1", "a.example").Build()
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		setting string
		choose  func(*config.ConfigBuilder) *config.ConfigBuilder
	}{
		"ClientRoutes": {"client routes", func(b *config.ConfigBuilder) *config.ConfigBuilder { return b.WithClientRoutes(routes) }},
		"MetadataServerTimeout": {"metadata server timeout", func(b *config.ConfigBuilder) *config.ConfigBuilder {
			return b.WithMetadataServerTimeout(time.Second)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			var dials atomic.Int32
			dialer := endpoint.DialerFunc(func(context.Context, endpoint.DialTarget) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("dialed")
			})
			cfg := mustBuildConfig(t, tc.choose(config.New("127.0.0.1").WithDialer(dialer)))
			for _, d := range []Dialect{CassandraV4, CassandraV5} {
				s, err := Connect(t.Context(), cfg, d)
				if s != nil {
					_ = s.Close()
				}
				unsupported, ok := errors.AsType[*cqlerr.UnsupportedSettingError](err)
				if !ok || unsupported.Setting != tc.setting || !strings.Contains(err.Error(), "ScyllaV4") || !strings.Contains(err.Error(), d.String()) {
					t.Fatalf("%v: Connect = %v, want an UnsupportedSettingError for %q naming ScyllaV4 and %v", d, err, tc.setting, d)
				}
				if cqlerr.KindOf(err) != cqlerr.KindInvalid {
					t.Fatalf("%v: kind %v", d, cqlerr.KindOf(err))
				}
			}
			if n := dials.Load(); n != 0 {
				t.Fatalf("Connect dialed %d times before rejecting the setting", n)
			}
			settings, err := dialectSettings(cfg, ScyllaV4)
			if err != nil {
				t.Fatal(err)
			}
			if err := settings.Validate(); err != nil {
				t.Fatalf("ScyllaV4: %v", err)
			}
		})
	}
	// A zero server timeout sends none, so Cassandra accepts it.
	cfg := mustBuildConfig(t, config.New("127.0.0.1").WithMetadataServerTimeout(0))
	settings, err := dialectSettings(cfg, CassandraV4)
	if err != nil {
		t.Fatal(err)
	}
	if err := settings.Validate(); err != nil {
		t.Fatalf("zero metadata server timeout on CassandraV4: %v", err)
	}
}

// The settings name a dialect as Dialect.String does.
func TestDialectName(t *testing.T) {
	for _, d := range []Dialect{ScyllaV4, CassandraV4, CassandraV5} {
		spec, _ := d.spec()
		if got := implSettings.DialectName(spec.server, spec.protocol); got != d.String() {
			t.Errorf("DialectName(%v) = %q, want %q", d, got, d.String())
		}
	}
}
