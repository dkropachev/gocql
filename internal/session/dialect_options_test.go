// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// An execution option one dialect lacks fails compiling for a session of
// that dialect, and executing options compiled without a session, with an
// UnsupportedSettingError naming the option and the dialect, before anything
// is sent. Another dialect accepts it.
func TestDialectOnlyOptions(t *testing.T) {
	host := cql.Some(cql.UUID{15: 1})
	for name, tc := range map[string]struct {
		values   Options
		setting  string
		want     string
		rejected []settings.Config
		accepted []settings.Config
	}{
		"ShardID": {
			values: Options{HostID: host, ShardID: cql.Some[uint32](1)}, setting: "shard ID", want: "ScyllaV4",
			rejected: []settings.Config{{Server: settings.ServerCassandra, Protocol: 4}, {Server: settings.ServerCassandra, Protocol: 5}},
			accepted: []settings.Config{{Server: settings.ServerScylla, Protocol: 4}, {Server: settings.ServerAny, Protocol: 4}},
		},
		"NowInSeconds": {
			values: Options{NowInSeconds: cql.Some[int32](10)}, setting: "now in seconds", want: "CassandraV5",
			rejected: []settings.Config{{Server: settings.ServerScylla, Protocol: 4}, {Server: settings.ServerCassandra, Protocol: 4}},
			accepted: []settings.Config{{Server: settings.ServerCassandra, Protocol: 5}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			check := func(t *testing.T, config settings.Config, err error) {
				t.Helper()
				unsupported, ok := errors.AsType[*cqlerr.UnsupportedSettingError](err)
				dialect := settings.DialectName(config.Server, config.Protocol)
				if !ok || unsupported.Setting != tc.setting || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), dialect) {
					t.Fatalf("%s: %v, want an UnsupportedSettingError for %q naming %s and %s", dialect, err, tc.setting, tc.want, dialect)
				}
			}
			unbound, err := NewCompiledOptions(nil, tc.values)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range tc.rejected {
				s := testSession(t)
				InternalRuntimeFor(s).config = config
				_, err := NewCompiledOptions(s, tc.values)
				check(t, config, err)
				bound := runtimeQuery(t)
				_, cancel, _, err := InternalAdmitExecution(context.Background(), s, bound, unbound)
				if cancel != nil {
					cancel()
				}
				check(t, config, err)
				bound.Close()
			}
			for _, config := range tc.accepted {
				s := testSession(t)
				InternalRuntimeFor(s).config = config
				if _, err := NewCompiledOptions(s, tc.values); err != nil {
					t.Fatalf("%v over protocol %d: %v", config.Server, config.Protocol, err)
				}
			}
		})
	}
}
