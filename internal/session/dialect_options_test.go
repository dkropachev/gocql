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
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/settings"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// An execution option one dialect lacks fails compiling for a session of
// that dialect, and executing there options compiled without a session or for
// a session of another dialect, with an UnsupportedSettingError naming the
// option and the dialect, before anything is sent. A session of the dialect
// that has it compiles and admits it. Prepared.Routing checks options as an
// execution does.
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
			rejected: []settings.Config{{Server: settings.ServerScylla, Protocol: 4}, {Server: settings.ServerCassandra, Protocol: 4}, {Server: settings.ServerAny, Protocol: 4}},
			accepted: []settings.Config{{Server: settings.ServerCassandra, Protocol: 5}},
		},
		"Keyspace": {
			values: Options{Keyspace: "other"}, setting: "Keyspace", want: "CassandraV5",
			rejected: []settings.Config{{Server: settings.ServerScylla, Protocol: 4}, {Server: settings.ServerCassandra, Protocol: 4}, {Server: settings.ServerAny, Protocol: 4}},
			accepted: []settings.Config{{Server: settings.ServerCassandra, Protocol: 5}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			session := func(t *testing.T, server settings.Server, protocol int) *implCluster.Core {
				t.Helper()
				s := testSession(t)
				rt := InternalRuntimeFor(s)
				config := rt.config
				config.Server, config.Protocol = server, protocol
				rt.config = config
				return s
			}
			admit := func(t *testing.T, s *implCluster.Core, options CompiledOptions) error {
				t.Helper()
				bound := runtimeQuery(t)
				defer bound.Close()
				_, cancel, request, err := InternalAdmitExecution(context.Background(), s, bound, options)
				if cancel != nil {
					cancel()
				}
				if request != nil && request.InternalLogical != nil {
					request.InternalLogical.InternalFinish(cql.Result{}, context.Canceled)
					request.InternalLogical.releaseInput()
				}
				return err
			}
			// route checks options as Prepared.Routing does, for a
			// statement that names no keyspace.
			route := func(s *implCluster.Core, options CompiledOptions) error {
				rt := InternalRuntimeFor(s)
				var scratch InternalResolvedOptions
				return fillOptionRouting(new(routing2.Info), rt, options.resolve(rt, &scratch))
			}
			check := func(t *testing.T, config settings.Config, stage string, err error) {
				t.Helper()
				unsupported, ok := errors.AsType[*cqlerr.UnsupportedSettingError](err)
				dialect := settings.DialectName(config.Server, config.Protocol)
				if !ok || unsupported.Setting != tc.setting || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), dialect) {
					t.Fatalf("%s, %s: %v, want an UnsupportedSettingError for %q naming %s and %s", dialect, stage, err, tc.setting, tc.want, dialect)
				}
			}
			unbound, err := NewCompiledOptions(nil, tc.values)
			if err != nil {
				t.Fatal(err)
			}
			supporting := tc.accepted[0]
			foreign, err := NewCompiledOptions(session(t, supporting.Server, supporting.Protocol), tc.values)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range tc.rejected {
				s := session(t, config.Server, config.Protocol)
				_, err := NewCompiledOptions(s, tc.values)
				check(t, config, "compile", err)
				check(t, config, "execute unbound", admit(t, s, unbound))
				check(t, config, "execute compiled for "+settings.DialectName(supporting.Server, supporting.Protocol), admit(t, s, foreign))
				check(t, config, "route compiled for "+settings.DialectName(supporting.Server, supporting.Protocol), route(s, foreign))
			}
			for _, config := range tc.accepted {
				s := session(t, config.Server, config.Protocol)
				compiled, err := NewCompiledOptions(s, tc.values)
				if err != nil {
					t.Fatalf("%v over protocol %d: compile: %v", config.Server, config.Protocol, err)
				}
				for stage, options := range map[string]CompiledOptions{"compiled": compiled, "unbound": unbound} {
					if err := admit(t, s, options); err != nil {
						t.Fatalf("%v over protocol %d: execute %s: %v", config.Server, config.Protocol, stage, err)
					}
					if err := route(s, options); err != nil {
						t.Fatalf("%v over protocol %d: route %s: %v", config.Server, config.Protocol, stage, err)
					}
				}
			}
		})
	}
}

// TestSessionKeyspaceOption checks that the session's own keyspace is
// accepted as a per-request keyspace on protocol 4, where it changes nothing.
func TestSessionKeyspaceOption(t *testing.T) {
	s := testSession(t)
	rt := InternalRuntimeFor(s)
	config := rt.config
	config.Server, config.Protocol, config.Keyspace = settings.ServerScylla, 4, "app"
	rt.config = config
	if _, err := NewCompiledOptions(s, Options{Keyspace: "app"}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCompiledOptions(s, Options{Keyspace: "other"}); err == nil {
		t.Fatal("protocol 4 compiled a keyspace other than the session's")
	}
	statement := binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "SELECT * FROM t", Params: cql.NoParams}
	_, err := Prepare(context.Background(), s, statement, PrepareOptions{Keyspace: "other"})
	if unsupported, ok := errors.AsType[*cqlerr.UnsupportedSettingError](err); !ok || unsupported.Setting != "Keyspace" {
		t.Fatalf("Prepare in another keyspace on protocol 4: %v", err)
	}
}
