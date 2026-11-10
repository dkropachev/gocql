/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package execution

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestConnStartup(t *testing.T) {
	t.Run("Compression", func(t *testing.T) {
		// TestConnStartup/Compression/DriverOwned pins that COMPRESSION in the STARTUP options is
		// always the driver's negotiated choice, never something an ApplicationInfo callback
		// left behind.
		//
		// The callback runs first and is handed the raw map, so it can set any key. The driver
		// overwrites the keys it always writes, but COMPRESSION is written only when the
		// server's SUPPORTED list names the configured compressor. On every other path a
		// callback-supplied value would otherwise be sent as though the driver had chosen it:
		// the server would compress with an algorithm the framers are not using, and every
		// frame after the handshake would be unreadable by one side.
		//
		// Both paths that skip the write are covered -- a compressor configured but refused,
		// and no compressor configured at all.
		t.Run("DriverOwned", func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				compressor implProtocol.Compressor
			}{
				{"configured compressor the server refuses", wirecompress.SnappyCompressor{}},
				{"no compressor configured", nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					var mu sync.Mutex
					var startups []map[string]string

					srv := newTestServerOpts{
						addr:     "127.0.0.1:0",
						protocol: implProtocol.ProtoVersion4,
						supportedFactory: func(net.Conn) map[string][]string {
							// The server does offer lz4, so the callback's value names a real
							// algorithm. Only the driver's own compressor is unsupported.
							return map[string][]string{"COMPRESSION": {"lz4"}}
						},
						recvHook: func(f *implProtocol.Framer) {
							if f.Header.Op != frm.OpStartup {
								return
							}
							mu.Lock()
							defer mu.Unlock()
							startups = append(startups, parseStartupOptions(t, append([]byte(nil), f.Buf...)))
						},
					}.newServer(t, ctx)
					defer srv.Stop()

					cluster := testCluster(implProtocol.ProtoVersion4, srv.Address)
					cluster.Compressor = tc.compressor
					cluster.ApplicationInfo = compressionInjectingAppInfo{algorithm: "lz4"}

					db, err := cluster.Open()
					require.NoError(t, err)
					defer db.Close()

					require.NoError(t, execTestStatement(context.Background(), db, "void", CompiledOptions{}))

					conn := db.GetConn()
					require.NotNil(t, conn)
					require.Nil(t, conn.InternalCompressor, "no compressor was negotiated, so none may remain")
					require.Zero(t, conn.InternalFramers.InternalDefaults.InternalFlags&frm.FlagCompress)

					mu.Lock()
					defer mu.Unlock()
					require.NotEmpty(t, startups, "no STARTUP frame was observed")
					for _, opts := range startups {
						require.NotContains(t, opts, "COMPRESSION",
							"STARTUP advertised a compression the driver is not using")
						// The callback still reached the server, so the test is not passing
						// because ApplicationInfo was ignored wholesale.
						require.Contains(t, opts, "DRIVER_NAME")
					}
				})
			}
		})

		// TestConnStartup/Compression/NotSupported covers the negotiation fallback: the server's
		// SUPPORTED response does not name the configured compressor, so the driver drops it and
		// continues uncompressed rather than sending a STARTUP the server would reject.
		//
		// The fallback itself is deliberate and stays. What is asserted here is that it is no
		// longer silent: a user who asked for snappy and got an uncompressed connection had no
		// way to find out short of reading driver internals, and on a mixed cluster this can
		// affect one node out of several. TestFraming/Compression/Negotiated in tests/integration is the check
		// that it does not happen on a healthy cluster.
		//
		// The server advertises lz4 rather than nothing, which is the sharper case: it separates
		// "this node offers no compression" from "this node offers a different algorithm", and
		// the warning has to name both sides to be worth logging.
		t.Run("NotSupported", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv := NewTestServerWithAddressAndSupportedFactory("127.0.0.1:0", t, implProtocol.ProtoVersion4, ctx,
				func(net.Conn) map[string][]string {
					return map[string][]string{"COMPRESSION": {"lz4"}}
				})
			defer srv.Stop()

			rec := &recordingLogger{}
			cluster := testCluster(implProtocol.ProtoVersion4, srv.Address)
			cluster.Compressor = wirecompress.SnappyCompressor{}
			cluster.Logger = rec

			db, err := cluster.Open()
			require.NoError(t, err)
			defer db.Close()

			conn := db.GetConn()
			require.NotNil(t, conn, "no connection available")

			require.Nil(t, conn.InternalCompressor, "the refused compressor must be dropped")
			// The flag is the part that actually matters: a framer still carrying FlagCompress
			// against a server expecting plaintext would fail every request.
			require.Zero(t, conn.InternalFramers.InternalDefaults.InternalFlags&frm.FlagCompress,
				"cached framer flags must not claim compression")

			logged := rec.String()
			require.Contains(t, logged, "snappy", "the warning must name the compressor that was refused")
			require.Contains(t, logged, "lz4", "the warning must name what the server offered instead")

			// The connection is still expected to work -- the point of the fallback.
			require.NoError(t, execTestStatement(context.Background(), db, "void", CompiledOptions{}))
		})
	})

	t.Run("InitFramerCache", func(t *testing.T) {
		// TestConnStartup/InitFramerCache/ProtoV3IgnoresScyllaUseMetadataID pins the negotiation gate from
		// the connection's end: a v3 connection to a server advertising SCYLLA_USE_METADATA_ID
		// must come out of the handshake with the extension nowhere on it. It goes through
		// parseCQLProtocolExtensions rather than setting cqlProtoExts directly, because that
		// list is also what startupCoordinator.startup serializes into STARTUP — an extension
		// filtered out there is one the driver neither announces nor encodes.
		t.Run("ProtoV3IgnoresScyllaUseMetadataID", func(t *testing.T) {
			t.Parallel()

			c := &transport.Conn{InternalVersion: implProtocol.ProtoVersion3, InternalLogger: &logging.InternalTestLogger{}}
			c.InternalCqlProtoExts = endpoint.InternalParseCQLProtocolExtensions(map[string][]string{endpoint.InternalScyllaUseMetadataID: {}}, c.InternalVersion, c.InternalLogger)
			c.InternalInitFramerCache()

			if endpoint.InternalFindCQLProtoExtByName(c.InternalCqlProtoExts, endpoint.InternalScyllaUseMetadataID) != nil {
				t.Error("SCYLLA_USE_METADATA_ID should not be negotiated on a protocol v3 connection")
			}
			if c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID {
				t.Error("framerConfig.scyllaUseMetadataID should be false on a protocol v3 connection")
			}
			if c.InternalUsesMetadataID() {
				t.Error("driverConn.usesMetadataID() should be false on a protocol v3 connection")
			}
			if c.InternalTracksResultMetadataID() {
				t.Error("driverConn.tracksResultMetadataID() should be false on a protocol v3 connection")
			}
			if c.InternalGetWriteFramer().ScyllaUseMetadataID {
				t.Error("write framer should not have scyllaUseMetadataID set on a protocol v3 connection")
			}
		})

		// TestConnStartup/InitFramerCache/ScyllaUseMetadataID guards against scyllaUseMetadataID
		// being negotiated on the driverConn but never reaching the pooled framers that
		// actually read/write frames (see frame.go's parseResultMetadata,
		// parseResultPrepared, fixtureExecuteFrame).
		//
		// It also pins driverConn.usesMetadataID against the same config, since the request path
		// reads the flag through it: were the two to diverge, the driver would ask the
		// server to skip metadata while writing no result metadata ID to compare against.
		t.Run("ScyllaUseMetadataID", func(t *testing.T) {
			c := &transport.Conn{
				InternalVersion:      implProtocol.ProtoVersion4,
				InternalCqlProtoExts: []endpoint.InternalCqlProtocolExtension{&endpoint.InternalScyllaUseMetadataIDExt{}},
			}
			c.InternalInitFramerCache()

			if !c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID {
				t.Fatal("framerConfig.scyllaUseMetadataID should be true once SCYLLA_USE_METADATA_ID is negotiated")
			}

			if !c.InternalUsesMetadataID() {
				t.Error("driverConn.usesMetadataID() should agree with the framer config")
			}

			wf := c.InternalGetWriteFramer()
			if !wf.ScyllaUseMetadataID {
				t.Error("write framer obtained from pool should have scyllaUseMetadataID set")
			}

			rf := c.InternalGetReadFramer()
			if !rf.ScyllaUseMetadataID {
				t.Error("read framer obtained from pool should have scyllaUseMetadataID set")
			}
		})

		// TestConnStartup/InitFramerCache/WithoutScyllaUseMetadataID is the negative counterpart: with
		// no extension negotiated, nothing on the connection may claim otherwise.
		t.Run("WithoutScyllaUseMetadataID", func(t *testing.T) {
			c := &transport.Conn{InternalVersion: implProtocol.ProtoVersion4}
			c.InternalInitFramerCache()

			if c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID {
				t.Error("framerConfig.scyllaUseMetadataID should be false when the extension was not negotiated")
			}
			if c.InternalUsesMetadataID() {
				t.Error("driverConn.usesMetadataID() should be false when the extension was not negotiated")
			}
			if c.InternalGetWriteFramer().ScyllaUseMetadataID {
				t.Error("write framer should not have scyllaUseMetadataID set")
			}
		})
	})

	t.Run("CompiledOptions", func(t *testing.T) {
		// TestConnStartup/CompiledOptions/KeepDriverKeys pins that ApplicationInfo cannot overwrite the
		// STARTUP options the driver owns.
		//
		// CQL_VERSION, DRIVER_NAME and DRIVER_VERSION describe the driver and the
		// protocol it speaks. A callback that set CQL_VERSION could make every connection
		// in the cluster fail its handshake; one that set DRIVER_NAME or DRIVER_VERSION
		// would misreport the driver to the server for the life of the connection.
		// SESSION_ID is driver-owned for the same reason: it is what correlates a
		// session's connections in system.clients.
		t.Run("KeepDriverKeys", func(t *testing.T) {
			t.Parallel()

			const (
				cqlVersion    = "3.4.5"
				driverName    = "gocql"
				driverVersion = "1.2.3"
				sessionID     = "91b0b1a2-0000-4000-8000-000000000001"
			)

			t.Run("no ApplicationInfo", func(t *testing.T) {
				m := transport.InternalStartupOptions(cqlVersion, driverName, driverVersion, nil, nil, sessionID, false)

				require.Equal(t, map[string]string{
					"CQL_VERSION":                         cqlVersion,
					"DRIVER_NAME":                         driverName,
					"DRIVER_VERSION":                      driverVersion,
					transport.InternalSessionIDStartupKey: sessionID,
				}, m)
			})

			t.Run("application options are kept", func(t *testing.T) {
				m := transport.InternalStartupOptions(cqlVersion, driverName, driverVersion,
					endpoint.NewStaticApplicationInfo("app", "9.9.9", "client-id"), nil, sessionID, false)

				require.Equal(t, "app", m["APPLICATION_NAME"])
				require.Equal(t, "9.9.9", m["APPLICATION_VERSION"])
				require.Equal(t, "client-id", m["CLIENT_ID"])
				require.Equal(t, cqlVersion, m["CQL_VERSION"])
				require.Equal(t, driverName, m["DRIVER_NAME"])
				require.Equal(t, driverVersion, m["DRIVER_VERSION"])
				require.Equal(t, sessionID, m[transport.InternalSessionIDStartupKey])
			})

			t.Run("driver-owned keys win", func(t *testing.T) {
				m := transport.InternalStartupOptions(cqlVersion, driverName, driverVersion,
					applicationInfoFunc(func(opts map[string]string) {
						opts["CQL_VERSION"] = "9.9.9"
						opts["DRIVER_NAME"] = "not-gocql"
						opts["DRIVER_VERSION"] = "0.0.0"
						opts[transport.InternalSessionIDStartupKey] = "not-the-session-id"
						opts["APPLICATION_NAME"] = "app"
					}), nil, sessionID, false)

				require.Equal(t, cqlVersion, m["CQL_VERSION"], "a custom CQL version would fail every handshake")
				require.Equal(t, driverName, m["DRIVER_NAME"])
				require.Equal(t, driverVersion, m["DRIVER_VERSION"])
				require.Equal(t, sessionID, m[transport.InternalSessionIDStartupKey], "a custom session id would break correlating a session's connections")
				require.Equal(t, "app", m["APPLICATION_NAME"], "keys the driver does not own must still come through")
			})
		})
	})

	t.Run("SystemRequest", func(t *testing.T) {
		// TestConnStartup/SystemRequest/ClauseFollowsTimeout pins the USING TIMEOUT clause as
		// derived purely from the timeout in effect and whether the peer is ScyllaDB.
		//
		// The zero case is the one worth guarding: MetadataSchemaRequestTimeout is
		// documented as "positive or zero" (see clusterSettings.Validate), and by the time
		// finalizeConnection applies it, startupCoordinator.options has already rendered a
		// clause from ConnectTimeout. Were that clause carried over, a caller who disabled
		// the metadata timeout would still have every system query bounded server-side by
		// an unrelated setting - while DRIVER_CONFIG reported no timeout at all, since
		// buildControlPlaneReport omits a non-positive one.
		t.Run("ClauseFollowsTimeout", func(t *testing.T) {
			t.Parallel()

			newConn := func(isScylla bool) *transport.Conn {
				c := &transport.Conn{InternalScyllaSupported: endpoint.ScyllaConnectionFeatures{
					ScyllaHostFeatures: *func() *endpoint.ScyllaHostFeatures {
						opaqueStorageValuexx := new(endpoint.ScyllaHostFeatures)
						implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalIsScylla = isScylla
						return opaqueStorageValuexx

						// As startupCoordinator.options leaves it: a clause rendered from
						// ConnectTimeout, before finalizeConnection applies the metadata timeout.
					}(),
				}}

				c.InternalSetSystemRequestTimeout(600 * time.Millisecond)
				return c
			}

			t.Run("scylla renders the clause", func(t *testing.T) {
				require.Equal(t, transport.InternalSystemRequestState{
					InternalTimeout:     600 * time.Millisecond,
					InternalUsingClause: " USING TIMEOUT 600ms",
				}, newConn(true).InternalGetSystemRequestState())
			})

			t.Run("cassandra renders no clause", func(t *testing.T) {
				require.Equal(t, transport.InternalSystemRequestState{InternalTimeout: 600 * time.Millisecond},
					newConn(false).InternalGetSystemRequestState())
			})

			t.Run("a disabled timeout clears the clause", func(t *testing.T) {
				c := newConn(true)
				c.InternalSetSystemRequestTimeout(0)
				require.Equal(t, transport.InternalSystemRequestState{}, c.InternalGetSystemRequestState(),
					"a zero metadata timeout must not leave the ConnectTimeout clause in force")
			})

			// The cases above assert the snapshot itself; this one goes through
			// systemRequestStatement, which is how the query paths reach it.
			t.Run("the statement carries the clause in force", func(t *testing.T) {
				c := newConn(true)
				stmt, timeout := c.InternalSystemRequestStatement(transport.InternalQrySystemLocal)
				require.Equal(t, transport.InternalQrySystemLocal+" USING TIMEOUT 600ms", stmt)
				require.Equal(t, 600*time.Millisecond, timeout)

				c.InternalSetSystemRequestTimeout(0)
				stmt, timeout = c.InternalSystemRequestStatement(transport.InternalQrySystemLocal)
				require.Equal(t, transport.InternalQrySystemLocal, stmt, "a disabled timeout appends nothing")
				require.Equal(t, time.Duration(0), timeout)
			})
		})

		// TestConnStartup/SystemRequest/TimeoutRaceWithFinalize pins the state
		// driver-issued system queries read (driverConn.systemRequest, holding the client-side
		// timeout and the pre-rendered USING TIMEOUT clause) as safe to read while
		// finalizeConnection switches it from ConnectTimeout to
		// MetadataSchemaRequestTimeout.
		//
		// The two are concurrent in practice: the control connection is registered for
		// server push events during controlConn.setupConn, but its
		// finalizeConnection() is deferred to the very end of Session.init. A keyspace
		// SCHEMA_CHANGE delivered inside that window (a freshly bootstrapped cluster
		// still creating its internal keyspaces is enough) makes the event-debouncer
		// goroutine run handleKeyspaceChange -> awaitSchemaAgreement -> querySystem on
		// the connection the main goroutine is mid-write on.
		//
		// Run under -race: were the two values plain fields again, this would report
		// "DATA RACE" on them — a torn string header for the clause and a stale timeout.
		t.Run("TimeoutRaceWithFinalize", func(t *testing.T) {
			t.Parallel()

			// finalizeConnection only stores into atomics - driverConn.writeTimeout,
			// driverConn.systemRequest, and the timeout field of each of these two - so neither
			// needs a net.Conn behind it.
			c := &transport.Conn{
				InternalR: &transport.InternalConnReader{},
				InternalW: &transport.InternalDeadlineContextWriter{},
				InternalCfg: &transport.ConnConfig{
					ConnectTimeout: 10 * time.Second,
					WriteTimeout:   2 * time.Second,
					ReadTimeout:    3 * time.Second,
				},
				InternalMetadataTimeout: 42 * time.Millisecond,
				// Only a ScyllaDB connection renders the USING TIMEOUT clause.
				InternalScyllaSupported: endpoint.ScyllaConnectionFeatures{
					ScyllaHostFeatures: *func() *endpoint.ScyllaHostFeatures {
						opaqueStorageValuex := new(endpoint.ScyllaHostFeatures)
						implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalIsScylla = true
						return opaqueStorageValuex
					}(),
				},
			}
			c.InternalSetSystemRequestTimeout(c.InternalCfg.ConnectTimeout)

			// The only two snapshots a reader may observe: the one the connection is
			// dialed with, and the one finalizeConnection replaces it with. Spelling both
			// out keeps this test from re-deriving the rendering rule, which is
			// TestConnStartup/SystemRequest/ClauseFollowsTimeout's job.
			var (
				beforeFinalize = transport.InternalSystemRequestState{
					InternalTimeout:     c.InternalCfg.ConnectTimeout,
					InternalUsingClause: " USING TIMEOUT 10000ms",
				}
				afterFinalize = transport.InternalSystemRequestState{
					InternalTimeout:     c.InternalMetadataTimeout,
					InternalUsingClause: " USING TIMEOUT 42ms",
				}
			)

			const readers = 4
			done := make(chan struct{})
			var wg, spinning sync.WaitGroup
			wg.Add(readers)
			spinning.Add(readers)
			for i := 0; i < readers; i++ {
				go func() {
					defer wg.Done()
					first := true
					for {
						select {
						case <-done:
							return
						default:
						}
						// The single load querySystem performs.
						state := c.InternalGetSystemRequestState()
						if first {
							// Only write once every reader is already spinning, so the
							// two sides genuinely overlap.
							spinning.Done()
							first = false
						}
						// Both values must come from the same snapshot: the clause the
						// server is asked to honour always matches the deadline the
						// client is waiting on, never the other transition's value.
						if state != beforeFinalize && state != afterFinalize {
							t.Errorf("observed %+v, which is neither transition's snapshot; want %+v or %+v",
								state, beforeFinalize, afterFinalize)
							return
						}
					}
				}()
			}

			spinning.Wait()
			c.InternalFinalizeConnection()
			close(done)
			wg.Wait()

			require.Equal(t, afterFinalize, c.InternalGetSystemRequestState(),
				"finalizeConnection should publish the metadata timeout and its clause as one snapshot")
		})
	})

	t.Run("TracksResultMetadataID", func(t *testing.T) {
		// TestConnStartup/TracksResultMetadataID/MasksDirectionBit guards the derivation against the
		// request/response direction bit: tracksResultMetadataID reads the framer config,
		// which initCache masks with protoVersionMask. Folding the bit into the version
		// instead would make a v4 connection look like protocol 0x84 and silently start
		// skipping metadata with no ID to recover it.
		t.Run("MasksDirectionBit", func(t *testing.T) {
			t.Parallel()

			c := &transport.Conn{InternalVersion: implProtocol.ProtoVersion4 | 0x80}
			c.InternalInitFramerCache()

			if c.InternalTracksResultMetadataID() {
				t.Error("driverConn.tracksResultMetadataID() = true for v4 with the direction bit set, want false")
			}
		})

		// TestConnStartup/TracksResultMetadataID/Negotiation pins the question the EXECUTE path actually asks:
		// does this connection exchange result metadata IDs at all? Two mechanisms answer
		// yes — native protocol v5, where the field is mandatory, and SCYLLA_USE_METADATA_ID,
		// which backports it to v4 — and the skip-metadata default follows either, per
		// scylladb/scylla-drivers#81.
		//
		// usesMetadataID must stay narrower than that: the integration tests use it to
		// assert the extension was negotiated specifically, so a v5 connection reporting
		// true there would turn a negotiation regression green.
		t.Run("Negotiation", func(t *testing.T) {
			t.Parallel()

			for _, tt := range []struct {
				name        string
				proto       byte
				exts        []endpoint.InternalCqlProtocolExtension
				wantTracked bool
				wantExt     bool
			}{
				{name: "v4 without the extension", proto: implProtocol.ProtoVersion4},
				{
					name:        "v4 with the extension",
					proto:       implProtocol.ProtoVersion4,
					exts:        []endpoint.InternalCqlProtocolExtension{&endpoint.InternalScyllaUseMetadataIDExt{}},
					wantTracked: true,
					wantExt:     true,
				},
				{
					// v5 makes the field mandatory, so no extension is involved.
					name:        "v5 without the extension",
					proto:       implProtocol.ProtoVersion5,
					wantTracked: true,
				},
				{
					// Not reachable through negotiation — newScyllaUseMetadataIDExt opts in on
					// v4 only — but initCache must not read the two mechanisms as exclusive.
					name:        "v5 with the extension",
					proto:       implProtocol.ProtoVersion5,
					exts:        []endpoint.InternalCqlProtocolExtension{&endpoint.InternalScyllaUseMetadataIDExt{}},
					wantTracked: true,
					wantExt:     true,
				},
			} {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					c := &transport.Conn{InternalVersion: tt.proto, InternalCqlProtoExts: tt.exts}
					c.InternalInitFramerCache()

					if got := c.InternalTracksResultMetadataID(); got != tt.wantTracked {
						t.Errorf("driverConn.tracksResultMetadataID() = %v, want %v", got, tt.wantTracked)
					}
					if got := c.InternalUsesMetadataID(); got != tt.wantExt {
						t.Errorf("driverConn.usesMetadataID() = %v, want %v", got, tt.wantExt)
					}
				})
			}
		})
	})

	t.Run("UseKeyspaceStmt", func(t *testing.T) {
		tests := []struct {
			keyspace string
			want     string
		}{
			{"simple", `USE "simple"`},
			{`my"ks`, `USE "my""ks"`},
			{`a""b`, `USE "a""""b"`},
			{`"`, `USE """"`},
			{"", `USE ""`},
		}
		for _, tt := range tests {
			got := transport.InternalUseKeyspaceStmt(tt.keyspace)
			if got != tt.want {
				t.Errorf("keyspace %q: got %q, want %q", tt.keyspace, got, tt.want)
			}
		}
	})
}
