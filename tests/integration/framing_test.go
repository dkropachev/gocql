//go:build integration

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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestFraming(t *testing.T) {
	t.Run("Compression", func(t *testing.T) {
		// Pins the compressor the suite was asked for to the one the
		// connections actually ended up with.
		//
		// startupCoordinator.startup silently clears driverConn.compressor when the server's SUPPORTED
		// response does not list Compressor.Name(), so a lane configured with -compressor=lz4
		// against a server that does not offer lz4 would run entirely uncompressed while
		// session.cfg.Compressor stayed non-nil. Every compression-dependent test guards on the
		// connection and would simply skip, leaving the lane green and empty. Fail here instead,
		// with the mismatch named.
		//
		// Every host is checked, not just the one session.InternalGetConn happens to pick: the compressor
		// is negotiated per connection from that node's own SUPPORTED response, so on a cluster
		// where one node does not advertise the compressor a single-connection check passes while
		// queries routed to that node run uncompressed. One connection per host is the right
		// granularity, since the SUPPORTED response is a property of the node.
		//
		// waitForHostConns is what makes "every host" true. Walking the hosts directly would skip
		// the pools still filling, so on a three-node cluster this would usually inspect one node
		// and pass -- leaving exactly the degraded node it exists to catch invisible.
		//
		// Deliberately not parallel. A parallel test resumes only once the sequential tests at its
		// level have started, which would land this diagnosis at the tail of a ~470s run, after
		// every test it explains has already skipped.
		t.Run("Negotiated", func(t *testing.T) {
			if *flagCompressTest == "" || *flagCompressTest == "no-compression" {
				t.Skip("no compressor requested")
			}

			session := createV2Session(t)
			defer session.Close()

			conns := waitForHostConns(t, session)
			t.Logf("checked the negotiated compressor on %d host(s)", len(conns))

			for addr, conn := range conns {
				switch {
				case conn.InternalCompressor == nil:
					t.Errorf("%s: requested -compressor=%s but the connection negotiated none; the server's SUPPORTED response did not offer it",
						addr, *flagCompressTest)
				case conn.InternalCompressor.Name() != *flagCompressTest:
					t.Errorf("%s: requested -compressor=%s but the connection negotiated %q",
						addr, *flagCompressTest, conn.InternalCompressor.Name())
				}
			}
		})
		// Runs a query that is not likely to be compressed efficiently
		// (uncompressed payload size > compressed payload size).
		// So, it should send a Compressed Frame where:
		//  1. Compressed length is set to the length of the uncompressed payload;
		//  2. Uncompressed length is set to zero;
		//  3. Payload is the uncompressed payload.
		t.Run("NotWorthIt", func(t *testing.T) {
			session := createV2Session(t)
			defer session.Close()
			if implExecution.InternalSessionData((*implExecution.Session)(session)).InternalCfg.ProtoVersion < 5 {
				t.Skip("compressed segments are only produced on protocol >= 5")
			}
			if getRandomConn(t, session).InternalCompressor == nil {
				t.Skip("no compressor negotiated on the connection; the compressed-segment path is unreachable")
			}
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,text_col text)", table))
			text := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()_+"
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,text_col) VALUES(?,?)", table), cql.IntValue(1), codecTestMust(cql.TextValue(text)))
			require.Equal(t, text, cassandraText(t, cassandraRead(t, session, fmt.Sprintf("SELECT text_col FROM %s WHERE id=?", table), cql.IntValue(1)), "text_col"))
		})
	})

	t.Run("NegativeStream", func(t *testing.T) {
		t.Parallel()

		session := createV2Session(t)
		defer session.Close()

		conn := getRandomConn(t, session)

		const stream = -50
		writer := frameWriterFunc(func(f *framer, streamID int) error {
			f.WriteHeader(0, frm.OpOptions, stream)
			return f.Finish()
		})

		frame, err := conn.InternalExec(context.Background(), writer, nil, time.Second)
		if err == nil {
			t.Fatalf("expected to get an error on stream %d", stream)
		} else if frame != nil {
			t.Fatalf("expected to get nil frame got %+v", frame)
		}
	})

	t.Run("ResultMetadataID", func(t *testing.T) {
		// Covers native v5 result metadata IDs
		// and the negotiated Scylla v4 extension with the same exact-connection flow.
		// The protocol gate selects one path per lane; advertised-but-unnegotiated
		// extension support is a failure, not a capability skip.
		t.Run("ChangedFlag", func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				table string
				// gate returns a reason to skip, or "" to run. It may also fail outright, for
				// a state that must not be allowed to pass as a skip.
				gate func(t *testing.T, session *gocql.Session, conn *driverConn) string
			}{
				{
					name:  "native protocol v5",
					table: "metadata_changed",
					gate: func(t *testing.T, session *gocql.Session, conn *driverConn) string {
						if implExecution.InternalSessionData((*implExecution.Session)(session)).InternalCfg.ProtoVersion < protoVersion5 {
							return "Metadata_changed mechanism is only available in proto > 4"
						}
						if *flagDistribution == "scylla" && flagCassVersion.Before(2025, 3, 0) {
							return "ScyllaDB before 2025.3 does not exchange result metadata ids"
						}
						return ""
					},
				},
				{
					name:  "protocol v4 with SCYLLA_USE_METADATA_ID",
					table: "scylla_metadata_changed",
					gate: func(t *testing.T, session *gocql.Session, conn *driverConn) string {
						// The extension backports the v5 result metadata id to v4 and is
						// negotiated there only, so a run pinned to another protocol version
						// says nothing about it either way.
						if conn.InternalVersion&protoVersionMask != protoVersion4 {
							return "SCYLLA_USE_METADATA_ID is negotiated on protocol v4 only"
						}
						// Skip only for a server that cannot do this at all. If the server
						// advertised SCYLLA_USE_METADATA_ID and the driver still failed to
						// negotiate it, that is a regression — and since this is the only
						// end-to-end coverage of the extension, skipping would turn that
						// regression green. Fail instead.
						if !conn.InternalScyllaSupported.IsMetadataIDSupported() {
							return "server does not advertise SCYLLA_USE_METADATA_ID"
						}
						if !conn.InternalUsesMetadataID() {
							t.Fatal("server advertises SCYLLA_USE_METADATA_ID but the driver did not negotiate it")
						}
						return ""
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					session := createV2Session(t, func(c *gocql.ConfigBuilder) {
						c.WithIgnoreEvents(gocql.SchemaEvent).WithManualSchemaAgreement().NoRefreshAfterAgreement()
					})
					defer session.Close()

					// We have to specify conn for all queries to ensure that
					// all queries are running on the same node
					conn := getRandomConn(t, session)
					if reason := tc.gate(t, session, conn); reason != "" {
						t.Skip(reason)
					}

					runMetadataChangedFlow(t, session, conn, testTableName(t, tc.table))
				})
			}
		})
		// Covers the rolling-upgrade case: a
		// prepared statement cached before SCYLLA_USE_METADATA_ID was negotiated has no
		// result metadata ID, and the prepared cache is keyed by host and survives
		// reconnects, so it can still be executed over an extension-enabled connection.
		//
		// Two things have to hold. The driver must not ask the server to skip metadata for
		// such a statement — it has no ID for the server to compare against, so a skipped
		// response would leave it decoding against whatever it had cached. And Scylla must
		// accept the empty ID as a mismatch rather than rejecting the frame, answering with
		// METADATA_CHANGED and a fresh ID so the statement heals itself.
		//
		// The same scenario is covered in the sibling drivers: java-driver's
		// PreparedStatementIT.should_handle_empty_metadata_id_when_executing_statement_when_supported
		// (scylladb/java-driver#758) and python-driver's
		// test_empty_sentinel_id_triggers_metadata_changed (scylladb/python-driver#770).
		t.Run("ScyllaEmpty", func(t *testing.T) {
			session := createV2Session(t, func(c *gocql.ConfigBuilder) {
				c.WithIgnoreEvents(gocql.SchemaEvent).WithManualSchemaAgreement().NoRefreshAfterAgreement()
			})
			defer session.Close()
			conn := getRandomConn(t, session)
			if conn.InternalVersion&protoVersionMask != protoVersion4 {
				t.Skip("SCYLLA_USE_METADATA_ID is negotiated on protocol v4 only")
			}
			if !conn.InternalScyllaSupported.IsMetadataIDSupported() {
				t.Skip("server does not advertise SCYLLA_USE_METADATA_ID")
			}
			require.True(t, conn.InternalUsesMetadataID(), "server advertises metadata IDs but driver did not negotiate them")
			table := testTableName(t)
			cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,val int)", table))
			cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,val) VALUES(?,?)", table), cql.IntValue(1), cql.IntValue(7))
			host := cql.Option[cql.UUID]{Value: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), Set: true}
			prepared := cassandraPrepared(t, session, cql.Statement[int32, cql.DynamicRow]{CQL: fmt.Sprintf("SELECT id,val FROM %s WHERE id=?", table), Params: cql.OneParam(cql.Int32), Rows: cql.DynamicRows}, *func() *gocql.PrepareOptions {
				opaqueStorageValuexxxxxx := new(gocql.PrepareOptions)
				opaqueStorageValuexxxxxx.HostID = host
				implExecution.PrepareOptionsData(opaqueStorageValuexxxxxx).InternalConnection = conn
				return opaqueStorageValuexxxxxx
			}())
			bound := cassandraBound(t, prepared, int32(1))
			cassandraReadBound(t, session, bound, *func() *gocql.Options {
				opaqueStorageValuexxxxxxx := new(gocql.Options)
				opaqueStorageValuexxxxxxx.HostID = host
				implExecution.OptionsData(opaqueStorageValuexxxxxxx).InternalConnection = conn
				return opaqueStorageValuexxxxxxx
			}())
			preparedData(prepared).InternalState.InternalMu.Lock()
			entry, ok := preparedData(prepared).InternalState.InternalHosts[host.Value]
			require.True(t, ok)
			cached := *entry
			require.NotEmpty(t, cached.InternalMetadataID)
			cached.InternalMetadataID = nil
			cached.InternalRevision++
			preparedData(prepared).InternalState.InternalHosts[host.Value] = &cached
			preparedData(prepared).InternalState.InternalMu.Unlock()
			row := cassandraReadBound(t, session, bound, *func() *gocql.Options {
				opaqueStorageValuexxxxxxxx := new(gocql.Options)
				opaqueStorageValuexxxxxxxx.HostID = host
				implExecution.OptionsData(opaqueStorageValuexxxxxxxx).InternalConnection = conn
				return opaqueStorageValuexxxxxxxx
			}())
			require.Equal(t, int64(1), cassandraInt(t, row, "id"))
			require.Equal(t, int64(7), cassandraInt(t, row, "val"))
			preparedData(prepared).InternalState.InternalMu.RLock()
			healed := preparedData(prepared).InternalState.InternalHosts[host.Value]
			preparedData(prepared).InternalState.InternalMu.RUnlock()
			require.NotEmpty(t, healed.InternalMetadataID, "empty metadata ID must be repaired from response")
			require.True(t, healed.InternalMetadataChanged)
			require.Greater(t, healed.InternalRevision, cached.InternalRevision)
		})
	})

	// Runs a query bigger than the max allowed size of the payload of a frame,
	// so it should be sent as 2 different frames where each contains a self-contained bit set to zero.
	t.Run("Segment", func(t *testing.T) {
		session := createV2Session(t)
		defer session.Close()
		if implExecution.InternalSessionData((*implExecution.Session)(session)).InternalCfg.ProtoVersion < 5 {
			t.Skip("segmented frames are only produced on protocol >= 5")
		}
		table := testTableName(t)
		cassandraDDL(t, session, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,text_col text)", table))
		text := strings.Repeat("a", 500000)
		cassandraWrite(t, session, fmt.Sprintf("INSERT INTO %s(id,text_col) VALUES(?,?)", table), cql.IntValue(1), codecTestMust(cql.TextValue(text)))
		require.Equal(t, text, cassandraText(t, cassandraRead(t, session, "SELECT text_col FROM "+table), "text_col"))
	})
}
