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

package protocol

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestRequestFrame(t *testing.T) {
	t.Run("QueryParams", func(t *testing.T) {
		nowInSeconds := 123

		cases := []struct {
			name    string
			proto   byte
			opts    fixtureQueryParams
			wantErr string
		}{
			{
				name:    "keyspace on v4",
				proto:   protoVersion4,
				opts:    fixtureQueryParams{consistency: Quorum, keyspace: "ks"},
				wantErr: "keyspace override can only be set with protocol v5 or higher",
			},
			{
				name:    "nowInSeconds on v4",
				proto:   protoVersion4,
				opts:    fixtureQueryParams{consistency: Quorum, nowInSeconds: &nowInSeconds},
				wantErr: "now_in_seconds can only be set with protocol v5 or higher",
			},
		}

		writers := []struct {
			name  string
			build func(opts fixtureQueryParams) frameBuilder
		}{
			{
				name: "QUERY",
				build: func(opts fixtureQueryParams) frameBuilder {
					return &fixtureQueryFrame{statement: "SELECT * FROM system.local", params: opts}
				},
			},
			{
				name: "EXECUTE",
				build: func(opts fixtureQueryParams) frameBuilder {
					return &fixtureExecuteFrame{
						preparedID:       []byte{0x01, 0x02},
						resultMetadataID: []byte{0x03, 0x04},
						params:           opts,
					}
				},
			},
		}

		for _, w := range writers {
			t.Run(w.name, func(t *testing.T) {
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						framer := newFramer(nil, tc.proto)

						err := w.build(tc.opts).BuildFrame(framer, 1)
						require.Error(t, err)
						require.Contains(t, err.Error(), tc.wantErr)
						require.Empty(t, framer.Buf, "a rejected option must not leave a partial frame in the framer buffer")
					})
				}
			})
		}
	})
	t.Run("ShortBytesWriters", func(t *testing.T) {
		tooLong := make([]byte, math.MaxUint16+1)
		ok := []byte{0x01, 0x02}

		for _, tc := range []struct {
			name  string
			proto byte
			build frameBuilder
		}{
			{
				name:  "EXECUTE prepared id",
				proto: protoVersion4,
				build: &fixtureExecuteFrame{preparedID: tooLong, params: fixtureQueryParams{consistency: Quorum}},
			},
			{
				name:  "EXECUTE result metadata id on v5",
				proto: protoVersion5,
				build: &fixtureExecuteFrame{preparedID: ok, resultMetadataID: tooLong, params: fixtureQueryParams{consistency: Quorum}},
			},
			{
				name:  "BATCH prepared id",
				proto: protoVersion4,
				build: &fixtureBatchFrame{
					consistency: Quorum,
					statements:  []fixtureBatchStatement{{preparedID: tooLong}},
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				framer := newFramer(nil, tc.proto)

				err := tc.build.BuildFrame(framer, 1)
				require.Error(t, err)
				require.Contains(t, err.Error(), "overflows the 2-byte length")
				require.Empty(t, framer.Buf, "a rejected value must not leave a partial frame in the framer buffer")
			})
		}
	})
	// This is the positive half: the
	// shared validator must not reject what v5 does support, or the writers above
	// would fail every legitimate v5 request.
	t.Run("ValidateV5Options", func(t *testing.T) {
		nowInSeconds := 123
		minInt32, maxInt32 := math.MinInt32, math.MaxInt32

		v5 := newFramer(nil, protoVersion5)
		require.NoError(t, v5.ValidateV5Options("ks", &nowInSeconds))
		require.NoError(t, v5.ValidateV5Options("", &minInt32))
		require.NoError(t, v5.ValidateV5Options("", &maxInt32))

		// And neither option set is required: v4 requests must still pass.
		require.NoError(t, newFramer(nil, protoVersion4).ValidateV5Options("", nil))
	})
	t.Run("WriteBatch", func(t *testing.T) {
		t.Run("Encodes", func(t *testing.T) {
			framer := newFramer(nil, protoVersion5)
			nowInSeconds := 123
			frame := fixtureBatchFrame{
				customPayload: map[string][]byte{
					"key1": []byte("value1"),
				},
				nowInSeconds: &nowInSeconds,
			}

			err := fixtureWriteBatch(framer, 123, &frame, frame.customPayload)
			if err != nil {
				t.Fatal(err)
			}

			// skipping header
			framer.Buf = framer.Buf[9:]

			assertDeepEqual(t, "customPayload", frame.customPayload, framer.ReadBytesMap())
			assertDeepEqual(t, "typ", frame.typ, BatchKind(framer.ReadByteValue()))
			assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.ReadShort()))
			assertDeepEqual(t, "consistency", frame.consistency, Consistency(framer.ReadShort()))

			flags := framer.ReadInt()
			if flags&int(frm.FlagWithNowInSeconds) != int(frm.FlagWithNowInSeconds) {
				t.Fatal("expected flagNowInSeconds to be set, but it is not")
			}

			assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.ReadInt())
		})
		t.Run("RejectsUnsupportedOptionsOnV4", func(t *testing.T) {
			nowInSeconds := 123

			namedValues := []fixtureQueryValue{{name: "id", value: []byte{1}}}

			// wantErr pins which validation rejected the frame, so a case cannot pass by
			// tripping some unrelated error.
			cases := []struct {
				name    string
				proto   byte
				frame   fixtureBatchFrame
				wantErr string
			}{
				{
					name:    "keyspace on v4",
					proto:   protoVersion4,
					frame:   fixtureBatchFrame{keyspace: "ks"},
					wantErr: "keyspace override can only be set with protocol v5 or higher",
				},
				{
					name:    "nowInSeconds on v4",
					proto:   protoVersion4,
					frame:   fixtureBatchFrame{nowInSeconds: &nowInSeconds},
					wantErr: "now_in_seconds can only be set with protocol v5 or higher",
				},
				// Named values are rejected on every protocol version (CASSANDRA-10246),
				// for both raw-statement and prepared-id batch entries.
				{
					name:  "named values on v4",
					proto: protoVersion4,
					frame: fixtureBatchFrame{
						statements: []fixtureBatchStatement{{statement: "INSERT INTO t (id) VALUES (?)", values: namedValues}},
					},
					wantErr: "named query values are not supported in batches",
				},
				{
					name:  "named values on v5",
					proto: protoVersion5,
					frame: fixtureBatchFrame{
						statements: []fixtureBatchStatement{{statement: "INSERT INTO t (id) VALUES (?)", values: namedValues}},
					},
					wantErr: "named query values are not supported in batches",
				},
				{
					name:  "named values on a prepared statement",
					proto: protoVersion5,
					frame: fixtureBatchFrame{
						statements: []fixtureBatchStatement{{preparedID: []byte{0xAA, 0xBB}, values: namedValues}},
					},
					wantErr: "named query values are not supported in batches",
				},
				{
					// The rejection must scan every statement, not just the first.
					name:  "named values in a later statement",
					proto: protoVersion5,
					frame: fixtureBatchFrame{
						statements: []fixtureBatchStatement{
							{statement: "INSERT INTO t (id) VALUES (?)", values: []fixtureQueryValue{{value: []byte{1}}}},
							{statement: "INSERT INTO u (id) VALUES (?)", values: namedValues},
						},
					},
					wantErr: "named query values are not supported in batches",
				},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					framer := newFramer(nil, tc.proto)
					err := fixtureWriteBatch(framer, 1, &tc.frame, tc.frame.customPayload)
					if err == nil {
						t.Fatal("expected an error, got nil")
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("error %q does not contain %q", err, tc.wantErr)
					}
					if len(framer.Buf) != 0 {
						t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.Buf))
					}
				})
			}
		})
		// Guards the happy path through the
		// statement/values write loop (unnamed positional values), which must still
		// succeed after named-value rejection was hoisted out of that loop.
		t.Run("UnnamedValues", func(t *testing.T) {
			framer := newFramer(nil, protoVersion5)
			frame := fixtureBatchFrame{
				typ:         LoggedBatch,
				consistency: Quorum,
				statements: []fixtureBatchStatement{
					{
						statement: "INSERT INTO t (id, v) VALUES (?, ?)",
						values: []fixtureQueryValue{
							{value: []byte{0, 0, 0, 1}},
							{value: []byte("x")},
						},
					},
				},
			}

			if err := fixtureWriteBatch(framer, 1, &frame, frame.customPayload); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// skipping header
			framer.Buf = framer.Buf[9:]
			assertDeepEqual(t, "typ", frame.typ, BatchKind(framer.ReadByteValue()))
			assertDeepEqual(t, "len(statements)", len(frame.statements), int(framer.ReadShort()))
			assertDeepEqual(t, "kind", byte(0), framer.ReadByteValue()) // 0 = raw query string
			assertDeepEqual(t, "statement", frame.statements[0].statement, framer.ReadLongString())
			assertDeepEqual(t, "len(values)", len(frame.statements[0].values), int(framer.ReadShort()))
			assertDeepEqual(t, "value0", frame.statements[0].values[0].value, framer.ReadBytesCopy())
			assertDeepEqual(t, "value1", frame.statements[0].values[1].value, framer.ReadBytesCopy())
		})
	})
	t.Run("WriteExecute", func(t *testing.T) {
		tests := []struct {
			name                 string
			protoVersion         byte
			scyllaUseMetadataID  bool
			resultMetadataID     []byte
			wantResultMetadataID []byte
		}{
			{
				name:                "protoVersion4 with ScyllaUseMetadataID false",
				protoVersion:        protoVersion4,
				scyllaUseMetadataID: false,
				resultMetadataID:    []byte{},
				// resultMetadataID is not written on v4 without the extension, so it is not read back.
			},
			{
				name:                 "protoVersion4 with ScyllaUseMetadataID true",
				protoVersion:         protoVersion4,
				scyllaUseMetadataID:  true,
				resultMetadataID:     []byte{4, 5, 6},
				wantResultMetadataID: []byte{4, 5, 6},
			},
			{
				name:                "protoVersion4 with ScyllaUseMetadataID true & nil resultMetadataID",
				protoVersion:        protoVersion4,
				scyllaUseMetadataID: true,
				// A resultPreparedFrame with a nil resultMetadataID (e.g. copyBytes(nil))
				// must serialize to a zero-length short bytes and read back as []byte{}.
				resultMetadataID:     nil,
				wantResultMetadataID: []byte{},
			},
			{
				name:                 "protoVersion5 with resultMetadataID support",
				protoVersion:         protoVersion5,
				scyllaUseMetadataID:  false,
				resultMetadataID:     []byte{4, 5, 6},
				wantResultMetadataID: []byte{4, 5, 6},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				framer := newFramer(nil, tt.protoVersion)
				if tt.scyllaUseMetadataID {
					framer.ScyllaUseMetadataID = true
				}

				nowInSeconds := 123
				// A non-zero consistency, so reading it back is an assertion that can
				// actually fail: with the zero value on both sides a misaligned read of
				// Consistency(ANY) would still compare equal.
				params := fixtureQueryParams{consistency: Quorum}
				if tt.protoVersion >= protoVersion5 {
					params.nowInSeconds = &nowInSeconds
					params.keyspace = "test_keyspace"
				}
				frame := fixtureExecuteFrame{
					preparedID:       []byte{1, 2, 3},
					resultMetadataID: tt.resultMetadataID,
					customPayload: map[string][]byte{
						"key1": []byte("value1"),
					},
					params: params,
				}

				err := fixtureWriteExecute(framer, 123, frame.preparedID, frame.resultMetadataID, &frame.params, &frame.customPayload)
				if err != nil {
					t.Fatal(err)
				}

				// skipping header
				framer.Buf = framer.Buf[9:]

				assertDeepEqual(t, "customPayload", frame.customPayload, framer.ReadBytesMap())
				assertDeepEqual(t, "preparedID", frame.preparedID, framer.ReadShortBytesCopy())

				if tt.protoVersion >= protoVersion5 || tt.scyllaUseMetadataID {
					assertDeepEqual(t, "resultMetadataID", tt.wantResultMetadataID, framer.ReadShortBytesCopy())
				}

				assertDeepEqual(t, "constistency", frame.params.consistency, Consistency(framer.ReadShort()))

				if tt.protoVersion >= protoVersion5 {
					flags := framer.ReadInt()
					if flags&int(frm.FlagWithNowInSeconds) != int(frm.FlagWithNowInSeconds) {
						t.Fatal("expected flagNowInSeconds to be set, but it is not")
					}

					if flags&int(frm.FlagWithKeyspace) != int(frm.FlagWithKeyspace) {
						t.Fatal("expected flagWithKeyspace to be set, but it is not")
					}
					assertDeepEqual(t, "keyspace", frame.params.keyspace, framer.ReadString())
					assertDeepEqual(t, "nowInSeconds", nowInSeconds, framer.ReadInt())
				} else {
					// Below v5 the query flags are a single byte, and nothing follows them
					// here. Consuming them and requiring the buffer to be empty turns this
					// into a check that every preceding field was read at exactly the length
					// it was written — in particular the resultMetadataID short bytes.
					require.Zero(t, framer.ReadByteValue(), "no query flags expected")
					require.Empty(t, framer.Buf, "whole EXECUTE body should be consumed")
				}
			})
		}
	})
	t.Run("WritePrepare", func(t *testing.T) {
		framer := newFramer(nil, protoVersion4)
		prep := &writePrepareFrame{Statement: "SELECT * FROM t", Keyspace: "ks"}

		// Must return an error, not panic.
		if err := prep.BuildFrame(framer, 1); err == nil {
			t.Fatal("expected an error, got nil")
		}
		if len(framer.Buf) != 0 {
			t.Fatalf("expected framer buffer to be untouched on error, got %d bytes", len(framer.Buf))
		}
	})
}
