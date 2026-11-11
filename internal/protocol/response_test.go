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
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestResponse(t *testing.T) {
	t.Run("ParseAuth", func(t *testing.T) {
		t.Parallel()

		// Auth frames are returned after the framer goes back to its pool, so their
		// tokens must not alias the read buffer.
		t.Run("CopiesData", func(t *testing.T) {
			t.Parallel()

			for _, op := range []frm.Op{frm.OpAuthChallenge, frm.OpAuthSuccess} {
				body := bodyOf(func(f *framer) { f.WriteBytes([]byte("token")) })
				got, err := parseOpBody(op, body)
				if err != nil {
					t.Fatal(err)
				}
				for i := range body {
					body[i] = 0xff
				}
				var data []byte
				switch fr := got.(type) {
				case *frm.AuthChallengeFrame:
					data = fr.Data
				case *frm.AuthSuccessFrame:
					data = fr.Data
				}
				if !bytes.Equal(data, []byte("token")) {
					t.Fatalf("%v: Data = %q after the read buffer was overwritten", op, data)
				}
			}
		})
		t.Run("Decodes", func(t *testing.T) {
			t.Parallel()

			hdr := func(op frm.Op) frm.FrameHeader {
				return frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: op, Stream: 3}
			}
			token := []byte{0x01, 0x02, 0x03}

			tests := []struct {
				name string
				op   frm.Op
				body []byte
				want frame
			}{
				{"Authenticate", frm.OpAuthenticate,
					bodyOf(func(f *framer) { f.WriteString("org.apache.cassandra.auth.PasswordAuthenticator") }),
					&frm.AuthenticateFrame{FrameHeader: hdr(frm.OpAuthenticate), Class: "org.apache.cassandra.auth.PasswordAuthenticator"}},
				{"Challenge", frm.OpAuthChallenge,
					bodyOf(func(f *framer) { f.WriteBytes(token) }),
					&frm.AuthChallengeFrame{FrameHeader: hdr(frm.OpAuthChallenge), Data: token}},
				{"ChallengeNull", frm.OpAuthChallenge,
					bodyOf(func(f *framer) { f.WriteInt(-1) }),
					&frm.AuthChallengeFrame{FrameHeader: hdr(frm.OpAuthChallenge)}},
				{"Success", frm.OpAuthSuccess,
					bodyOf(func(f *framer) { f.WriteBytes(token) }),
					&frm.AuthSuccessFrame{FrameHeader: hdr(frm.OpAuthSuccess), Data: token}},
				// SASL mechanisms distinguish an empty final token from none at all.
				{"SuccessEmpty", frm.OpAuthSuccess,
					bodyOf(func(f *framer) { f.WriteBytes([]byte{}) }),
					&frm.AuthSuccessFrame{FrameHeader: hdr(frm.OpAuthSuccess), Data: []byte{}}},
				{"SuccessNull", frm.OpAuthSuccess,
					bodyOf(func(f *framer) { f.WriteInt(-1) }),
					&frm.AuthSuccessFrame{FrameHeader: hdr(frm.OpAuthSuccess)}},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					got, err := parseOpBody(tc.op, tc.body)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("parsed %#v\nwant   %#v", got, tc.want)
					}

					for n := 0; n < len(tc.body); n++ {
						if got, err := parseOpBody(tc.op, append([]byte(nil), tc.body[:n]...)); err == nil {
							t.Fatalf("body truncated to %d of %d bytes parsed as %#v, want an error", n, len(tc.body), got)
						}
					}
				})
			}
		})
	})
	t.Run("ParseError", func(t *testing.T) {
		t.Parallel()

		t.Run("Decodes", func(t *testing.T) {
			t.Parallel()

			for _, tc := range errorFrameCases() {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					got, err := parseErrorBody(tc.proto, tc.body, tc.rateLimitCode)
					if err != nil {
						t.Fatalf("parseFrame: %v", err)
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("parsed %#v\nwant   %#v", got, tc.want)
					}
					// Every server error must surface as an error ending with the
					// server's message, whatever its concrete type.
					e, ok := got.(error)
					if !ok || !strings.HasSuffix(e.Error(), ": "+messageOf(tc.want)) {
						t.Fatalf("parsed frame %T is not an error carrying the server message", got)
					}
				})
			}
		})
		// Every prefix of a valid error body is malformed. Decoding one must return a
		// protocol error, not panic out of parseFrame.
		t.Run("RejectsTruncated", func(t *testing.T) {
			t.Parallel()

			for _, tc := range errorFrameCases() {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					for n := 0; n < len(tc.body)-tc.unread; n++ {
						body := append([]byte(nil), tc.body[:n]...)
						if got, err := parseErrorBody(tc.proto, body, tc.rateLimitCode); err == nil {
							t.Fatalf("body truncated to %d of %d bytes parsed as %#v, want an error", n, len(tc.body), got)
						}
					}
				})
			}
		})
	})
	t.Run("ParseEvent", func(t *testing.T) {
		t.Parallel()

		fr := newFramer(nil, protoVersion4)
		fr.Header = &frm.FrameHeader{Version: protoVersion4}
		fr.WriteString("CLIENT_ROUTES_CHANGE")
		fr.WriteString("UPDATED")
		fr.WriteStringList([]string{"c1", ""})
		fr.WriteStringList([]string{})

		frame := fr.ParseEventFrame()
		evt, ok := frame.(*frm.ClientRoutesChanged)
		if !ok {
			t.Fatalf("expected ClientRoutesChanged frame, got %T", frame)
		}
		if evt.ChangeType != "UPDATED" {
			t.Fatalf("ChangeType = %v, want UPDATED", evt.ChangeType)
		}
		if len(evt.ConnectionIDs) != 2 || evt.ConnectionIDs[1] != "" {
			t.Fatalf("ConnectionIDs = %v, want [c1 \"\"]", evt.ConnectionIDs)
		}
		if len(evt.HostIDs) != 0 {
			t.Fatalf("HostIDs = %v, want empty", evt.HostIDs)
		}
	})
	t.Run("ParsePreparedMetadata", func(t *testing.T) {
		t.Parallel()

		// Pins the upper half of the
		// pkeyCount guard: a count that the remaining buffer can actually supply must be
		// accepted. Without this, a bound tightened by mistake (down to `> 0`, say) would
		// reject every prepared statement that has a partition key and no unit test would
		// notice — pk_count is 0 in every other prepared frame the suite builds.
		t.Run("AcceptsValidPkeyCount", func(t *testing.T) {
			t.Parallel()

			t.Run("exactly at the bound", func(t *testing.T) {
				t.Parallel()

				fr := newFramer(nil, protoVersion4)
				fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask}

				// NO_METADATA so parsing stops right after the pkeys, leaving the buffer
				// holding exactly 2*pkeyCount bytes at the guard: pkeyCount == len(buf)/2.
				fr.WriteInt(int32(frm.FlagNoMetaData))
				fr.WriteInt(0) // colCount
				fr.WriteInt(2) // pkeyCount
				fr.WriteShort(0)
				fr.WriteShort(1)

				// NotPanics, because parsePreparedMetadata is called directly here and so
				// the guard's panic would otherwise take down the whole test binary rather
				// than failing this subtest.
				var meta preparedMetadata
				require.NotPanics(t, func() { meta = fr.ParsePreparedMetadata() })

				require.Equal(t, []int{0, 1}, meta.PkeyColumns)
				require.Empty(t, fr.Buf, "whole metadata block should be consumed")
			})

			t.Run("full prepared frame", func(t *testing.T) {
				t.Parallel()

				fr := newFramer(nil, protoVersion4)
				fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}

				fr.WriteInt(frm.ResultKindPrepared)
				fr.WriteShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
				// prepared metadata
				fr.WriteInt(int32(frm.FlagGlobalTableSpec))
				fr.WriteInt(2) // colCount
				fr.WriteInt(2) // pkeyCount
				fr.WriteShort(0)
				fr.WriteShort(1)
				fr.WriteString("test_ks")
				fr.WriteString("test_tbl")
				fr.WriteString("pk_a")
				fr.WriteShort(uint16(TypeInt))
				fr.WriteString("pk_b")
				fr.WriteShort(uint16(TypeVarchar))
				// result metadata
				fr.WriteInt(int32(frm.FlagNoMetaData))
				fr.WriteInt(0)

				frame, err := fr.ParseFrame()
				require.NoError(t, err)

				prepared, ok := frame.(*resultPreparedFrame)
				require.True(t, ok, "got %T", frame)
				require.Equal(t, []int{0, 1}, prepared.ReqMeta.PkeyColumns)
				require.Len(t, prepared.ReqMeta.Columns, 2)
				require.Empty(t, fr.Buf, "whole frame should be consumed")
			})
		})
		// Verifies that a RESULT/Prepared
		// frame declaring a negative or absurdly large partition-key count is reported as
		// an error rather than crashing the serve goroutine (a negative count makes
		// make([]int, n) raise a runtime error that parseFrame's recover re-panics) or
		// forcing a huge speculative allocation from a small frame.
		t.Run("RejectsInvalidPkeyCount", func(t *testing.T) {
			t.Parallel()

			cases := []struct {
				name      string
				pkeyCount int32
			}{
				{name: "negative", pkeyCount: -1},
				{name: "huge", pkeyCount: 1 << 30},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					fr := newFramer(nil, protoVersion4)
					fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}

					fr.WriteInt(frm.ResultKindPrepared)
					fr.WriteShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
					// prepared metadata: flags=0, colCount=0, then the bogus pkeyCount.
					fr.WriteInt(0)
					fr.WriteInt(0)
					fr.WriteInt(tc.pkeyCount)

					frame, err := fr.ParseFrame()
					if err == nil {
						t.Fatalf("expected an error for pkeyCount=%d, got frame %+v", tc.pkeyCount, frame)
					}
					if frame != nil {
						t.Errorf("expected nil frame on error, got %+v", frame)
					}
					// Assert on the message, not just on err != nil. Without the guard the
					// "huge" case still returns an error — make() succeeds and the first
					// readShort() then panics on the empty buffer — so a bare err != nil
					// check passes either way and the allocation bound goes untested.
					require.ErrorContains(t, err, "invalid partition key count")
				})
			}
		})
	})
	t.Run("ParseResultMetadata", func(t *testing.T) {
		t.Parallel()

		// Pins that the
		// METADATA_CHANGED field is only on the wire from v5 onwards: on v4 the flag bit
		// must not cause a read, or the parser would consume bytes that belong to the
		// column specs.
		t.Run("NewMetadataIDIgnoredBelowV5", func(t *testing.T) {
			t.Parallel()

			fr := newFramer(nil, protoVersion4)
			fr.Header = &frm.FrameHeader{Version: protoVersion4}

			// METADATA_CHANGED set but no id on the wire, as a v4 server would send.
			fr.WriteInt(int32(frm.FlagGlobalTableSpec | frm.FlagMetaDataChanged))
			fr.WriteInt(1)
			fr.WriteString("test_ks")
			fr.WriteString("test_tbl")
			fr.WriteString("col_a")
			fr.WriteShort(uint16(TypeInt))

			meta := fr.ParseResultMetadata()

			require.Nil(t, meta.NewMetadataID, "no metadata id should be read below v5")
			require.Len(t, meta.Columns, 1)
			require.Equal(t, "col_a", meta.Columns[0].Name)
			require.Empty(t, fr.Buf, "whole metadata block should be consumed")
		})
		// Pins the wire order of
		// the two optional fields in a v5 RESULT/Rows <metadata> block.
		//
		// With only one of HAS_MORE_PAGES / METADATA_CHANGED set, either ordering parses
		// identically, so no other test in this repo can tell them apart. Only a frame
		// carrying both distinguishes them, and such a frame is hard to obtain from a
		// live server (it needs the client to execute with a stale result_metadata_id
		// while a page boundary falls in the same response). Synthesising it here is
		// both deterministic and stronger than an integration test.
		//
		// The layout asserted below is the one Cassandra actually emits, read out of
		// ResultSet$ResultMetadata$Codec.encode in the 5.0.6 distribution:
		//
		//	writeInt(flags)
		//	writeInt(columnCount)
		//	if HAS_MORE_PAGES:                CBUtil.writeValue(pagingState)    // [bytes]
		//	if v5+ && METADATA_CHANGED:       CBUtil.writeBytes(resultMetadataId) // [short bytes]
		//	if !NO_METADATA:                  global table spec / column specs
		//
		// Note the differing wire types: paging state is [bytes] (4-byte length) and the
		// metadata id is [short bytes] (2-byte length), so swapping the two reads
		// desynchronises the rest of the block rather than merely exchanging the values.
		//
		// Run for both ways the field can be on the wire: native v5, and v4 with
		// SCYLLA_USE_METADATA_ID negotiated. The extension reuses this parser, so the v4
		// case is the only unit coverage of the read side of METADATA_CHANGED on v4 —
		// otherwise it rests entirely on an integration test that skips unless a capable
		// server is present.
		t.Run("PagingStateBeforeNewMetadataID", func(t *testing.T) {
			t.Parallel()

			const (
				keyspace = "test_ks"
				table    = "test_tbl"
			)
			pagingState := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}
			newMetadataID := []byte{0xAA, 0xBB, 0xCC}

			for _, tc := range []struct {
				name      string
				proto     byte
				extension bool
			}{
				{name: "native v5", proto: protoVersion5},
				{name: "v4 with SCYLLA_USE_METADATA_ID", proto: protoVersion4, extension: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					fr := newFramer(nil, tc.proto)
					fr.Header = &frm.FrameHeader{Version: frm.ProtoVersion(tc.proto)}
					fr.ScyllaUseMetadataID = tc.extension

					fr.WriteInt(int32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagMetaDataChanged))
					fr.WriteInt(1) // colCount
					fr.WriteBytes(pagingState)
					fr.WriteShortBytes(newMetadataID)
					fr.WriteString(keyspace)
					fr.WriteString(table)
					fr.WriteString("col_a")
					fr.WriteShort(uint16(TypeInt))

					meta := fr.ParseResultMetadata()

					assertDeepEqual(t, "pagingState", pagingState, meta.PagingState)
					assertDeepEqual(t, "newMetadataID", newMetadataID, meta.NewMetadataID)

					// The column spec must still be readable, which is what actually proves the
					// two optional fields were consumed in the right order and with the right
					// wire types.
					require.Len(t, meta.Columns, 1)
					require.Equal(t, keyspace, meta.Columns[0].Keyspace)
					require.Equal(t, table, meta.Columns[0].Table)
					require.Equal(t, "col_a", meta.Columns[0].Name)
					require.Empty(t, fr.Buf, "whole metadata block should be consumed")
				})
			}
		})
		t.Run("PerColumnSpec", func(t *testing.T) {
			t.Parallel()

			// Build a synthetic ROWS result metadata frame with FlagGlobalTableSpec unset
			// (per-column keyspace/table encoding). This tests the !globalSpec optimization
			// in parseResultMetadata() which reads keyspace/table from the first column
			// position and reuses them for all columns via skipString().
			fr := newFramer(nil, protoVersion4)
			fr.Header = &frm.FrameHeader{Version: protoVersion4}

			// flags: no FlagGlobalTableSpec — per-column keyspace/table
			fr.WriteInt(0)
			// colCount
			fr.WriteInt(3)

			// Column 0: keyspace/table + name + type
			fr.WriteString("test_ks")
			fr.WriteString("test_tbl")
			fr.WriteString("col_a")
			fr.WriteShort(uint16(TypeInt))

			// Column 1: same keyspace/table (will be skipped by optimization)
			fr.WriteString("test_ks")
			fr.WriteString("test_tbl")
			fr.WriteString("col_b")
			fr.WriteShort(uint16(TypeVarchar))

			// Column 2: same keyspace/table
			fr.WriteString("test_ks")
			fr.WriteString("test_tbl")
			fr.WriteString("col_c")
			fr.WriteShort(uint16(TypeBoolean))

			meta := fr.ParseResultMetadata()

			if meta.ColCount != 3 {
				t.Fatalf("colCount = %d, want 3", meta.ColCount)
			}
			if len(meta.Columns) != 3 {
				t.Fatalf("len(columns) = %d, want 3", len(meta.Columns))
			}

			// Verify all columns got the correct keyspace/table from the optimization
			for i, col := range meta.Columns {
				if col.Keyspace != "test_ks" {
					t.Errorf("columns[%d].Keyspace = %q, want %q", i, col.Keyspace, "test_ks")
				}
				if col.Table != "test_tbl" {
					t.Errorf("columns[%d].Table = %q, want %q", i, col.Table, "test_tbl")
				}
			}

			// Verify column names
			expectedNames := []string{"col_a", "col_b", "col_c"}
			for i, col := range meta.Columns {
				if col.Name != expectedNames[i] {
					t.Errorf("columns[%d].Name = %q, want %q", i, col.Name, expectedNames[i])
				}
			}

			// Verify column types
			expectedTypes := []Type{TypeInt, TypeVarchar, TypeBoolean}
			for i, col := range meta.Columns {
				nt, ok := col.TypeInfo.(NativeType)
				if !ok {
					t.Fatalf("columns[%d].TypeInfo is %T, want NativeType", i, col.TypeInfo)
				}
				if cqltype.NativeKind(nt) != expectedTypes[i] {
					t.Errorf("columns[%d].Type = %v, want %v", i, cqltype.NativeKind(nt), expectedTypes[i])
				}
			}

			// Verify the entire buffer was consumed (no misalignment from skipString)
			if len(fr.Buf) != 0 {
				t.Errorf("buffer has %d unconsumed bytes, want 0 (possible skipString misalignment)", len(fr.Buf))
			}
		})
	})
	// Verifies that a malformed
	// RESULT/Prepared frame whose resultMetadataID short-bytes length runs past the
	// frame body is reported as an error, not a serve-goroutine panic. The extension
	// makes this field live on protocol v4, and readShortBytesCopy panics with a
	// plain error on a short buffer; parseFrame's recover must convert it to a
	// returned error.
	t.Run("ParseResultPrepared", func(t *testing.T) {
		t.Parallel()

		fr := newFramer(nil, protoVersion4)
		// Response direction bit set so parseFrame does not reject it as a request.
		fr.Header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}
		fr.ScyllaUseMetadataID = true

		fr.WriteInt(frm.ResultKindPrepared)
		fr.WriteShortBytes([]byte{0x01, 0x02, 0x03}) // preparedID
		// resultMetadataID: claim 10 bytes but supply none.
		fr.WriteShort(10)

		frame, err := fr.ParseFrame()
		if err == nil {
			t.Fatalf("expected an error for a truncated resultMetadataID, got frame %+v", frame)
		}
		if frame != nil {
			t.Errorf("expected nil frame on error, got %+v", frame)
		}
	})
	// Pins that a malformed VectorType
	// column is an error rather than a dead serve goroutine. The well-formed cases
	// show the guard did not narrow the grammar, and "VectorTypeXxx" that a name
	// merely sharing the prefix degrades to an opaque custom type.
	t.Run("ParseResultRows", func(t *testing.T) {
		t.Parallel()

		const p = apacheCassandraTypePrefix
		const vp = p + "VectorType"

		tests := []struct {
			name    string
			custom  string
			wantErr bool
			want    TypeInfo
		}{
			{name: "no argument list", custom: vp, wantErr: true},
			{name: "unclosed argument list", custom: vp + "(", wantErr: true},
			{name: "empty argument list", custom: vp + "()", wantErr: true},
			{name: "no dimensions", custom: vp + "(" + p + "FloatType)", wantErr: true},
			{name: "empty subtype", custom: vp + "(, 3)", wantErr: true},
			{name: "unterminated", custom: vp + "(" + p + "FloatType, 3", wantErr: true},
			{name: "dimensions not a number", custom: vp + "(" + p + "FloatType, abc)", wantErr: true},
			{name: "negative dimensions", custom: vp + "(" + p + "FloatType, -3)", wantErr: true},
			{name: "zero dimensions", custom: vp + "(" + p + "FloatType, 0)", wantErr: true},
			{
				name:   "not a vector type",
				custom: vp + "Xxx(" + p + "FloatType, 3)",
				want:   NewCustomType(protoVersion4, TypeCustom, vp+"Xxx("+p+"FloatType, 3)"),
			},
			{
				name:   "float vector",
				custom: vp + "(" + p + "FloatType, 3)",
				want: VectorType{
					NativeType: NewCustomType(protoVersion4, TypeCustom, vp+"("+p+"FloatType, 3)"),
					SubType:    NewNativeType(protoVersion4, TypeFloat),
					Dimensions: 3,
				},
			},
			{
				name:   "nested vector",
				custom: vp + "(" + vp + "(" + p + "InetAddressType, 2), 3)",
				want: VectorType{
					NativeType: NewCustomType(protoVersion4, TypeCustom, vp+"("+vp+"("+p+"InetAddressType, 2), 3)"),
					SubType: VectorType{
						NativeType: NewCustomType(protoVersion4, TypeCustom, vp),
						SubType:    NewNativeType(protoVersion4, TypeInet),
						Dimensions: 2,
					},
					Dimensions: 3,
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				fr := newFramer(nil, protoVersion4)
				// Response direction bit set so parseFrame does not reject it as a request.
				fr.Header = &frm.FrameHeader{Version: protoVersion4 | 0x80, Op: frm.OpResult}
				writeRowsResultWithColumnType(fr, test.custom)

				frame, err := fr.ParseFrame()
				if test.wantErr {
					if err == nil {
						t.Fatalf("expected an error for %q, got frame %+v", test.custom, frame)
					}
					if frame != nil {
						t.Errorf("expected nil frame on error, got %+v", frame)
					}
					return
				}

				require.NoError(t, err)
				rows, ok := frame.(*resultRowsFrame)
				require.True(t, ok, "expected a resultRowsFrame, got %T", frame)
				require.Len(t, rows.Meta.Columns, 1)
				require.Equal(t, test.want, rows.Meta.Columns[0].TypeInfo)
			})
		}
	})
	t.Run("ReadErrorMap", func(t *testing.T) {
		t.Parallel()

		// A negative failure count reads as an empty map rather than an error.
		t.Run("NegativeCountIsEmpty", func(t *testing.T) {
			t.Parallel()

			body := errBody(ProtoVersion5, CodeReadFailure, "rf", func(f *Framer) {
				f.WriteConsistency(cqltype.One)
				f.WriteInt(0)
				f.WriteInt(1)
				f.WriteInt(-1)
				f.WriteByteValue(0)
			})
			got, err := parseErrorBody(ProtoVersion5, body, 0)
			if err != nil {
				t.Fatal(err)
			}
			rf := got.(*ErrorResponseFrame).Err.(*ReadFailureError)
			if len(rf.ErrorMap) != 0 || rf.NumFailures != 0 {
				t.Fatalf("ErrorMap = %v, NumFailures = %d; want empty", rf.ErrorMap, rf.NumFailures)
			}
		})
		t.Run("RejectsBadAddress", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name    string
				write   func(f *Framer)
				wantErr string
			}{
				{"InvalidSize", func(f *Framer) {
					f.WriteInt(1)
					f.WriteByteValue(5)
					f.Buf = append(f.Buf, 1, 2, 3, 4, 5)
					f.WriteShort(1)
				}, "invalid IP size: 5"},
				{"ZeroSize", func(f *Framer) {
					f.WriteInt(1)
					f.WriteByteValue(0)
					f.WriteShort(1)
				}, "invalid IP size: 0"},
				{"ShortAddress", func(f *Framer) {
					f.WriteInt(1)
					f.WriteByteValue(16)
					f.Buf = append(f.Buf, 1, 2, 3, 4)
				}, "not enough bytes in buffer to read inet"},
				{"MoreEntriesThanData", func(f *Framer) {
					f.WriteInt(3)
					writeInetOnly(f, net.ParseIP("10.0.0.1"))
					f.WriteShort(1)
					// what follows is read as the next address and rejected
				}, "invalid IP size"},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					body := errBody(ProtoVersion5, CodeReadFailure, "rf", func(f *Framer) {
						f.WriteConsistency(cqltype.One)
						f.WriteInt(0)
						f.WriteInt(1)
						tc.write(f)
						f.WriteByteValue(0)
					})
					_, err := parseErrorBody(ProtoVersion5, body, 0)
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("err = %v, want %q", err, tc.wantErr)
					}
				})
			}
		})
	})
	t.Run("ReadTypeInfo", func(t *testing.T) {
		t.Parallel()

		native := func(t Type) NativeType { return cqltype.NewCustomType(protoVersion4, t, "") }

		tests := []struct {
			name string
			opt  typeOption
			want TypeInfo
		}{
			{"Native", optNative(TypeInt), native(TypeInt)},
			{"NativeDuration", optNative(TypeDuration), native(TypeDuration)},
			// Cassandra's marshal class names for native types resolve to the
			// native type, keeping the class name.
			{"CustomNativeClass", optCustom("org.apache.cassandra.db.marshal.Int32Type"), cqltype.NewCustomType(protoVersion4, TypeInt, "org.apache.cassandra.db.marshal.Int32Type")},
			{"CustomUnknownClass", optCustom("com.example.MyType"), cqltype.NewCustomType(protoVersion4, TypeCustom, "com.example.MyType")},
			{"List", optCollection(TypeList, optNative(TypeVarchar)),
				CollectionType{NativeType: native(TypeList), Elem: native(TypeVarchar)}},
			{"Set", optCollection(TypeSet, optNative(TypeUUID)),
				CollectionType{NativeType: native(TypeSet), Elem: native(TypeUUID)}},
			{"Map", optCollection(TypeMap, optNative(TypeVarchar), optNative(TypeBigInt)),
				CollectionType{NativeType: native(TypeMap), Key: native(TypeVarchar), Elem: native(TypeBigInt)}},
			{"Tuple", optTuple(optNative(TypeInt), optNative(TypeBoolean)),
				TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{native(TypeInt), native(TypeBoolean)}}},
			{"EmptyTuple", optTuple(), TupleTypeInfo{NativeType: native(TypeTuple), Elems: []TypeInfo{}}},
			{"UDT", optUDT("ks", "addr",
				optField{"street", optNative(TypeVarchar)},
				optField{"zip", optNative(TypeInt)},
			), UDTTypeInfo{NativeType: native(TypeUDT), KeySpace: "ks", Name: "addr", Elements: []UDTField{
				{Name: "street", Type: native(TypeVarchar)},
				{Name: "zip", Type: native(TypeInt)},
			}}},
			{"Nested", optCollection(TypeMap,
				optNative(TypeVarchar),
				optCollection(TypeList, optTuple(optNative(TypeInt), optUDT("ks", "u", optField{"f", optNative(TypeBlob)}))),
			), CollectionType{
				NativeType: native(TypeMap),
				Key:        native(TypeVarchar),
				Elem: CollectionType{NativeType: native(TypeList), Elem: TupleTypeInfo{
					NativeType: native(TypeTuple),
					Elems: []TypeInfo{native(TypeInt), UDTTypeInfo{
						NativeType: native(TypeUDT), KeySpace: "ks", Name: "u",
						Elements: []UDTField{{Name: "f", Type: native(TypeBlob)}},
					}},
				}},
			}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				got, rest := readTypeOption(tc.opt)
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("readTypeInfo = %#v\nwant %#v", got, tc.want)
				}
				if rest != 0 {
					t.Fatalf("%d bytes left unread", rest)
				}
			})
		}
	})
	t.Run("ServerErrorCode", func(t *testing.T) {
		for _, err := range []error{
			NewOtherServerError(CodeSyntax, "syntax"),
			&UnavailableError{serverError: serverError{message: "unavailable"}},
			&WriteTimeoutError{serverError: serverError{message: "write timeout"}},
			&UnpreparedError{serverError: serverError{message: "unprepared"}},
		} {
			direct, ok := errors.AsType[ServerError](err)
			if !ok {
				t.Fatalf("%T does not satisfy server error contract", err)
			}
			if wrapped, ok := errors.AsType[ServerError](fmt.Errorf("outer: %w", err)); !ok || wrapped.Code() != direct.Code() || wrapped.Message() != direct.Message() {
				t.Fatalf("wrapped %T lost server details", err)
			}
		}
	})
	t.Run("TypeString", func(t *testing.T) {
		t.Parallel()

		if got := Type(0x00ff).String(); got != "unknown_type_255" {
			t.Fatalf("String() = %q, want unknown_type_255", got)
		}
	})
}
