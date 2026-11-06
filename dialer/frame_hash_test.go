// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package dialer

import (
	"bytes"
	"math"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/murmur"
)

func TestFrameHash(t *testing.T) {
	t.Run("Batch", func(t *testing.T) {
		// Bounds walks every truncation of a BATCH frame, plus a few
		// overstated lengths. Every length in a batch body is file-supplied, so none of them
		// may index past the frame.
		t.Run("Bounds", func(t *testing.T) {
			full := frameV4(opBatch, 0x00, batchBody(byte(frm.FlagDefaultTimestamp), timestamp8(111)))

			for n := 0; n <= len(full); n++ {
				truncated := append([]byte(nil), full[:n]...)
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("GetFrameHash panicked on a %d-byte prefix: %v", n, r)
						}
					}()
					GetFrameHash(truncated, false)
				}()
			}

			for _, tc := range []struct {
				name string
				body []byte
			}{
				{name: "overstated statement count", body: []byte{0x00, 0x7F, 0xFF, 0x00}},
				{name: "overstated query length", body: []byte{0x00, 0x00, 0x01, 0x00, 0x7F, 0xFF, 0xFF, 0xFF, 'x'}},
				{name: "overstated prepared id length", body: []byte{0x00, 0x00, 0x01, 0x01, 0x7F, 0xFF, 'x'}},
				{name: "overstated value count", body: append(append([]byte{0x00, 0x00, 0x01, 0x00}, longString("q")...), 0x7F, 0xFF)},
				{name: "unknown statement kind", body: []byte{0x00, 0x00, 0x01, 0x09, 0x00}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					frame := frameV4(opBatch, 0x00, tc.body)
					if got, want := GetFrameHash(frame, false), murmur3OfStreamBlanked(frame); got != want {
						t.Errorf("GetFrameHash = %d, want the raw-bytes fallback %d", got, want)
					}
				})
			}
		})

		// DistinguishesStatements pins the other half: walking the body
		// has to cover the statements, or every batch of the same shape hashes alike and the
		// replayer serves the first one it finds.
		t.Run("DistinguishesStatements", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				a, b []byte
			}{
				{
					name: "query text",
					a:    batchBody(0x00),
					b: func() []byte {
						body := []byte{0x00, 0x00, 0x01, 0x00}
						body = append(body, longString("INSERT INTO t (pk) VALUES (2)")...)
						body = append(body, 0x00, 0x00, 0x00, 0x01, 0x00)
						return body
					}(),
				},
				{
					name: "statement count",
					a:    batchBody(0x00),
					b: func() []byte {
						body := []byte{0x00, 0x00, 0x02}
						for i := 0; i < 2; i++ {
							body = append(body, 0x00)
							body = append(body, longString("INSERT INTO t (pk) VALUES (1)")...)
							body = append(body, 0x00, 0x00)
						}
						return append(body, 0x00, 0x01, 0x00)
					}(),
				},
				{
					name: "bound values",
					a: func() []byte {
						body := []byte{0x00, 0x00, 0x01, 0x00}
						body = append(body, longString("INSERT INTO t (pk) VALUES (?)")...)
						body = append(body, 0x00, 0x01)
						body = append(body, bytesValue([]byte{0x2A})...)
						return append(body, 0x00, 0x01, 0x00)
					}(),
					b: func() []byte {
						body := []byte{0x00, 0x00, 0x01, 0x00}
						body = append(body, longString("INSERT INTO t (pk) VALUES (?)")...)
						body = append(body, 0x00, 0x01)
						body = append(body, bytesValue([]byte{0x2B})...)
						return append(body, 0x00, 0x01, 0x00)
					}(),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					a := GetFrameHash(frameV4(opBatch, 0x00, tc.a), false)
					b := GetFrameHash(frameV4(opBatch, 0x00, tc.b), false)
					if a == b {
						t.Errorf("batches differing in %s hash the same (%d)", tc.name, a)
					}
				})
			}
		})

		// EmptyCustomPayload pins that an empty custom payload map is
		// skipped like any other.
		//
		// A [bytes map] of zero entries still spends the two bytes that say so. Stopping short
		// of them left the walk reading that count as the batch type, its own second byte as
		// the top half of the statement count, and so on: for the frames below that misreading
		// is entirely plausible — type LOGGED, zero statements, consistency ONE, no flags — so
		// nothing fails a bounds check and nothing falls back to the raw bytes. It just hashes
		// six bytes of the wrong field, identically for every batch that carries a custom
		// payload, and the replayer serves whichever of them it finds first.
		t.Run("EmptyCustomPayload", func(t *testing.T) {
			emptyCustomPayload := func(statement string) []byte {
				body := []byte{0x00, 0x00}      // custom payload: zero entries
				body = append(body, 0x00)       // type: LOGGED
				body = append(body, 0x00, 0x01) // one query
				body = append(body, 0x00)       // kind: query string
				body = append(body, longString(statement)...)
				body = append(body, 0x00, 0x00) // zero values
				body = append(body, 0x00, 0x01) // consistency ONE
				body = append(body, byte(frm.FlagDefaultTimestamp))
				return append(body, timestamp8(111)...)
			}

			a := frameV4(opBatch, frm.FlagCustomPayload, emptyCustomPayload("INSERT INTO t (pk) VALUES (1)"))
			b := frameV4(opBatch, frm.FlagCustomPayload, emptyCustomPayload("INSERT INTO t (pk) VALUES (2)"))

			if GetFrameHash(a, false) == GetFrameHash(b, false) {
				t.Error("batches differing in their statement hash the same: the empty custom payload map was not skipped")
			}
		})

		// IgnoresDefaultTimestamp pins the reason the BATCH arm walks
		// its body at all.
		//
		// writeBatchFrame fills the default-timestamp field with time.Now() unless the caller
		// pinned a value, and the flag defaults to on. Hashing a BATCH whole therefore
		// produced a different hash on every run, so a recorded BATCH could only ever fail to
		// find its response — the replayer panics with "unable to find a response to replay".
		t.Run("IgnoresDefaultTimestamp", func(t *testing.T) {
			a := frameV4(opBatch, 0x00, batchBody(byte(frm.FlagDefaultTimestamp), timestamp8(1)))
			b := frameV4(opBatch, 0x00, batchBody(byte(frm.FlagDefaultTimestamp), timestamp8(999999)))

			if GetFrameHash(a, false) != GetFrameHash(b, false) {
				t.Error("the default timestamp reached a BATCH's hash, so a recorded BATCH cannot match its replay")
			}
		})

		// PreparedStatements pins that a batch of prepared statements —
		// [short bytes] ids rather than query strings — is walked too, and that the ids are
		// what tells two such batches apart.
		t.Run("PreparedStatements", func(t *testing.T) {
			preparedBatch := func(id []byte) []byte {
				body := []byte{0x00, 0x00, 0x01, 0x01} // type, one query, kind: prepared
				body = append(body, byte(len(id)>>8), byte(len(id)))
				body = append(body, id...)
				body = append(body, 0x00, 0x00)                                 // zero values
				return append(body, 0x00, 0x01, byte(frm.FlagDefaultTimestamp)) // consistency + flags
			}

			a := frameV4(opBatch, 0x00, append(preparedBatch([]byte{0xAA, 0xBB}), timestamp8(1)...))
			b := frameV4(opBatch, 0x00, append(preparedBatch([]byte{0xCC, 0xDD}), timestamp8(2)...))
			same := frameV4(opBatch, 0x00, append(preparedBatch([]byte{0xAA, 0xBB}), timestamp8(3)...))

			if GetFrameHash(a, false) == GetFrameHash(b, false) {
				t.Error("batches with different prepared ids hash the same")
			}
			if GetFrameHash(a, false) != GetFrameHash(same, false) {
				t.Error("the same prepared batch hashed differently across two timestamps")
			}
		})

		// V5TailFields pins that a v5 batch's keyspace override and
		// now_in_seconds are covered, and its default timestamp still is not.
		t.Run("V5TailFields", func(t *testing.T) {
			const flags = frm.FlagDefaultTimestamp | frm.FlagWithKeyspace | frm.FlagWithNowInSeconds
			frame := func(keyspace string, now int32, ts int64) []byte {
				return frameV5(opBatch, 0x00, batchBodyV5("INSERT INTO t (pk) VALUES (1)", flags,
					timestamp8(ts), shortString(keyspace), nowInSeconds4(now)))
			}

			base := GetFrameHash(frame("ks_a", 1000, 111), false)

			if GetFrameHash(frame("ks_b", 1000, 111), false) == base {
				t.Error("a v5 batch's keyspace override is not hashed")
			}
			if GetFrameHash(frame("ks_a", 2000, 111), false) == base {
				t.Error("a v5 batch's now_in_seconds is not hashed")
			}
			if GetFrameHash(frame("ks_a", 1000, 999999), false) != base {
				t.Error("a v5 batch's default timestamp reached the hash")
			}
		})
	})

	t.Run("Bounds", func(t *testing.T) {
		// ShortFrame pins the guard for a frame shorter than its own
		// header. Everything after it indexes unconditionally — the stream-id blanking reads
		// frame[2] and frame[3], the switch reads the opcode at frame[4] — so a one-byte frame
		// used to panic before the parser had looked at anything.
		//
		// The fallback here hashes the frame as given rather than with the stream id
		// blanked, because there may be no stream id to blank. That is fine: record and
		// replay run the same guard over the same bytes.
		t.Run("ShortFrame", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				full []byte
			}{
				{"v4", frameV4(opExecute, 0x00, nil)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// A header-only frame is the shortest one the parser accepts, so every
					// prefix of it must divert. Its own length is the first that must not.
					for n := 1; n < len(tc.full); n++ {
						short := append([]byte(nil), tc.full[:n]...)

						got := GetFrameHash(short, false)
						if want := murmur.Murmur3H1(tc.full[:n]); got != want {
							t.Errorf("GetFrameHash(%d-byte frame) = %d, want raw-bytes hash %d", n, got, want)
						}
						if !bytes.Equal(short, tc.full[:n]) {
							t.Errorf("GetFrameHash mutated the %d-byte frame: got % X, want % X", n, short, tc.full[:n])
						}
					}
				})
			}
		})

		// TruncatedBody pins that every variable-length field the
		// parser walks is bounded, not just the two short-bytes length fields covered by
		// TestFrameHash/Execute/BoundsTruncated. A length that reads cleanly but announces
		// more bytes than remain used to move the cursor past the end and reach
		// addQueryParams, which then indexed with no check of its own.
		//
		// loadResponseFramesFromFiles hashes whatever a recording file holds, so each of
		// these has to fall back to the raw bytes rather than panic.
		t.Run("TruncatedBody", func(t *testing.T) {
			// overrunAt rewrites the short at index to a length longer than the frame.
			overrunAt := func(frame []byte, index int) []byte {
				out := append([]byte(nil), frame...)
				out[index], out[index+1] = 0x00, 0xFF
				return out
			}

			for _, tc := range []struct {
				name          string
				frame         []byte
				useMetadataID bool
			}{
				{
					// The preparedID length parses but its payload runs off the end. Without
					// the extension nothing else reads before addQueryParams, so this is the
					// path that reached it with an out-of-range cursor.
					name:  "preparedID payload overruns",
					frame: overrunAt(v4ExecuteFrame(false), 9),
				},
				{
					name:          "resultMetadataID payload overruns",
					frame:         overrunAt(v4ExecuteFrame(true), 14),
					useMetadataID: true,
				},
				{
					// Both short-bytes fields parse; the query flags byte is missing.
					name:          "query params truncated at the flags",
					frame:         v4ExecuteFrame(true)[:20],
					useMetadataID: true,
				},
				{
					// Flags announce bound values, the count parses, the value itself is
					// absent — this one lands in addBytes.
					name: "query params truncated at a bound value",
					frame: frameV4(opExecute, 0x00, []byte{
						0x00, 0x03, 0xAA, 0xBB, 0xCC, // preparedID
						0x00, 0x01, // consistency
						byte(frm.FlagValues), // query flags
						0x00, 0x01,           // one value follows, and then nothing does
					}),
				},
				{
					name:  "query frame truncated at the flags",
					frame: frameV4(opQuery, 0x00, append(longString("SELECT * FROM t"), 0x00, 0x01)),
				},
				{
					// The query text's own [long string] length field is incomplete, so the
					// parameters cannot be located at all.
					name:  "query text length truncated",
					frame: frameV4(opQuery, 0x00, []byte{0x00, 0x00, 0x00}),
				},
				{
					// The length parses but announces more text than the frame holds.
					name:  "query text length overruns",
					frame: frameV4(opQuery, 0x00, append([]byte{0x7F, 0xFF, 0xFF, 0xFF}, "SELECT * FROM t"...)),
				},
				{
					// The custom-payload header flag sends the parser through
					// addCustomPayload, which reads its own lengths out of the body.
					name:  "query custom payload truncated",
					frame: frameV4(opQuery, frm.FlagCustomPayload, []byte{0x00, 0x01}),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					frame := append([]byte(nil), tc.frame...)

					// These fall back from inside the switch, i.e. with the stream id already
					// blanked — see TestFrameHash/Execute/BoundsTruncated.
					want := murmur3OfStreamBlanked(tc.frame)

					if got := GetFrameHash(frame, tc.useMetadataID); got != want {
						t.Errorf("GetFrameHash(%s) = %d, want raw-bytes hash %d", tc.name, got, want)
					}
					if !bytes.Equal(frame, tc.frame) {
						t.Errorf("GetFrameHash mutated the frame: got % X, want % X", frame, tc.frame)
					}
				})
			}

			// A control: bounding the walk must not turn a frame that parses into a
			// fallback. The opQuery branch hashes from the body start through the end of the
			// query parameters, so a QUERY whose flags announce no optional field hashes its
			// whole body — query text included.
			t.Run("well-formed query still parses", func(t *testing.T) {
				frame := frameV4(opQuery, 0x00, queryBody("SELECT * FROM t", 0x00))

				got := GetFrameHash(frame, false)
				if want := murmur.Murmur3H1(frame[9:]); got != want {
					t.Errorf("GetFrameHash(QUERY) = %d, want body hash %d", got, want)
				}
			})
		})
	})

	t.Run("CustomPayload", func(t *testing.T) {
		// Distinguishes pins that a request's custom payload is
		// part of its identity, on each of the three arms that walk a body.
		//
		// The payload is a [bytes map] between the header and the first body field, so an arm
		// has to step over it to find anything of its own. The hashed range used to start where
		// that step landed, which put the payload outside it: two requests alike in everything
		// but their payload hashed the same, and the replayer, which serves the first hash it
		// matches, answered one with the other's response. Query.CustomPayload is public, so
		// that is a pair a caller can send.
		t.Run("Distinguishes", func(t *testing.T) {
			// payload renders a one-entry [bytes map], the shape a CUSTOM_PAYLOAD frame opens
			// with, followed by whatever body the opcode expects.
			payload := func(value byte, body []byte) []byte {
				out := []byte{0x00, 0x01}
				out = append(out, shortString("k")...)
				out = append(out, bytesValue([]byte{value})...)
				return append(out, body...)
			}

			// Each body has to be one the arm can walk to the end of. A body that fails a
			// bound falls back to hashing the frame whole -- payload included -- which would
			// make this test pass without the range covering anything.
			queryTail := queryBody("SELECT * FROM system.local", 0x00)
			executeTail := []byte{
				0x00, 0x03, 0xAA, 0xBB, 0xCC, // preparedID: len 3
				0x00, 0x01, 0x00, // consistency ONE, no flags
			}
			batchTail := batchBody(0x00)

			for _, tc := range []struct {
				name string
				op   frameOp
				tail []byte
			}{
				{name: "query", op: opQuery, tail: queryTail},
				{name: "execute", op: opExecute, tail: executeTail},
				{name: "batch", op: opBatch, tail: batchTail},
			} {
				t.Run(tc.name, func(t *testing.T) {
					a := frameV4(tc.op, frm.FlagCustomPayload, payload(0xAB, tc.tail))
					b := frameV4(tc.op, frm.FlagCustomPayload, payload(0xCD, tc.tail))
					if x, y := GetFrameHash(a, false), GetFrameHash(b, false); x == y {
						t.Errorf("two requests differing only in their custom payload hash the same (%d)", x)
					}

					// And the payload's presence has to register at all: the same body with
					// no map in front of it is a different request.
					bare := frameV4(tc.op, 0x00, tc.tail)
					if x, y := GetFrameHash(a, false), GetFrameHash(bare, false); x == y {
						t.Errorf("a request with a custom payload hashes as one without (%d)", x)
					}
				})
			}
		})

		// OrderIndependent pins that a custom payload's identity
		// does not depend on the byte order its entries were encoded in.
		//
		// writeBytesMap (frame.go) ranges directly over a Go map to encode a CUSTOM_PAYLOAD
		// [bytes map], so the wire bytes for the same logical entries land in a different
		// order on every call once the payload holds two or more of them. addCustomPayload
		// sorts each entry's raw bytes before folding them together specifically so this does
		// not matter -- a single entry, which is all TestFrameHash/CustomPayload/Distinguishes
		// above ever builds, cannot exercise that: a one-entry sort has nothing to reorder.
		t.Run("OrderIndependent", func(t *testing.T) {
			entries := [][2]string{
				{"k1", "\xAA"},
				{"k2", "\xBB"},
				{"k3", "\xCC"},
			}

			// payload renders the entries at the given indices, in that order, as a [bytes
			// map], followed by whatever body the opcode expects.
			payload := func(order []int, body []byte) []byte {
				out := []byte{0x00, byte(len(order))}
				for _, i := range order {
					out = append(out, shortString(entries[i][0])...)
					out = append(out, bytesValue([]byte(entries[i][1]))...)
				}
				return append(out, body...)
			}

			// Each body has to be one the arm can walk to the end of, for the same reason
			// TestFrameHash/CustomPayload/Distinguishes's do.
			queryTail := queryBody("SELECT * FROM system.local", 0x00)
			executeTail := []byte{
				0x00, 0x03, 0xAA, 0xBB, 0xCC, // preparedID: len 3
				0x00, 0x01, 0x00, // consistency ONE, no flags
			}
			batchTail := batchBody(0x00)

			for _, tc := range []struct {
				name string
				op   frameOp
				tail []byte
			}{
				{name: "query", op: opQuery, tail: queryTail},
				{name: "execute", op: opExecute, tail: executeTail},
				{name: "batch", op: opBatch, tail: batchTail},
			} {
				t.Run(tc.name, func(t *testing.T) {
					forward := frameV4(tc.op, frm.FlagCustomPayload, payload([]int{0, 1, 2}, tc.tail))
					reversed := frameV4(tc.op, frm.FlagCustomPayload, payload([]int{2, 1, 0}, tc.tail))

					x, y := GetFrameHash(forward, false), GetFrameHash(reversed, false)
					if x != y {
						t.Errorf("the same payload encoded in two different orders hashes differently: %d != %d", x, y)
					}

					// Neither hash may be the whole-frame fallback -- that would make the
					// equality above vacuous, since a truncated or unwalkable frame always
					// falls back to the same murmur.Murmur3H1(frame) regardless of order.
					if raw := murmur3OfStreamBlanked(forward); x == raw {
						t.Fatalf("GetFrameHash hit the whole-frame fallback (%d); the body did not fully bounds-check", raw)
					}

					// And a genuinely different payload -- not just a reordering -- still has
					// to hash differently, so this test cannot be satisfied by a
					// canonicalization that collapses every payload to the same value.
					missingEntry := frameV4(tc.op, frm.FlagCustomPayload, payload([]int{0, 1}, tc.tail))
					if z := GetFrameHash(missingEntry, false); z == x {
						t.Errorf("a payload missing an entry hashes the same as the full one (%d)", z)
					}
				})
			}
		})
	})

	t.Run("Execute", func(t *testing.T) {
		// BoundsTruncated pins that an EXECUTE frame truncated
		// across one of the short-bytes length fields falls back to hashing the raw bytes
		// rather than indexing past the end. The resultMetadataID skip is live on protocol
		// v4 now, and loadResponseFramesFromFiles feeds GetFrameHash bytes straight out of a
		// recording file, so a damaged record must not panic the replayer.
		//
		// This covers the two length fields in the opExecute branch; the payloads they
		// announce and everything addQueryParams walks are covered by
		// TestFrameHash/Bounds/TruncatedBody.
		t.Run("BoundsTruncated", func(t *testing.T) {
			full := v4ExecuteFrame(true)

			for _, tc := range []struct {
				name          string
				frame         []byte
				useMetadataID bool
			}{
				{
					// Ends inside the preparedID length field.
					name:          "truncated at the preparedID length",
					frame:         full[:10],
					useMetadataID: true,
				},
				{
					// preparedID parses whole — length and payload — and then the metadata-id
					// length field runs off the end. full[:11] would not get this far: it
					// stops at the preparedID payload, which is TestFrameHash/Bounds/TruncatedBody's
					// case, not this one.
					name:          "truncated at the metadata id length",
					frame:         full[:15],
					useMetadataID: true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					frame := append([]byte(nil), tc.frame...)

					// The fallback hashes the frame as the parser left it, i.e. with the stream
					// id blanked — which is what makes the hash stable between record and replay,
					// where the stream ids differ. Mirror that here.
					want := murmur3OfStreamBlanked(tc.frame)

					got := GetFrameHash(frame, tc.useMetadataID)
					if got != want {
						t.Errorf("GetFrameHash(%s) = %d, want raw-bytes hash %d", tc.name, got, want)
					}
					// The parser must also have left the caller's frame as it found it.
					if !bytes.Equal(frame, tc.frame) {
						t.Errorf("GetFrameHash mutated the frame: got % X, want % X", frame, tc.frame)
					}
				})
			}
		})

		// V4MetadataID verifies that on protocol v4 the caller-supplied
		// useMetadataID flag makes GetFrameHash skip the EXECUTE resultMetadataID field,
		// so the extracted query-params range ends exactly at the frame boundary. Without
		// the skip the parser would mis-read the metadata-id bytes as query params (and
		// can index past the frame), which is the bug SCYLLA_USE_METADATA_ID introduces
		// on v4.
		t.Run("V4MetadataID", func(t *testing.T) {
			frame := v4ExecuteFrame(true)
			// GetFrameHash hashes frame[bodyStart:endIndex]; a correct skip lands endIndex
			// on len(frame), so the hashed range is the whole body.
			const bodyStart = 9
			want := murmur.Murmur3H1(frame[bodyStart:])
			if got := GetFrameHash(frame, true); got != want {
				t.Fatalf("GetFrameHash(v4+ext EXECUTE, true) = %d, want %d (resultMetadataID not skipped correctly)", got, want)
			}

			// The v4-without-extension path must still parse an EXECUTE that carries no
			// resultMetadataID field.
			noExt := v4ExecuteFrame(false)
			wantNoExt := murmur.Murmur3H1(noExt[bodyStart:])
			if got := GetFrameHash(noExt, false); got != wantNoExt {
				t.Fatalf("GetFrameHash(v4 EXECUTE, false) = %d, want %d", got, wantNoExt)
			}
		})

		// V5BoundsTailFields walks every truncation of a v5 EXECUTE whose
		// parameters end in the tail fields. Each length there is file-supplied — a recording
		// is JSON on disk — so a truncated one must fall back to hashing the frame rather
		// than index past it.
		t.Run("V5BoundsTailFields", func(t *testing.T) {
			full := v5ExecuteFrame(frm.FlagDefaultTimestamp|frm.FlagWithKeyspace|frm.FlagWithNowInSeconds,
				timestamp8(111), shortString("ks"), nowInSeconds4(7))

			for n := 0; n < len(full); n++ {
				truncated := append([]byte(nil), full[:n]...)
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("GetFrameHash panicked on a %d-byte prefix: %v", n, r)
						}
					}()
					GetFrameHash(truncated, false)
				}()
			}

			// An overstated keyspace length must not be walked either.
			bad := v5ExecuteFrame(frm.FlagWithKeyspace, []byte{0x7F, 0xFF, 'k', 's'})
			if got, want := GetFrameHash(bad, false), murmur3OfStreamBlanked(bad); got != want {
				t.Errorf("overstated keyspace length: GetFrameHash = %d, want the raw-bytes fallback %d", got, want)
			}
		})

		// V5DistinguishesTailFields pins that the protocol v5 keyspace
		// override and now_in_seconds are part of a request's identity.
		//
		// Both sit after the default timestamp in the parameter block, so a walk that stops
		// at serial consistency leaves them out and two requests differing only there hash
		// the same. The replayer picks the first hash match, so that is not a near miss: it
		// serves the response recorded for the other keyspace.
		t.Run("V5DistinguishesTailFields", func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				flags uint32
				a, b  [][]byte
			}{
				{
					name:  "keyspace override",
					flags: frm.FlagWithKeyspace,
					a:     [][]byte{shortString("ks_a")},
					b:     [][]byte{shortString("ks_b")},
				},
				{
					name:  "now_in_seconds",
					flags: frm.FlagWithNowInSeconds,
					a:     [][]byte{nowInSeconds4(1000)},
					b:     [][]byte{nowInSeconds4(2000)},
				},
				{
					// The keyspace sits behind the timestamp, so this only differs from the
					// first case if the walk steps over the timestamp rather than stopping.
					name:  "keyspace override behind a default timestamp",
					flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace,
					a:     [][]byte{timestamp8(111), shortString("ks_a")},
					b:     [][]byte{timestamp8(111), shortString("ks_b")},
				},
				{
					name:  "both, behind a default timestamp",
					flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace | frm.FlagWithNowInSeconds,
					a:     [][]byte{timestamp8(111), shortString("ks"), nowInSeconds4(1000)},
					b:     [][]byte{timestamp8(111), shortString("ks"), nowInSeconds4(2000)},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					a := GetFrameHash(v5ExecuteFrame(tc.flags, tc.a...), false)
					b := GetFrameHash(v5ExecuteFrame(tc.flags, tc.b...), false)
					if a == b {
						t.Errorf("frames differing in %s hash the same (%d); the replayer would serve the wrong response", tc.name, a)
					}
				})
			}
		})

		// V5IgnoresDefaultTimestamp pins the other half of the contract: the
		// default timestamp must stay out of the hash even though the fields behind it are in
		// it. It is time.Now() at send time (framer.writeQueryParams), so hashing it would
		// make every recorded frame fail to match its own replay.
		t.Run("V5IgnoresDefaultTimestamp", func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				flags uint32
				a, b  [][]byte
			}{
				{
					name:  "timestamp alone",
					flags: frm.FlagDefaultTimestamp,
					a:     [][]byte{timestamp8(1)},
					b:     [][]byte{timestamp8(999999)},
				},
				{
					name:  "timestamp ahead of a keyspace override",
					flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace,
					a:     [][]byte{timestamp8(1), shortString("ks")},
					b:     [][]byte{timestamp8(999999), shortString("ks")},
				},
				{
					name:  "timestamp ahead of now_in_seconds",
					flags: frm.FlagDefaultTimestamp | frm.FlagWithNowInSeconds,
					a:     [][]byte{timestamp8(1), nowInSeconds4(7)},
					b:     [][]byte{timestamp8(999999), nowInSeconds4(7)},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					a := GetFrameHash(v5ExecuteFrame(tc.flags, tc.a...), false)
					b := GetFrameHash(v5ExecuteFrame(tc.flags, tc.b...), false)
					if a != b {
						t.Errorf("the default timestamp reached the hash: %d != %d", a, b)
					}
				})
			}
		})
	})

	// Fits pins that fits does not compute index+n. A
	// [bytes] field can encode a length up to MaxInt32, which overflows a 32-bit int
	// once added to any index; the negative sum then satisfies every upper bound and
	// the parser walks on with a cursor outside the frame. Reachable on 32-bit targets
	// only, but the checks below overflow on any word size.
	t.Run("Fits", func(t *testing.T) {
		frame := make([]byte, 8)

		if fits(frame, 1, math.MaxInt) {
			t.Error("fits(len 8, index 1, n MaxInt) = true, want false")
		}
		if fits(frame, math.MaxInt, 1) {
			t.Error("fits(len 8, index MaxInt, n 1) = true, want false")
		}
		// The boundary either side, so the fix cannot have been an off-by-one.
		if !fits(frame, 8, 0) {
			t.Error("fits(len 8, index 8, n 0) = false, want true")
		}
		if fits(frame, 8, 1) {
			t.Error("fits(len 8, index 8, n 1) = true, want false")
		}
	})

	// FrameIsProtoV5OrNewer pins that the version comparison masks off the
	// direction bit (a v5 response byte is 0x85) and that short input is not a
	// match.
	t.Run("FrameIsProtoV5OrNewer", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			b    []byte
			want bool
		}{
			{"v5 request", []byte{0x05, 0x00}, true},
			{"v5 response", []byte{0x85}, true},
			{"v4 request", []byte{0x04}, false},
			{"v4 response", []byte{0x84}, false},
			{"empty", nil, false},
		} {
			if got := FrameIsProtoV5OrNewer(tc.b); got != tc.want {
				t.Errorf("%s: FrameIsProtoV5OrNewer(% X) = %v, want %v", tc.name, tc.b, got, tc.want)
			}
		}
	})

	// HashLock is the load-bearing regression lock described at the top of
	// utils_hashlock_test.go.
	t.Run("HashLock", func(t *testing.T) {
		for _, tc := range hashLockCases() {
			want, ok := hashLockWant[tc.name]
			if !ok {
				t.Errorf("%s: no pinned hash — add one rather than deleting the case", tc.name)
				continue
			}
			if got := GetFrameHash(tc.frame, tc.useMetadataID); got != want {
				t.Errorf("%s: GetFrameHash = %d, pinned %d", tc.name, got, want)
			}
		}

		if len(hashLockWant) != len(hashLockCases()) {
			t.Errorf("pinned %d hashes for %d cases", len(hashLockWant), len(hashLockCases()))
		}
	})

	t.Run("Header", func(t *testing.T) {
		// Empty verifies the empty-input guard.
		t.Run("Empty", func(t *testing.T) {
			if got := GetFrameHash(nil, false); got != murmur.Murmur3H1(nil) {
				t.Fatalf("GetFrameHash(nil) = %d, want %d", got, murmur.Murmur3H1(nil))
			}
		})

		// IgnoresDirectionBit pins that every protocol-version
		// comparison in GetFrameHash masks off the direction bit before comparing.
		//
		// The version tests used to be a mix of masked and unmasked. The unmasked ones
		// read 0x84 (v4 with the direction bit set) as "greater than v4", i.e. as v5, so
		// they took the v5-only branches: the EXECUTE parser skipped a resultMetadataID
		// that is not on the wire (useMetadataID being false here) and addQueryParams
		// read a 4-byte query-flags field instead of a 1-byte one. Both walk the offsets
		// off the end of the body, so before the fix the 0x84 case panicked instead of
		// producing the v4 hash.
		t.Run("IgnoresDirectionBit", func(t *testing.T) {
			plain := v4ExecuteFrame(false)
			directionBitSet := v4ExecuteFrame(false)
			directionBitSet[0] |= 0x80

			got := GetFrameHash(plain, false)
			withBit := GetFrameHash(directionBitSet, false)

			if got != withBit {
				t.Errorf("direction bit changed the hash: 0x04 -> %d, 0x84 -> %d", got, withBit)
			}

			// Also confirm the frame was really parsed, rather than diverted to the
			// raw-bytes fallback: the hash must cover the query-params range.
			want := murmur.Murmur3H1(plain[9:])
			if got != want {
				t.Errorf("GetFrameHash(v4 EXECUTE) = %d, want body hash %d", got, want)
			}
			if raw := murmur.Murmur3H1(plain); got == raw {
				t.Error("GetFrameHash fell back to hashing the raw frame")
			}
		})

		// ParsesProtoV5 pins that a protocol v5 request frame is
		// parsed rather than diverted to a whole-frame hash.
		//
		// It used to be diverted, by a guard that treated any frame[0] whose low 7 bits were
		// >= 5 as transport-segment data. That guard was unsound in both directions: a v5
		// segment whose length low-byte is < 5 slipped past it into the CQL parser anyway,
		// and a genuine v5 frame — which the driver does produce, unsegmented, throughout the
		// handshake — was hashed whole, default timestamp included, so it could never match
		// its own replay. Telling the two apart needs protocol context the bytes do not
		// carry, so it is the caller's job to pass a bare frame; see GetFrameHash's contract.
		t.Run("ParsesProtoV5", func(t *testing.T) {
			// A v5 EXECUTE with a keyspace override. Parsed, the hashed range stops before
			// the frame's end; diverted, the hash is the whole frame.
			frame := v5ExecuteFrame(frm.FlagWithKeyspace, shortString("ks"))

			if got, whole := GetFrameHash(frame, false), murmur3OfStreamBlanked(frame); got == whole {
				t.Error("v5 frame was hashed whole rather than parsed")
			}
			if got, asGiven := GetFrameHash(frame, false), murmur.Murmur3H1(frame); got == asGiven {
				t.Error("v5 frame fell through to a guard that hashes it as given")
			}
		})

		// ShortProtoV5Input pins that v5 input too short to hold a header
		// still reaches the short-frame guard, which hashes it as given.
		t.Run("ShortProtoV5Input", func(t *testing.T) {
			short := []byte{0x05, 0x00, 0x11, 0x22}

			if got, want := GetFrameHash(short, false), murmur.Murmur3H1(short); got != want {
				t.Fatalf("GetFrameHash(short v5) = %d, want %d", got, want)
			}
		})
	})

	// These tests pin what a QUERY frame's hash has to stand for, which is the
	// contract scylladb/gocql#1000 broke.
	//
	// A QUERY body is the query text as a [long string] followed by the query parameters.
	// The parameter walk used to be handed the body start rather than the position past
	// the text, so it read the text's 4-byte length as if it were parameters: the first
	// two bytes as the consistency and the third as the flags. Those are 0x00 0x00 0x00
	// for any query under 16 MiB, so the walk stopped immediately and every QUERY frame
	// hashed the same three zero bytes — the text and the bound values were never in the
	// hash at all.
	//
	// That was not a near miss. The replayer scans for the first matching hash, and back
	// then over a slice built by iterating a map, so a recording holding two distinct
	// queries served an arbitrary one of their responses, chosen differently from run to
	// run, with nothing failing. The slice is sorted now, which only makes such a
	// collision reproducibly wrong instead of randomly wrong.
	t.Run("Query", func(t *testing.T) {
		// BoundsEveryPrefix walks every truncation of a fully-populated
		// QUERY. Every length the walk reads is file-supplied — a recording is JSON on disk —
		// so a truncated one has to fall back to hashing the frame rather than index past it.
		t.Run("BoundsEveryPrefix", func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				frame []byte
			}{
				{
					name: "v4",
					frame: frameV4(opQuery, 0x00, queryBody("SELECT v FROM t WHERE pk = ?",
						byte(frm.FlagValues|frm.FlagPageSize|frm.FlagWithSerialConsistency|frm.FlagDefaultTimestamp),
						[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}), []byte{0x00, 0x00, 0x13, 0x88},
						[]byte{0x00, 0x09}, timestamp8(111))),
				},
				{
					name: "v5",
					frame: v5QueryFrame("SELECT v FROM t WHERE pk = ?",
						frm.FlagValues|frm.FlagPageSize|frm.FlagWithSerialConsistency|
							frm.FlagDefaultTimestamp|frm.FlagWithKeyspace|frm.FlagWithNowInSeconds,
						[]byte{0x00, 0x01}, bytesValue([]byte{0x2A}), []byte{0x00, 0x00, 0x13, 0x88},
						[]byte{0x00, 0x09}, timestamp8(111), shortString("ks"), nowInSeconds4(7)),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for n := 0; n < len(tc.frame); n++ {
						truncated := append([]byte(nil), tc.frame[:n]...)
						func() {
							defer func() {
								if r := recover(); r != nil {
									t.Fatalf("GetFrameHash panicked on a %d-byte prefix: %v", n, r)
								}
							}()
							GetFrameHash(truncated, false)
						}()
					}
				})
			}

			// A length that overstates the space left must not be walked either, the case a
			// truncation cannot reach: the frame is whole and only the field lies.
			bad := v5QueryFrame("SELECT v FROM t", frm.FlagWithKeyspace, []byte{0x7F, 0xFF, 'k', 's'})
			if got, want := GetFrameHash(bad, false), murmur3OfStreamBlanked(bad); got != want {
				t.Errorf("overstated keyspace length: GetFrameHash = %d, want the raw-bytes fallback %d", got, want)
			}
		})

		// DistinguishesText pins that the query text is part of a
		// QUERY's identity, on every protocol version.
		t.Run("DistinguishesText", func(t *testing.T) {
			const (
				a = "SELECT * FROM system.local WHERE key='local'"
				b = "SELECT broadcast_address, cluster_name FROM system.local WHERE key='local'"
			)

			for _, tc := range []struct {
				name string
				a, b []byte
			}{
				{
					name: "v4",
					a:    frameV4(opQuery, 0x00, queryBody(a, 0x00)),
					b:    frameV4(opQuery, 0x00, queryBody(b, 0x00)),
				},
				{
					// The pair the checked-in recordings actually turn on: a page size and a
					// default timestamp, as querySystem sends.
					name: "v4 with a page size and a default timestamp",
					a: frameV4(opQuery, 0x00, queryBody(a, byte(frm.FlagPageSize|frm.FlagDefaultTimestamp),
						[]byte{0x00, 0x00, 0x13, 0x88}, timestamp8(111))),
					b: frameV4(opQuery, 0x00, queryBody(b, byte(frm.FlagPageSize|frm.FlagDefaultTimestamp),
						[]byte{0x00, 0x00, 0x13, 0x88}, timestamp8(111))),
				},
				{
					name: "v5",
					a:    v5QueryFrame(a, 0),
					b:    v5QueryFrame(b, 0),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if x, y := GetFrameHash(tc.a, false), GetFrameHash(tc.b, false); x == y {
						t.Errorf("two queries differing only in their text hash the same (%d); the replayer would serve the wrong response", x)
					}
				})
			}
		})

		// DistinguishesValues pins the other half of a QUERY's identity.
		// An unprepared statement with bound values is one the driver does send — a batch's
		// simple statements and anything with skipPrepare set — and two of them differing only
		// in a value are two different requests.
		t.Run("DistinguishesValues", func(t *testing.T) {
			const query = "SELECT v FROM t WHERE pk = ?"

			v4 := func(value byte) []byte {
				return frameV4(opQuery, 0x00, queryBody(query, byte(frm.FlagValues),
					[]byte{0x00, 0x01}, bytesValue([]byte{value})))
			}
			v5 := func(value byte) []byte {
				return v5QueryFrame(query, frm.FlagValues, []byte{0x00, 0x01}, bytesValue([]byte{value}))
			}

			for _, tc := range []struct {
				name string
				a, b []byte
			}{
				{name: "v4", a: v4(0x2A), b: v4(0x2B)},
				{name: "v5", a: v5(0x2A), b: v5(0x2B)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if x, y := GetFrameHash(tc.a, false), GetFrameHash(tc.b, false); x == y {
						t.Errorf("two queries differing only in a bound value hash the same (%d)", x)
					}
				})
			}
		})

		// IgnoresDefaultTimestamp pins the requirement that pulls the
		// other way. writeQueryParams fills the default timestamp with time.Now() at send
		// time, so a hash covering it differs on every run and a recorded QUERY can never
		// match its own replay — which is what happened to every v5 QUERY while the walk
		// failed its bound and fell back to hashing the frame whole.
		t.Run("IgnoresDefaultTimestamp", func(t *testing.T) {
			const query = "SELECT * FROM system.local WHERE key='local'"

			for _, tc := range []struct {
				name string
				a, b []byte
			}{
				{
					name: "v4",
					a:    frameV4(opQuery, 0x00, queryBody(query, byte(frm.FlagDefaultTimestamp), timestamp8(1))),
					b:    frameV4(opQuery, 0x00, queryBody(query, byte(frm.FlagDefaultTimestamp), timestamp8(999999))),
				},
				{
					name: "v5",
					a:    v5QueryFrame(query, frm.FlagDefaultTimestamp, timestamp8(1)),
					b:    v5QueryFrame(query, frm.FlagDefaultTimestamp, timestamp8(999999)),
				},
				{
					// On v5 the keyspace override sits behind the timestamp, so this only
					// holds if the walk steps over the timestamp rather than stopping at it.
					name: "v5 with a keyspace override behind it",
					a:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithKeyspace, timestamp8(1), shortString("ks")),
					b:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithKeyspace, timestamp8(999999), shortString("ks")),
				},
				{
					// And the same for now_in_seconds, the other field v5 puts behind it.
					name: "v5 with now_in_seconds behind it",
					a:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithNowInSeconds, timestamp8(1), nowInSeconds4(7)),
					b:    v5QueryFrame(query, frm.FlagDefaultTimestamp|frm.FlagWithNowInSeconds, timestamp8(999999), nowInSeconds4(7)),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if x, y := GetFrameHash(tc.a, false), GetFrameHash(tc.b, false); x != y {
						t.Errorf("the default timestamp reached a QUERY's hash: %d != %d", x, y)
					}
				})
			}
		})

		// V5DistinguishesTailFields pins that the two fields protocol v5
		// adds behind the default timestamp are part of a QUERY's identity, the same way
		// TestFrameHash/Execute/V5DistinguishesTailFields pins it for an EXECUTE. A walk that stops
		// at serial consistency leaves them out, and the replayer would serve the response
		// recorded for the other keyspace.
		t.Run("V5DistinguishesTailFields", func(t *testing.T) {
			const query = "SELECT * FROM t"

			for _, tc := range []struct {
				name  string
				flags uint32
				a, b  [][]byte
			}{
				{
					name:  "keyspace override",
					flags: frm.FlagWithKeyspace,
					a:     [][]byte{shortString("ks_a")},
					b:     [][]byte{shortString("ks_b")},
				},
				{
					name:  "now_in_seconds",
					flags: frm.FlagWithNowInSeconds,
					a:     [][]byte{nowInSeconds4(1000)},
					b:     [][]byte{nowInSeconds4(2000)},
				},
				{
					// The keyspace sits behind the timestamp, so this only differs from the
					// first case if the walk steps over the timestamp rather than stopping.
					name:  "keyspace override behind a default timestamp",
					flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace,
					a:     [][]byte{timestamp8(111), shortString("ks_a")},
					b:     [][]byte{timestamp8(111), shortString("ks_b")},
				},
				{
					name:  "both, behind a default timestamp",
					flags: frm.FlagDefaultTimestamp | frm.FlagWithKeyspace | frm.FlagWithNowInSeconds,
					a:     [][]byte{timestamp8(111), shortString("ks"), nowInSeconds4(1000)},
					b:     [][]byte{timestamp8(111), shortString("ks"), nowInSeconds4(2000)},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					a := GetFrameHash(v5QueryFrame(query, tc.flags, tc.a...), false)
					b := GetFrameHash(v5QueryFrame(query, tc.flags, tc.b...), false)
					if a == b {
						t.Errorf("v5 queries differing in %s hash the same (%d)", tc.name, a)
					}
				})
			}
		})
	})

	// Startup pins that the STARTUP hash does not
	// depend on the body, directly or through the length field in the header.
	//
	// The checked-in replay recordings in tests/bench/e2e store raw frames and are
	// rehashed at load time, so a body-dependent STARTUP hash silently couples them
	// to the exact set of STARTUP options the driver sent when they were recorded.
	// Adding one shifts the length field, no recorded frame matches any more, and
	// the replay benchmarks panic with "unable to find a response to replay" until
	// every recording is regenerated.
	t.Run("Startup", func(t *testing.T) {
		// useMetadataID is irrelevant to a STARTUP frame — it only moves the EXECUTE
		// query-params offset — so pass false and keep this about the body length.
		short := GetFrameHash(startupFrameV4([]byte{0x00, 0x01}), false)
		long := GetFrameHash(startupFrameV4(make([]byte, 512)), false)

		if short != long {
			t.Errorf("STARTUP hash depends on the body length: %d != %d", short, long)
		}

		// Confirm the frame was parsed rather than diverted to the raw-bytes
		// fallback, which would also make the two hashes differ if it regressed.
		if raw := murmur.Murmur3H1(startupFrameV4([]byte{0x00, 0x01})); short == raw {
			t.Error("GetFrameHash fell back to hashing the raw frame")
		}
	})

	// StartupNegotiatesMetadataID verifies detection of the SCYLLA_USE_METADATA_ID
	// opt-in in a STARTUP request, that the opcode guard rejects other frames, and
	// that only map *keys* count — gocql's startupOptions puts caller-supplied
	// DRIVER_NAME/DRIVER_VERSION/ApplicationInfo values into the same map, so a
	// substring scan would let a caller latch this by naming their application after
	// the extension.
	t.Run("StartupNegotiatesMetadataID", func(t *testing.T) {
		// The driver serializes the extension as a key mapping to the empty string,
		// alongside the mandatory options.
		optIn := stringMap(
			[2]string{"CQL_VERSION", "3.0.0"},
			[2]string{scyllaUseMetadataIDKey, ""},
		)
		noOptIn := stringMap(
			[2]string{"CQL_VERSION", "3.0.0"},
			[2]string{"COMPRESSION", "lz4"},
		)

		if !StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, optIn)) {
			t.Error("StartupNegotiatesMetadataID(STARTUP with key) = false, want true")
		}
		if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, noOptIn)) {
			t.Error("StartupNegotiatesMetadataID(STARTUP without key) = true, want false")
		}
		// Same key bytes, but not a STARTUP opcode — must not match.
		if StartupNegotiatesMetadataID(frameV4(opQuery, 0x00, optIn)) {
			t.Error("StartupNegotiatesMetadataID(QUERY with key) = true, want false")
		}
		// Below protocol v3 the opcode is a byte earlier, so index 4 holds a length byte
		// and no v2 frame is a STARTUP as far as this is concerned. Not a gap: a
		// connection that old is refused by FrameSplitter.consume before a STARTUP can be
		// recorded, and reporting "no opt-in" for one is the safe way to be wrong.
		if StartupNegotiatesMetadataID(frameV2(opStartup, optIn)) {
			t.Error("StartupNegotiatesMetadataID(v2 STARTUP with key) = true, want false")
		}
		if StartupNegotiatesMetadataID(nil) {
			t.Error("StartupNegotiatesMetadataID(nil) = true, want false")
		}

		// A caller-supplied value carrying the key literal must not latch it.
		valueOnly := stringMap(
			[2]string{"CQL_VERSION", "3.0.0"},
			[2]string{"APPLICATION_NAME", "we love " + scyllaUseMetadataIDKey},
			[2]string{"DRIVER_NAME", scyllaUseMetadataIDKey},
		)
		if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, valueOnly)) {
			t.Error("StartupNegotiatesMetadataID(STARTUP with key only in values) = true, want false")
		}

		// A map truncated mid-entry reads as "not negotiated" rather than indexing past
		// the end: the recorder feeds this whatever bytes a connection produced, and a
		// read that straddles two frames still records the second one from its middle.
		for n := len(frameV4(opStartup, 0x00, optIn)) - 1; n > 4; n-- {
			if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, optIn)[:n]) {
				t.Errorf("StartupNegotiatesMetadataID(STARTUP truncated to %d bytes) = true, want false", n)
			}
		}
		// A count claiming more entries than the body holds.
		overCount := append([]byte{0x00, 0x09}, stringMap([2]string{"CQL_VERSION", "3.0.0"})[2:]...)
		if StartupNegotiatesMetadataID(frameV4(opStartup, 0x00, overCount)) {
			t.Error("StartupNegotiatesMetadataID(STARTUP with an overstated map count) = true, want false")
		}

		// The recorder's read-side FrameWriter calls this on every completed response
		// frame, so a SUPPORTED response advertising the key must not latch the flag.
		supported := frameV4(opSupported, 0x00, optIn)
		supported[0] |= 0x80 // response direction bit
		if StartupNegotiatesMetadataID(supported) {
			t.Error("StartupNegotiatesMetadataID(SUPPORTED response with key) = true, want false")
		}
	})

	// Tracing pins that a request's tracing flag is part of its
	// identity, on each of the three arms that walk a body.
	//
	// framer.trace sets FlagTracing in the header and changes nothing else -- the tracing id
	// comes back on the response -- so a traced request and an untraced one are byte-identical
	// from the body onwards. No range of the body can tell them apart, which is what makes
	// this different from the custom payload: that one is body bytes, so a hash of them can be
	// folded in. The replayer serves the first hash it matches, so without the flags byte in
	// the hash it answers one with the other's response. Query.Trace, Batch.Trace and
	// Session.SetTrace are all public, so this is a pair a caller can send.
	t.Run("Tracing", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			op   frameOp
			tail []byte
		}{
			{name: "query", op: opQuery, tail: queryBody("SELECT * FROM system.local", 0x00)},
			{name: "execute", op: opExecute, tail: v4ExecuteFrame(false)[FrameHeaderLen:]},
			{name: "batch", op: opBatch, tail: batchBody(0x00)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				traced := frameV4(tc.op, frm.FlagTracing, tc.tail)
				plain := frameV4(tc.op, 0x00, tc.tail)
				if x, y := GetFrameHash(traced, false), GetFrameHash(plain, false); x == y {
					t.Errorf("a traced and an untraced request hash the same (%d); the replayer would serve the wrong response", x)
				}

				// The fold has to distinguish which flag, not merely that one is set.
				// FlagCompress never reaches a recorded frame, but it is the nearest
				// neighbour in the byte and the cheapest way to say so.
				if x, y := GetFrameHash(traced, false), GetFrameHash(frameV4(tc.op, frm.FlagCompress, tc.tail), false); x == y {
					t.Errorf("two requests differing only in which header flag is set hash the same (%d)", x)
				}
			})
		}
	})
}
