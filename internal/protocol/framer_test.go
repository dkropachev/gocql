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
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	lz4mod "github.com/scylladb/gocql/v2/internal/wirecompress"
)

func TestFramer(t *testing.T) {
	t.Run("DefaultFramerFlags", func(t *testing.T) {
		comp := testMockedCompressor{}

		cases := []struct {
			name       string
			compressor Compressor
			version    byte
			want       byte
		}{
			{"v4 no compressor", nil, protoVersion4, 0},
			{"v4 with compressor", comp, protoVersion4, frm.FlagCompress},
			{"v5 no compressor", nil, protoVersion5, 0},
			// v5 compresses at the segment layer, so no frame-header FlagCompress.
			{"v5 with compressor", comp, protoVersion5, 0},
			// A direction/reserved high bit on the version must not defeat the v5
			// check and re-enable FlagCompress at v5, nor suppress it at v4.
			{"v5|dir with compressor", comp, protoVersion5 | protoDirectionMask, 0},
			{"v4|dir with compressor", comp, protoVersion4 | protoDirectionMask, frm.FlagCompress},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if got := defaultFramerFlags(tc.compressor, tc.version); got != tc.want {
					t.Fatalf("defaultFramerFlags(%v, v%d) = 0x%02x, want 0x%02x", tc.compressor != nil, tc.version, got, tc.want)
				}
			})
		}
	})
	t.Run("Finish", func(t *testing.T) {
		t.Parallel()

		t.Run("RejectsTooLong", func(t *testing.T) {
			t.Parallel()

			if os.Getenv("TRAVIS") == "true" {
				t.Skip("skipping test in travis due to memory pressure with the race detecor")
			}

			framer := newFramer(nil, 3)

			framer.WriteHeader(0, frm.OpStartup, 1)
			framer.WriteBytes(make([]byte, frm.MaxFrameSize+1))
			err := framer.Finish()
			if err != ErrFrameTooBig {
				t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
			}
		})
		// Covers the asymmetry between the
		// two size checks in finish(): the one at the top measures the uncompressed body, but
		// the length written into the header is the compressed one.
		//
		// Snappy grows an incompressible body, so a payload that passes the first check can
		// exceed the limit after Encode. Left unchecked the frame goes out declaring a length
		// that both this driver's reader and the server refuse, turning a local ErrFrameTooBig
		// into a remote rejection or a killed connection.
		t.Run("V4CompressionRespectsFrameLimit", func(t *testing.T) {
			t.Parallel()

			f := newFramer(inflatingCompressor{}, protoVersion4)
			require.NotZero(t, f.Flags&frm.FlagCompress)

			f.WriteHeader(f.Flags, frm.OpQuery, 1)
			f.Buf = append(f.Buf, []byte("a body well inside the limit before compression")...)

			// The body above passes the pre-compression check; only the compressed size is
			// over, so reaching ErrFrameTooBig proves the second check is the one that fired.
			require.ErrorIs(t, f.Finish(), ErrFrameTooBig)
		})
		// This is the uncompressed control for
		// TestFramer/Finish/V4SnappyRoundTrip: same framer, same payload, no compressor. It pins that
		// the compression there is driven by the compressor and not by something incidental to
		// finish().
		t.Run("V4NoCompressorLeavesBody", func(t *testing.T) {
			t.Parallel()

			payload := bytes.Repeat([]byte("a"), 1024)

			w := newFramer(nil, protoVersion4)
			require.Zero(t, w.Flags&frm.FlagCompress)

			w.WriteHeader(w.Flags, frm.OpQuery, 1)
			w.Buf = append(w.Buf, payload...)
			require.NoError(t, w.Finish())

			require.Zero(t, w.Buf[1]&frm.FlagCompress)
			require.Equal(t, payload, w.Buf[headSize:])
		})
		// Drives a real body through the protocol v4 compression
		// path and back: framer.finish() compresses the body and rewrites the header length,
		// framer.readFrame() reads that length and decompresses it again.
		//
		// This is the default configuration -- discoverProtocol caps at protoVersion4, so a
		// newClusterSettings() user who sets a Compressor lands here and never on v5 segments -- and
		// until now nothing covered it. TestFramer/DefaultFramerFlags and TestFramer/NewFramer
		// check only that FlagCompress is set, and TestFramer/ReadFrame/Compressed is about v5 segments
		// with a no-op compressor that never compresses a byte. A snappy encode/decode
		// asymmetry, or a length field describing the wrong side of the compression, would have
		// gone unnoticed by all three.
		//
		// The framer is driven directly rather than through a query builder: the subject is the
		// compression layer, and a fixtureQueryParams route would drag in v5-only option validation.
		t.Run("V4SnappyRoundTrip", func(t *testing.T) {
			t.Parallel()

			incompressible := make([]byte, 64*1024)
			// Fixed seed: an incompressible payload has to be the same bytes on every run, or a
			// failure here would not reproduce.
			rnd := rand.New(rand.NewSource(1))
			rnd.Read(incompressible)

			tests := []struct {
				name    string
				payload []byte
				// shrinks records which side of the "not worth it" line the payload falls on.
				// Both are legal on the wire; v4 compresses unconditionally either way.
				shrinks bool
			}{
				{"compressible", bytes.Repeat([]byte("a"), 256*1024), true},
				{"incompressible", incompressible, false},
				// The frame.go "only compress frames which are big enough" TODO: snappy expands
				// a body this short. Pinned here so the current behaviour is deliberate.
				{"tiny", []byte("SELECT * FROM system.local"), false},
				{"empty", []byte{}, false},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					w := newFramer(wirecompress.SnappyCompressor{}, protoVersion4)
					require.NotZero(t, w.Flags&frm.FlagCompress, "v4 framer with a compressor must set FlagCompress")

					w.WriteHeader(w.Flags, frm.OpQuery, 42)
					w.Buf = append(w.Buf, test.payload...)
					require.NoError(t, w.Finish())

					wire := append([]byte(nil), w.Buf...)
					require.NotZero(t, wire[1]&frm.FlagCompress, "the compress flag must reach the wire")

					// The body must actually have been transformed, otherwise a compressor that
					// silently returned its input would pass the round-trip below.
					if len(test.payload) > 0 {
						require.NotEqual(t, test.payload, wire[headSize:], "body was not compressed")
					}
					if test.shrinks {
						require.Less(t, len(wire)-headSize, len(test.payload), "compressible payload should shrink")
					} else {
						// Snappy's block format has no stored-raw mode, so literal-only output
						// always exceeds its input by the varint header plus tag bytes. This is
						// the growth the frame.go "only compress frames which are big enough"
						// TODO is about, and pinning it is why the short payloads are here.
						require.Greater(t, len(wire)-headSize, len(test.payload), "incompressible payload should grow")
					}

					r := bytes.NewReader(wire)
					head, err := readHeader(r, make([]byte, headSize))
					require.NoError(t, err)
					// setLength must describe the compressed body, not the original: a reader
					// that trusted the uncompressed length would desync the connection.
					require.Equal(t, len(wire)-headSize, head.Length)
					require.NotZero(t, head.Flags&frm.FlagCompress)

					rf := newFramer(wirecompress.SnappyCompressor{}, protoVersion4)
					require.NoError(t, rf.ReadFrame(r, &head))
					require.Zero(t, r.Len(), "readFrame must consume exactly the declared body")
					// bytes.Equal rather than require.Equal: an empty body decodes to a nil
					// slice, which is the same bytes but not the same value.
					require.True(t, bytes.Equal(test.payload, rf.Buf), "body did not survive the round trip")
				})
			}
		})
	})
	t.Run("HandshakeBudgets", func(t *testing.T) {
		for _, compressor := range []Compressor{nil, wirecompress.LZ4Compressor{}, wirecompress.SnappyCompressor{}} {
			body := bytes.Repeat([]byte{1}, 512)
			data := body
			flags := byte(0)
			if compressor != nil {
				var err error
				data, err = compressor.Encode(body)
				if err != nil {
					t.Fatal(err)
				}
				flags = frm.FlagCompress
			}
			for _, budget := range []int64{511, 512} {
				reader := NewFramer(compressor, 4)
				reader.MaxFrameBytes, reader.MaxDecompressedBytes = 1024, budget
				err := reader.ReadFrame(bytes.NewReader(data), &frm.FrameHeader{Length: len(data), Flags: flags})
				if budget == 511 && err == nil {
					t.Fatal("handshake decode exceeded budget")
				}
				if budget == 512 && (err != nil || !bytes.Equal(reader.Buf, body)) {
					t.Fatal("handshake decode", err)
				}
			}
		}
		for _, budget := range []int64{3, 4} {
			writer := NewFramer(nil, 4)
			writer.MaxFrameBytes = budget
			writer.WriteHeader(0, frm.OpOptions, 0)
			writer.Buf = append(writer.Buf, 1, 2, 3, 4)
			err := writer.Finish()
			if budget == 3 && !errors.Is(err, ErrFrameTooBig) || budget == 4 && err != nil {
				t.Fatal("handshake write bound", budget, err)
			}
		}
	})
	// Pins the carrier's two jobs: it reads as the error it
	// holds, and it gives that error back unchanged, so a caller of parseFrame never sees
	// the wrapper.
	t.Run("MalformedFrameError", func(t *testing.T) {
		t.Parallel()

		inner := fmt.Errorf("short buffer: %d", 3)
		mf := malformedFrameError{Err: inner}

		assert.Equal(t, inner.Error(), mf.Error())
		assert.Same(t, inner, errors.Unwrap(mf))

		var err error
		recoverMalformedFrame(mf, &err)
		assert.Same(t, inner, err, "parseFrame must return the wrapped error, not the wrapper")

		assert.Equal(t, "short buffer: 3", malformedFramef("short buffer: %d", 3).Error())
	})
	t.Run("NewFramer", func(t *testing.T) {
		if flags := newFramer(testMockedCompressor{}, protoVersion5).Flags; flags&frm.FlagCompress != 0 {
			t.Error("newFramer(v5) should not set FlagCompress (v5 compresses at the segment layer)")
		}
		if flags := newFramer(testMockedCompressor{}, protoVersion4).Flags; flags&frm.FlagCompress == 0 {
			t.Error("newFramer(v4) should set FlagCompress")
		}
	})
	t.Run("ParseFrame", func(t *testing.T) {
		t.Parallel()

		t.Run("RejectsFuzzCorpus", func(t *testing.T) {
			t.Parallel()

			// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
			// and should cause a panic unless fixed.
			tests := [][]byte{
				[]byte("00000\xa0000"),
				[]byte("\x8000\x0e\x00\x00\x00\x000"),
				[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
				[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
				[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
					"\x00\n0000000000\x00\x14000000" +
					"00000000000000\x00\x020000" +
					"\x00\a000000000\x00\x050000000" +
					"\xff0000000000000000000" +
					"0000000"),
				[]byte("\x82\xe600\x00\x00\x00\x000"),
				[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
				[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
					"000000000"),
				[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
					"00000000000000e00000" +
					"000\x800000000000000000" +
					"0000000000000"),
			}

			for i, test := range tests {
				t.Logf("test %d input: %q", i, test)

				r := bytes.NewReader(test)
				head, err := readHeader(r, make([]byte, 9))
				if err != nil {
					continue
				}

				framer := newFramer(nil, byte(head.Version))
				err = framer.ReadFrame(r, &head)
				if err != nil {
					continue
				}

				frame, err := framer.ParseFrame()
				if err != nil {
					continue
				}

				t.Errorf("(%d) expected to fail for input % X", i, test)
				t.Errorf("(%d) frame=%+#v", i, frame)
			}
		})
	})
	t.Run("PrepareModernLayout", func(t *testing.T) {
		// Verifies that when segmentation
		// fails partway through a multi-segment frame, framer.buf is left byte-for-byte
		// unchanged so the caller can safely release the framer.
		t.Run("LeavesBufIntactOnError", func(t *testing.T) {
			t.Parallel()

			// A payload spanning more than one segment.MaxPayloadSize chunk forces the
			// chunk loop to run, so failing on the second AppendCompressed call fails
			// after the first chunk has already been appended to the local buffer.
			original := bytes.Repeat([]byte{0xAB}, segment.MaxPayloadSize+100)

			f := newFramer(&failingCompressor{failAt: 2}, protoVersion5)
			f.Buf = append([]byte(nil), original...)

			err := f.PrepareModernLayout()
			if err == nil {
				t.Fatal("expected prepareModernLayout to fail")
			}
			if !bytes.Equal(f.Buf, original) {
				t.Fatalf("f.buf was mutated on error: len=%d, want len=%d", len(f.Buf), len(original))
			}
		})
		// Verifies that calling
		// prepareModernLayout on a framer negotiated below protocol v5 returns an
		// error instead of panicking, since the function's contract is to report
		// every failure mode (including this internal precondition) via its error
		// return.
		t.Run("RejectsPreV5", func(t *testing.T) {
			t.Parallel()

			f := newFramer(nil, protoVersion4)
			f.Buf = append([]byte(nil), []byte("some frame bytes")...)

			require.NotPanics(t, func() {
				err := f.PrepareModernLayout()
				require.Error(t, err)
			})
		})
		// Pins that a framer reused for consecutive
		// v5 requests segments them without allocating: the wire buffer must be kept on
		// the framer and swapped with the raw-frame buffer, not built from scratch (which
		// allocated the whole wire output, plus a temporary per segment, per request).
		//
		// The expanding cases additionally cover a multi-segment compressed frame whose
		// every payload grows under compression, which is what segment.EncodedSize's
		// one-segment slack is for. Expansion does not accumulate across segments: only
		// one segment is ever mid-compression, and a segment whose compressed form came
		// out larger is rewritten as its raw payload before the next one starts. Note
		// that a short estimate would only cost one extra grow rather than a per-request
		// allocation, since both buffers converge on the capacity actually needed — this
		// asserts the steady state, not the constant.
		t.Run("ReusesBuffers", func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				compressor Compressor
				size       int
			}{
				{"single segment", nil, 4096},
				{"multi segment", nil, 2*segment.MaxPayloadSize + 7},
				{"compressed", testMockedCompressor{}, 4096},
				{"expanding compressed, single segment", expandingCompressor{}, segment.MaxPayloadSize - 1},
				{"expanding compressed, multi segment", expandingCompressor{}, 5*segment.MaxPayloadSize - 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					payload := bytes.Repeat([]byte{0x5A}, tc.size)
					f := newFramer(tc.compressor, protoVersion5)

					segment := func() {
						f.Buf = append(f.Buf[:0], payload...)
						if err := f.PrepareModernLayout(); err != nil {
							t.Fatalf("prepareModernLayout: %v", err)
						}
					}

					// The first calls legitimately allocate: both buffers still have to
					// grow to the size of this frame.
					for i := 0; i < 5; i++ {
						segment()
					}

					if allocs := tests.MinAllocsPerRun(3, 20, segment); allocs != 0 {
						t.Errorf("segmenting a warmed-up framer allocated %v times per request, want 0", allocs)
					}
				})
			}
		})
		// Guards that the local-cursor refactor
		// did not change the segmented output on the success path.
		t.Run("SuccessUnchanged", func(t *testing.T) {
			t.Parallel()

			for _, size := range []int{1, segment.MaxPayloadSize - 1, segment.MaxPayloadSize, segment.MaxPayloadSize + 1, 2*segment.MaxPayloadSize + 7} {
				original := bytes.Repeat([]byte{0x5A}, size)

				// Reference output computed directly from the segment helpers.
				var want []byte
				src := original
				selfContained := true
				for len(src) > segment.MaxPayloadSize {
					seg, err := newUncompressedSegment(src[:segment.MaxPayloadSize], false)
					if err != nil {
						t.Fatalf("size %d: reference segment: %v", size, err)
					}
					want = append(want, seg...)
					src = src[segment.MaxPayloadSize:]
					selfContained = false
				}
				seg, err := newUncompressedSegment(src, selfContained)
				if err != nil {
					t.Fatalf("size %d: reference tail segment: %v", size, err)
				}
				want = append(want, seg...)

				f := newFramer(nil, protoVersion5)
				f.Buf = append([]byte(nil), original...)
				if err := f.PrepareModernLayout(); err != nil {
					t.Fatalf("size %d: prepareModernLayout: %v", size, err)
				}
				if !bytes.Equal(f.Buf, want) {
					t.Fatalf("size %d: segmented output changed", size)
				}
			}
		})
	})
	t.Run("ReadFrame", func(t *testing.T) {
		// Pins that a failed body read stays
		// recognisable as a net.Error through readFrame's wrapping. Conn.processFrameSource
		// decides whether the connection is desynced (and must be closed) from exactly this
		// check; formatting the cause with %v instead of %w silently defeats it, and the
		// half-read body is then read back as the next frame header.
		t.Run("BodyErrorKeepsNetError", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name string
				r    io.Reader
				want bool
			}{
				{
					name: "timeout mid-body",
					r:    io.MultiReader(bytes.NewReader([]byte{0x01, 0x02}), errReader{timeoutErr{}}),
					want: true,
				},
				{
					// A peer that closes mid-body is equally desyncing, but io.ErrUnexpectedEOF
					// is not a net.Error; the connection dies on the next read instead.
					name: "peer closed mid-body",
					r:    bytes.NewReader([]byte{0x01, 0x02}),
					want: false,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					f := newFramer(nil, protoVersion4)
					head := frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpReady, Length: 8}

					err := f.ReadFrame(tc.r, &head)
					require.Error(t, err)

					var netErr net.Error
					require.Equal(t, tc.want, errors.As(err, &netErr),
						"errors.As(net.Error) on %q", err)
				})
			}
		})
		t.Run("Compressed", func(t *testing.T) {
			tests := []struct {
				name string
				// modifyFrameFn is useful for simulating frame data invalidation
				modifyFrameFn func([]byte) []byte
				compressor    testMockedCompressor

				// expectedErrorMsg is an error message that should be returned by Error() method.
				// We need this to understand which of fmt.Errorf() is returned
				expectedErrorMsg string
			}{
				{
					name: "header crc24 mismatch",
					modifyFrameFn: func(frame []byte) []byte {
						// simulating some crc invalidation
						frame[0] = 255
						return frame
					},
					expectedErrorMsg: "gocql: crc24 mismatch in frame header",
				},
				{
					name: "body crc32 mismatch",
					modifyFrameFn: func(frame []byte) []byte {
						// simulating body crc32 mismatch
						frame[len(frame)-1] = 255
						return frame
					},
					expectedErrorMsg: "gocql: crc32 mismatch in payload",
				},
				{
					name: "invalid frame length",
					modifyFrameFn: func(frame []byte) []byte {
						// simulating body length invalidation
						return frame[:12]
					},
					expectedErrorMsg: "gocql: failed to read compressed frame payload",
				},
				{
					name: "cannot read body checksum",
					modifyFrameFn: func(frame []byte) []byte {
						// simulating body length invalidation
						return frame[:len(frame)-4]
					},
					expectedErrorMsg: "gocql: failed to read payload crc32",
				},
				{
					name:          "failed to encode payload",
					modifyFrameFn: nil,
					compressor: testMockedCompressor{
						expectedError: errors.New("failed to encode payload"),
					},
					expectedErrorMsg: "failed to encode payload",
				},
				{
					name:          "failed to decode payload",
					modifyFrameFn: nil,
					compressor: testMockedCompressor{
						expectedError: errors.New("failed to decode payload"),
					},
					expectedErrorMsg: "failed to decode payload",
				},
				{
					name:          "length mismatch after decoding",
					modifyFrameFn: nil,
					compressor: testMockedCompressor{
						invalidateDecodedDataLength: true,
					},
					expectedErrorMsg: "gocql: length mismatch after payload decoding",
				},
				{
					name:             "success",
					modifyFrameFn:    nil,
					expectedErrorMsg: "",
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					framer := newFramer(nil, protoVersion5)
					req := fixtureQueryFrame{
						statement: strings.Repeat("SELECT * FROM system.local ", 32),
						params: fixtureQueryParams{
							consistency: Quorum,
							keyspace:    "gocql_test",
						},
					}

					err := req.BuildFrame(framer, 128)
					require.NoError(t, err)

					frame, err := newCompressedSegment(framer.Buf, true, lz4mod.LZ4Compressor{})
					require.NoError(t, err)

					if tt.modifyFrameFn != nil {
						frame = tt.modifyFrameFn(frame)
					}

					readFrame, selfContained, err := readCompressedSegment(bytes.NewReader(frame), compressedReadFixture{tt.compressor})

					switch {
					case tt.expectedErrorMsg != "":
						require.Error(t, err)
						require.Contains(t, err.Error(), tt.expectedErrorMsg)
					case tt.compressor.expectedError != nil:
						require.ErrorIs(t, err, tt.compressor.expectedError)
					default:
						require.NoError(t, err)
						assert.True(t, selfContained)
						assert.Equal(t, framer.Buf, readFrame)
					}
				})
			}
		})
		// Covers the other half of the v4 read
		// path: a peer that sets FlagCompress on a connection the driver believes is
		// uncompressed. The body is unreadable, so this has to be an error rather than a body
		// parsed as plaintext.
		t.Run("CompressedNeedsCompressor", func(t *testing.T) {
			t.Parallel()

			w := newFramer(wirecompress.SnappyCompressor{}, protoVersion4)
			w.WriteHeader(w.Flags, frm.OpQuery, 1)
			w.Buf = append(w.Buf, []byte("SELECT * FROM system.local")...)
			require.NoError(t, w.Finish())

			r := bytes.NewReader(append([]byte(nil), w.Buf...))
			head, err := readHeader(r, make([]byte, headSize))
			require.NoError(t, err)

			rf := newFramer(nil, protoVersion4)
			err = rf.ReadFrame(r, &head)
			require.Error(t, err)
			require.Contains(t, err.Error(), "no compressor available with compressed frame body")
		})
		// This is the same pin for the oversized-frame
		// path: an over-long frame is normally recovered from by discarding its body, but
		// if the discard itself fails the remainder is still on the wire.
		t.Run("DiscardErrorKeepsNetError", func(t *testing.T) {
			t.Parallel()

			f := newFramer(nil, protoVersion4)
			head := frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpReady, Length: frm.MaxFrameSize + 1}

			err := f.ReadFrame(errReader{timeoutErr{}}, &head)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrFrameTooBig, "the discard failed, so the frame was not skipped")

			var netErr net.Error
			require.True(t, errors.As(err, &netErr), "errors.As(net.Error) on %q", err)
		})
		t.Run("RejectsTooLong", func(t *testing.T) {
			t.Parallel()

			if os.Getenv("TRAVIS") == "true" {
				t.Skip("skipping test in travis due to memory pressure with the race detecor")
			}

			r := &bytes.Buffer{}
			r.Write(make([]byte, frm.MaxFrameSize+1))
			// write a new header right after this frame to verify that we can read it
			r.Write([]byte{0x03, 0x00, 0x00, 0x00, byte(frm.OpReady), 0x00, 0x00, 0x00, 0x00})

			framer := newFramer(nil, 3)

			head := frm.FrameHeader{
				Version: protoVersion3,
				Op:      frm.OpReady,
				Length:  r.Len() - 9,
			}

			err := framer.ReadFrame(r, &head)
			if err != ErrFrameTooBig {
				t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
			}

			head, err = readHeader(r, make([]byte, 9))
			if err != nil {
				t.Fatal(err)
			}
			if head.Op != frm.OpReady {
				t.Fatalf("expected to get header %v got %v", frm.OpReady, head.Op)
			}
		})
		t.Run("Uncompressed", func(t *testing.T) {
			tests := []struct {
				name        string
				modifyFrame func([]byte) []byte
				expectedErr string
			}{
				{
					name: "header crc24 mismatch",
					modifyFrame: func(frame []byte) []byte {
						// simulating some crc invalidation
						frame[0] = 255
						return frame
					},
					expectedErr: "gocql: crc24 mismatch in frame header",
				},
				{
					name: "body crc32 mismatch",
					modifyFrame: func(frame []byte) []byte {
						// simulating body crc32 mismatch
						frame[len(frame)-1] = 255
						return frame
					},
					expectedErr: "gocql: payload crc32 mismatch",
				},
				{
					name: "invalid frame length",
					modifyFrame: func(frame []byte) []byte {
						// simulating body length invalidation
						frame = frame[:7]
						return frame
					},
					expectedErr: "gocql: failed to read uncompressed frame payload",
				},
				{
					name: "cannot read body checksum",
					modifyFrame: func(frame []byte) []byte {
						// simulating body length invalidation
						frame = frame[:len(frame)-4]
						return frame
					},
					expectedErr: "gocql: failed to read payload crc32",
				},
				{
					name:        "success",
					modifyFrame: nil,
					expectedErr: "",
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					framer := newFramer(nil, protoVersion5)
					req := fixtureQueryFrame{
						statement: "SELECT * FROM system.local",
						params: fixtureQueryParams{
							consistency: Quorum,
							keyspace:    "gocql_test",
						},
					}

					err := req.BuildFrame(framer, 128)
					require.NoError(t, err)

					frame, err := newUncompressedSegment(framer.Buf, true)
					require.NoError(t, err)

					if tt.modifyFrame != nil {
						frame = tt.modifyFrame(frame)
					}

					readFrame, isSelfContained, err := readUncompressedSegment(bytes.NewReader(frame))

					if tt.expectedErr != "" {
						require.Error(t, err)
						require.Contains(t, err.Error(), tt.expectedErr)
					} else {
						require.NoError(t, err)
						assert.True(t, isSelfContained)
						assert.Equal(t, framer.Buf, readFrame)
					}
				})
			}
		})
	})
	// Pins that a length field with the high bit
	// set is rejected by readHeader. The field is signed on the wire, so such a value
	// arrives negative; readFrame also rejects it, but only an error out of readHeader
	// closes the connection, and a header this broken means the stream position can no
	// longer be trusted.
	t.Run("ReadHeader", func(t *testing.T) {
		t.Parallel()

		header := []byte{
			protoVersion4 | protoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
			0xFF, 0xFF, 0xFF, 0xFF, // length = -1
		}

		_, err := readHeader(bytes.NewReader(header), make([]byte, headSize))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid frame body length")
	})
	t.Run("ReadLongString", func(t *testing.T) {
		f := newFramer(nil, protoVersion4)
		f.Buf = []byte{0xFF, 0xFF, 0xFF, 0xFF, 'x'} // a declared length of -1

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("a negative long string length was accepted")
			}
			mf, ok := r.(malformedFrameError)
			if !ok {
				t.Fatalf("panicked with %T, which parseFrame re-panics instead of converting: %v", r, r)
			}
			if !strings.Contains(mf.Error(), "-1") {
				t.Errorf("the error does not name the length: %v", mf)
			}
		}()

		f.ReadLongString()
	})
	t.Run("RecoverMalformedFrame", func(t *testing.T) {
		t.Parallel()

		// Pins the nil case: nothing panicked,
		// so the error parseFrame is about to return stays as it is.
		t.Run("IgnoresNormalReturn", func(t *testing.T) {
			t.Parallel()

			err := error(nil)
			recoverMalformedFrame(nil, &err)
			assert.NoError(t, err)

			sentinel := fmt.Errorf("got a request frame from server")
			err = sentinel
			recoverMalformedFrame(nil, &err)
			assert.Same(t, sentinel, err)
		})
		// Pins that only the read path's own value
		// becomes an error. Anything else -- a runtime.Error, a bare error from code that does
		// not speak this contract, a panic("...") -- keeps unwinding, so a driver bug is never
		// reported as a malformed frame from the peer.
		t.Run("RePanicsDriverBug", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name  string
				value any
			}{
				{name: "runtime error", value: capturedRuntimeError(t)},
				{name: "bare error", value: fmt.Errorf("not enough bytes in buffer")},
				{name: "string", value: "compress flag set with no compressor"},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					var err error
					func() {
						defer func() {
							r := recover()
							if r == nil {
								t.Fatalf("%T was swallowed instead of re-panicked", test.value)
							}
							assert.Equal(t, test.value, r, "the panic value must survive unchanged")
						}()

						recoverMalformedFrame(test.value, &err)
					}()

					assert.NoError(t, err, "a re-panicked value must not also be reported as a frame error")
				})
			}
		})
	})
}
