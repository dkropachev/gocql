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

package wirecompress

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestCompressor(t *testing.T) {
	t.Run("LZ4", func(t *testing.T) {
		t.Run("EncodeDecode", func(t *testing.T) {
			t.Parallel()

			var c LZ4Compressor
			require.Equal(t, "lz4", c.Name())

			_, err := c.Decode([]byte{0, 1, 2})
			require.EqualError(t, err, "cassandra lz4 block size should be >4, got=3")

			_, err = c.Decode([]byte{0, 1, 2, 4, 5})
			require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

			// If uncompressed size is zero then nothing is decoded even if present.
			decoded, err := c.Decode([]byte{0, 0, 0, 0, 5, 7, 8})
			require.NoError(t, err)
			require.Nil(t, decoded)

			original := []byte("My Test String")
			encoded, err := c.Encode(original)
			require.NoError(t, err)
			decoded, err = c.Decode(encoded)
			require.NoError(t, err)
			require.Equal(t, original, decoded)
		})

		// TestCompressor/LZ4/IncompressibleRoundTrip covers incompressible input, which
		// the existing "My Test String" case does not. lz4 cannot shrink random bytes,
		// so Encode emits a block slightly larger than the input; Decode must still
		// reverse it exactly. This guards the expand-rather-than-compress path.
		t.Run("IncompressibleRoundTrip", func(t *testing.T) {
			var c LZ4Compressor

			// Random bytes do not compress. Use a fixed seed for determinism.
			rng := rand.New(rand.NewSource(1))
			original := make([]byte, 1024)
			_, _ = rng.Read(original)

			encoded, err := c.Encode(original)
			require.NoError(t, err)
			// Confirm we really are on the expand path: the lz4 block (minus the 4-byte
			// length prefix) is larger than the input.
			require.Greater(t, len(encoded)-dataLengthSize, len(original),
				"random input should expand, exercising the incompressible path")

			decoded, err := c.Decode(encoded)
			require.NoError(t, err)
			require.Equal(t, original, decoded)
		})

		// TestCompressor/LZ4/DecodeRejectsHugeLength ensures a peer-supplied uncompressed
		// length above maxDecompressedSize is rejected before allocating, rather than
		// driving a multi-GB allocation (or a makeslice panic on 32-bit).
		t.Run("DecodeRejectsHugeLength", func(t *testing.T) {
			var c LZ4Compressor
			var hdr [dataLengthSize]byte
			binary.BigEndian.PutUint32(hdr[:], maxDecompressedSize+1)
			_, err := c.Decode(append(hdr[:], 0x00))
			require.Error(t, err)
			require.Contains(t, err.Error(), "exceeds maximum")
		})

		// TestCompressor/LZ4/AppendDecompressedRejectsHugeLength is the AppendDecompressed
		// counterpart of the Decode case above. The driver never reaches it — the v5
		// segment header carries the length in 17 bits — but the method is exported, and
		// without the bound a large uncompressedLength is either an unbounded allocation
		// or, on 32-bit where int() of a value above math.MaxInt32 goes negative, a slice
		// bounds panic inside grow.
		t.Run("AppendDecompressedRejectsHugeLength", func(t *testing.T) {
			var c LZ4Compressor

			for _, tc := range []struct {
				name   string
				length uint32
			}{
				{"just above the limit", maxDecompressedSize + 1},
				{"negative once narrowed to a 32-bit int", math.MaxUint32},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := c.AppendDecompressed(nil, []byte{0x00}, tc.length)
					require.Error(t, err)
					require.Contains(t, err.Error(), "exceeds maximum")
				})
			}

			// The bound must not reject a legal segment-sized payload.
			original := []byte("My Test String")
			encoded, err := c.AppendCompressed(nil, original)
			require.NoError(t, err)
			decoded, err := c.AppendDecompressed(nil, encoded, uint32(len(original)))
			require.NoError(t, err)
			require.Equal(t, original, decoded)
		})

		t.Run("AppendCompressedDecompressed", func(t *testing.T) {
			c := LZ4Compressor{}

			invalidUncompressedLength := uint32(10)
			_, err := c.AppendDecompressed(nil, []byte{0, 1, 2, 4, 5}, invalidUncompressedLength)
			require.EqualError(t, err, "lz4: invalid source or destination buffer too short")

			original := []byte("My Test String")
			encoded, err := c.AppendCompressed(nil, original)
			require.NoError(t, err)
			decoded, err := c.AppendDecompressed(nil, encoded, uint32(len(original)))
			require.NoError(t, err)
			require.Equal(t, original, decoded)
		})

		t.Run("AppendGrowSliceWithData", func(t *testing.T) {
			var tests = []struct {
				name                 string
				src                  []byte
				dst                  []byte
				shouldReuseDst       bool
				decodeDst            []byte
				shouldReuseDecodeDst bool
			}{
				{
					name:      "both dst are empty",
					src:       []byte("small data"),
					dst:       nil,
					decodeDst: nil,
				},
				{
					name:      "dst is nil",
					src:       []byte("another piece of data"),
					dst:       nil,
					decodeDst: []byte("something"),
				},
				{
					name:      "decodeDst is nil",
					src:       []byte("another piece of data"),
					dst:       []byte("some"),
					decodeDst: nil,
				},
				{
					name:      "both dst are not empty",
					src:       []byte("another piece of data"),
					dst:       []byte("dst"),
					decodeDst: []byte("decodeDst"),
				},
				{
					name:                 "both dst slices have enough capacity",
					src:                  []byte("small"),
					dst:                  createBufWithCapAndData("cap=128", 128),
					shouldReuseDst:       true,
					decodeDst:            createBufWithCapAndData("cap=256", 256),
					shouldReuseDecodeDst: true,
				},
				{
					name:      "both dst slices have some data and not enough capacity",
					src:       []byte("small"),
					dst:       createBufWithCapAndData("data", 6),
					decodeDst: createBufWithCapAndData("wow", 4),
				},
				// The two reuse flags have to disagree in at least one case each way.
				// Every case above sets them equal, which makes both branches below
				// compute the same expectation — so an assertion keyed on the wrong
				// flag would pass on the whole table. These two are recombinations of
				// the dst/decodeDst values already used above, so what each one grows
				// or reuses is pinned by the symmetric cases.
				{
					name:                 "compress dst has capacity, decode dst does not",
					src:                  []byte("small"),
					dst:                  createBufWithCapAndData("cap=128", 128),
					shouldReuseDst:       true,
					decodeDst:            createBufWithCapAndData("wow", 4),
					shouldReuseDecodeDst: false,
				},
				{
					name:                 "decode dst has capacity, compress dst does not",
					src:                  []byte("small"),
					dst:                  createBufWithCapAndData("data", 6),
					shouldReuseDst:       false,
					decodeDst:            createBufWithCapAndData("cap=256", 256),
					shouldReuseDecodeDst: true,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					compressor := LZ4Compressor{}

					// Appending compressed data to dst,
					// expecting that dst still contains "test"
					result, err := compressor.AppendCompressed(tt.dst, tt.src)
					require.NoError(t, err)

					var expectedCap int
					if tt.shouldReuseDst {
						expectedCap = cap(tt.dst)
					} else {
						expectedCap = len(tt.dst) + lz4.CompressBlockBound(len(tt.src))
					}

					require.Equal(t, expectedCap, cap(result))
					if len(tt.dst) > 0 {
						require.Equal(t, tt.dst, result[:len(tt.dst)])
					}

					uncompressedLen := uint32(len(tt.src))
					result, err = compressor.AppendDecompressed(tt.decodeDst, result[len(tt.dst):], uncompressedLen)
					require.NoError(t, err)

					var expectedDecodeCap int
					if tt.shouldReuseDecodeDst {
						expectedDecodeCap = cap(tt.decodeDst)
					} else {
						expectedDecodeCap = len(tt.decodeDst) + len(tt.src)
					}

					require.Equal(t, expectedDecodeCap, cap(result))
					require.Equal(t, tt.src, result[len(tt.decodeDst):])
				})
			}
		})
	})

	t.Run("Snappy", func(t *testing.T) {
		t.Run("EncodeDecode", func(t *testing.T) {
			t.Parallel()

			t.Run("basic", func(t *testing.T) {
				c := SnappyCompressor{}
				if c.Name() != "snappy" {
					t.Fatalf("expected name to be 'snappy', got %v", c.Name())
				}

				str := "My Test String"
				//Test Encoding with S2 library, Snappy compatible encoding.
				expected := s2.EncodeSnappy(nil, []byte(str))
				if res, err := c.Encode([]byte(str)); err != nil {
					t.Fatalf("failed to encode '%v' with error %v", str, err)
				} else if bytes.Compare(expected, res) != 0 {
					t.Fatal("failed to match the expected encoded value with the result encoded value.")
				}

				val, err := c.Encode([]byte(str))
				if err != nil {
					t.Fatalf("failed to encode '%v' with error '%v'", str, err)
				}

				//Test Decoding with S2 library, Snappy compatible encoding.
				if expected, err := s2.Decode(nil, val); err != nil {
					t.Fatalf("failed to decode '%v' with error %v", val, err)
				} else if res, err := c.Decode(val); err != nil {
					t.Fatalf("failed to decode '%v' with error %v", val, err)
				} else if bytes.Compare(expected, res) != 0 {
					t.Fatal("failed to match the expected decoded value with the result decoded value.")
				}
			})

			t.Run("frame-examples", func(t *testing.T) {
				c := SnappyCompressor{}

				t.Run("Encode", func(t *testing.T) {
					for id := range frameExamples.Requests {
						frame := frameExamples.Requests[id]
						t.Run(frame.Name, func(t *testing.T) {
							t.Parallel()

							encoded, err := c.Encode(frame.Frame)
							if err != nil {
								t.Fatalf("failed to encode frame %s", frame.Name)
							}
							decoded, err := c.Decode(encoded)
							if err != nil {
								t.Fatalf("failed to decode frame %s", frame.Name)
							}

							if bytes.Compare(decoded, frame.Frame) != 0 {
								t.Fatalf("failed to match the decoded value with the original value")
							}
							t.Logf("Compression rate %f", float64(len(encoded))/float64(len(frame.Frame)))
						})
					}
				})

				t.Run("Decode", func(t *testing.T) {
					for id := range frameExamples.Responses {
						frame := frameExamples.Responses[id]
						t.Run(frame.Name, func(t *testing.T) {
							t.Parallel()

							decoded, err := c.Decode(frame.Frame)
							if err != nil {
								t.Fatalf("failed to decode frame %s", frame.Name)
							}

							if len(decoded) == 0 {
								t.Fatalf("frame was decoded to empty slice")
							}
						})
					}
				})
			})
		})

		t.Run("DecodeRejectsHugeLength", func(t *testing.T) {
			t.Parallel()

			c := SnappyCompressor{}

			for _, tc := range []struct {
				name   string
				length uint64
			}{
				{"just above the limit", frm.MaxFrameSize + 1},
				// Capped at MaxInt32 rather than s2's own 4 GiB ceiling. Above MaxInt32,
				// s2.DecodedLen rejects the length itself where int is 32 bits wide and
				// returns "s2: decoded block is too large", so the assertion below would be
				// pinning s2's guard rather than this one. gocql does not build on a 32-bit
				// platform today -- serialization/varint has constants that overflow int
				// there -- so this is hygiene rather than a live case, but it keeps the test
				// about the only thing it is for.
				{"far above the limit", math.MaxInt32},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					var hdr [binary.MaxVarintLen64]byte
					n := binary.PutUvarint(hdr[:], tc.length)

					// One trailing byte so the input is a plausible block rather than a bare
					// header: the bound must reject it before the stream is even looked at.
					_, err := c.Decode(append(hdr[:n], 0x00))
					require.Error(t, err)
					require.Contains(t, err.Error(), "exceeds maximum")
				})
			}
		})
	})
}
