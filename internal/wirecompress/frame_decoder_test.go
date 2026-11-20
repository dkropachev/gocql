// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"testing"
)

func TestFrameDecoder(t *testing.T) {
	t.Run("DecodeTo", func(t *testing.T) {
		t.Run("FinalArena", func(t *testing.T) {
			for _, codec := range []interface {
				Name() string
				Encode([]byte) ([]byte, error)
				Decode([]byte) ([]byte, error)
				DecodedLen([]byte) (int, error)
				DecodeTo([]byte, []byte) ([]byte, error)
			}{LZ4Compressor{}, SnappyCompressor{}} {
				for _, n := range []int{0, 32, 4096, 8 << 20} {
					t.Run(fmt.Sprintf("%s/%d", codec.Name(), n), func(t *testing.T) {
						input := bytes.Repeat([]byte{0x71}, n)
						encoded, e := codec.Encode(input)
						if e != nil {
							t.Fatal(e)
						}
						dst := make([]byte, 0, n)
						out, e := codec.DecodeTo(dst, encoded)
						if e != nil || !bytes.Equal(out, input) {
							t.Fatal(e)
						}
						if n > 0 && &out[0] != &dst[:cap(dst)][0] {
							t.Fatal("decompressed output copied/replaced")
						}
						ordinary, e := codec.Decode(encoded)
						if e != nil || !bytes.Equal(ordinary, out) {
							t.Fatal("ordinary decode drift", e)
						}
						for prefix := 0; prefix < min(16, len(encoded)); prefix++ {
							_, _ = codec.DecodeTo(dst, encoded[:prefix])
						}
					})
				}
			}
			if _, e := (LZ4Compressor{}).DecodeTo(make([]byte, 0, 16), []byte{0x7f, 0xff, 0xff, 0xff}); e == nil {
				t.Fatal("oversized LZ4")
			}
			if _, e := (SnappyCompressor{}).DecodeTo(make([]byte, 0, 16), []byte{0xff, 0xff, 0xff, 0xff, 0x0f}); e == nil {
				t.Fatal("oversized Snappy")
			}
		})
	})

	t.Run("FrameDecoderWithLimit", func(t *testing.T) {
		t.Run("ConfiguredBounds", func(t *testing.T) {
			for _, compressor := range []interface {
				Name() string
				Encode([]byte) ([]byte, error)
				DecodedLen([]byte) (int, error)
			}{LZ4Compressor{}, SnappyCompressor{}} {
				t.Run(compressor.Name(), func(t *testing.T) {
					decoder, ok := FrameDecoderWithLimit(compressor, math.MaxInt32)
					if !ok {
						t.Fatal("missing bounded decoder")
					}
					for _, length := range []uint64{256<<20 + 1, 512 << 20, math.MaxInt32, math.MaxInt32 + 1, math.MaxUint32} {
						var encoded []byte
						// Enough body to make the length possible, where that
						// fits a test; DecodedLen reads only its size.
						var body uint64
						if compressor.Name() == "lz4" {
							encoded = binary.BigEndian.AppendUint32(nil, uint32(length))
							body = (length + maxLZ4Expansion - 1) / maxLZ4Expansion
						} else {
							encoded = binary.AppendUvarint(nil, length)
							body = (length*snappyMinCopySize + snappyMaxCopy - 1) / snappyMaxCopy
						}
						possible := length <= math.MaxInt32 && body <= 32<<20
						if possible {
							encoded = append(encoded, make([]byte, body)...)
						}
						n, err := decoder.DecodedLen(encoded)
						if possible && (err != nil || uint64(n) != length) {
							t.Fatal(length, n, err)
						}
						if length > math.MaxInt32 && err == nil {
							t.Fatal("invalid signed length accepted", length)
						}
						if _, err := compressor.DecodedLen(encoded); err == nil {
							t.Fatal("standalone default ceiling bypassed")
						}
					}
					input := bytes.Repeat([]byte("bounded transform"), 32)
					encoded, err := compressor.Encode(input)
					if err != nil {
						t.Fatal(err)
					}
					bounded, _ := FrameDecoderWithLimit(compressor, int64(len(input)))
					dst := make([]byte, 0, len(input))
					out, err := bounded.DecodeTo(dst, encoded)
					if err != nil || !bytes.Equal(out, input) || &out[0] != &dst[:cap(dst)][0] {
						t.Fatal("final output/backing", err)
					}
					if allocations := testing.AllocsPerRun(100, func() {
						decoder, _ := FrameDecoderWithLimit(compressor, 512<<20)
						out, err = decoder.DecodeTo(dst, encoded)
					}); allocations != 0 || err != nil {
						t.Fatal("configured decoder wrapper allocated", allocations, err)
					}
					reject, _ := FrameDecoderWithLimit(compressor, int64(len(input)-1))
					if _, err := reject.DecodeTo(dst, encoded); err == nil {
						t.Fatal("output allocated/decoded above configured bound")
					}
				})
			}
		})

		// A declared LZ4 length the block cannot decode to is rejected before
		// the output is allocated, however generous the limit; a block at
		// LZ4's real expansion ceiling still decodes.
		t.Run("LZ4ExpansionBound", func(t *testing.T) {
			decoder, _ := FrameDecoderWithLimit(LZ4Compressor{}, math.MaxInt32)
			forged := binary.BigEndian.AppendUint32(nil, 256<<20)
			forged = append(forged, 0x00)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for _, decode := range []func() error{
				func() error { _, err := decoder.DecodedLen(forged); return err },
				func() error { _, err := decoder.DecodeTo(nil, forged); return err },
				func() error { _, err := LZ4Compressor{}.Decode(forged); return err },
				func() error { _, err := LZ4Compressor{}.AppendDecompressed(nil, forged[4:], 1<<17); return err },
			} {
				if err := decode(); err == nil {
					t.Fatal("forged length accepted")
				}
			}
			runtime.ReadMemStats(&after)
			if grown := after.TotalAlloc - before.TotalAlloc; grown > 1<<20 {
				t.Fatalf("rejecting a forged length allocated %d bytes", grown)
			}

			input := make([]byte, 16<<20) // compresses close to 255:1
			encoded, err := LZ4Compressor{}.Encode(input)
			if err != nil {
				t.Fatal(err)
			}
			if ratio := float64(len(input)) / float64(len(encoded)-4); ratio < 250 {
				t.Fatalf("zeros compressed only %.1f:1; the case no longer tests the ceiling", ratio)
			}
			out, err := decoder.DecodeTo(nil, encoded)
			if err != nil || !bytes.Equal(out, input) {
				t.Fatal("block at the expansion ceiling", err)
			}
			if out, err = (LZ4Compressor{}).Decode(encoded); err != nil || !bytes.Equal(out, input) {
				t.Fatal("block at the expansion ceiling", err)
			}
		})

		// Snappy likewise: a copy element takes three bytes for at most 64.
		t.Run("SnappyExpansionBound", func(t *testing.T) {
			decoder, _ := FrameDecoderWithLimit(SnappyCompressor{}, math.MaxInt32)
			forged := binary.AppendUvarint(nil, 256<<20)
			forged = append(forged, 0x00)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for _, decode := range []func() error{
				func() error { _, err := decoder.DecodedLen(forged); return err },
				func() error { _, err := decoder.DecodeTo(nil, forged); return err },
				func() error { _, err := SnappyCompressor{}.Decode(forged); return err },
			} {
				if err := decode(); err == nil {
					t.Fatal("forged length accepted")
				}
			}
			runtime.ReadMemStats(&after)
			if grown := after.TotalAlloc - before.TotalAlloc; grown > 1<<20 {
				t.Fatalf("rejecting a forged length allocated %d bytes", grown)
			}

			input := make([]byte, 16<<20) // compresses to about 20:1, near the 64:3 ceiling
			encoded, err := SnappyCompressor{}.Encode(input)
			if err != nil {
				t.Fatal(err)
			}
			if ratio := float64(len(input)) / float64(len(encoded)); ratio < 19 {
				t.Fatalf("zeros compressed only %.1f:1; the case no longer tests the ceiling", ratio)
			}
			out, err := decoder.DecodeTo(nil, encoded)
			if err != nil || !bytes.Equal(out, input) {
				t.Fatal("block at the expansion ceiling", err)
			}
			if out, err = (SnappyCompressor{}).Decode(encoded); err != nil || !bytes.Equal(out, input) {
				t.Fatal("block at the expansion ceiling", err)
			}
			// Short legal blocks, down to a single literal, are not rejected.
			for _, input := range [][]byte{{}, {'a'}, []byte("aaaa"), bytes.Repeat([]byte{'b'}, 70)} {
				encoded, _ := SnappyCompressor{}.Encode(input)
				if out, err := decoder.DecodeTo(nil, encoded); err != nil || !bytes.Equal(out, input) {
					t.Fatalf("short block %q: %q, %v", input, out, err)
				}
			}
		})

		t.Run("CustomRetainsFallback", func(t *testing.T) {
			if _, ok := FrameDecoderWithLimit(aliasingDecoder{}, math.MaxInt32); ok {
				t.Fatal("custom aliasing decoder treated as a built-in")
			}
		})
	})
}
