// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
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
						if compressor.Name() == "lz4" {
							encoded = binary.BigEndian.AppendUint32(nil, uint32(length))
						} else {
							encoded = binary.AppendUvarint(nil, length)
						}
						n, err := decoder.DecodedLen(encoded)
						if length <= math.MaxInt32 && (err != nil || uint64(n) != length) {
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

		t.Run("CustomRetainsFallback", func(t *testing.T) {
			if _, ok := FrameDecoderWithLimit(aliasingDecoder{}, math.MaxInt32); ok {
				t.Fatal("custom aliasing decoder treated as a built-in")
			}
		})
	})
}
