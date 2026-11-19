// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wirecompress

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// FuzzDecompress feeds arbitrary compressed bodies, as a peer sends them, to
// every built-in decoder. None may panic, and the connection decoders never
// produce more than their limit.
func FuzzDecompress(f *testing.F) {
	plain := bytes.Repeat([]byte("compressible "), 40)
	lz4Frame, _ := LZ4Compressor{}.Encode(plain)
	snappyFrame, _ := SnappyCompressor{}.Encode(plain)
	block, _ := LZ4Compressor{}.AppendCompressed(nil, plain)
	f.Add(lz4Frame, uint32(len(plain)), uint16(1024))
	f.Add(snappyFrame, uint32(len(plain)), uint16(1024))
	f.Add(block, uint32(len(plain)), uint16(100))
	f.Add([]byte{0x7f, 0xff, 0xff, 0xff, 0x00}, uint32(1<<30), uint16(64))   // a huge declared length
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x0f, 0x00}, uint32(0), uint16(64)) // a huge snappy varint
	f.Add([]byte{0, 0, 0, 8, 0xf0}, uint32(8), uint16(8))                    // a truncated block
	f.Fuzz(func(t *testing.T, data []byte, declared uint32, limit uint16) {
		for _, c := range []interface{ Name() string }{LZ4Compressor{}, SnappyCompressor{}} {
			decoder, ok := FrameDecoderWithLimit(c, int64(limit))
			if !ok {
				t.Fatalf("%s has no frame decoder", c.Name())
			}
			if n, err := decoder.DecodedLen(data); err == nil && n > int(limit) {
				t.Fatalf("%s: declared length %d passed limit %d", c.Name(), n, limit)
			}
			if out, err := decoder.DecodeTo(nil, data); err == nil && len(out) > int(limit) {
				t.Fatalf("%s: decoded %d bytes past limit %d", c.Name(), len(out), limit)
			}
		}
		_, _ = SnappyCompressor{}.Decode(data)
		// The out-of-band length of a v5 segment, bounded to 17 bits.
		declared &= 0x1ffff
		if out, err := (LZ4Compressor{}).AppendDecompressed(nil, data, declared); err == nil && len(out) != int(declared) {
			t.Fatalf("AppendDecompressed: %d bytes, declared %d", len(out), declared)
		}
		if out, err := (LZ4Compressor{}).Decode(data); err == nil && len(out) != int(binary.BigEndian.Uint32(data)) {
			t.Fatalf("Decode: %d bytes, declared %d", len(out), binary.BigEndian.Uint32(data))
		}
	})
}

// TestLZ4ShortBlock checks that a block which decodes to fewer bytes than its
// declared length is rejected rather than returned short.
func TestLZ4ShortBlock(t *testing.T) {
	frame, err := LZ4Compressor{}.Encode([]byte("short block"))
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(frame, 1000)
	if out, err := (LZ4Compressor{}).Decode(frame); err == nil {
		t.Fatalf("decoded %d bytes of a block declaring 1000", len(out))
	}
	if out, err := (LZ4Compressor{}).AppendDecompressed(nil, frame[4:], 1000); err == nil {
		t.Fatalf("decoded %d bytes of a block declaring 1000", len(out))
	}
}
