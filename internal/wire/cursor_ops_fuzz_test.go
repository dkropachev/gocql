// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// FuzzCursorOps drives a random mix of scalar, Take, TakeNested and Skip
// operations over an arbitrarily segmented chain and checks every result,
// error and remaining count against a flat reference slice. It covers the
// transitions between single-span fast paths and span-crossing slow paths,
// including loads that end exactly on a span boundary.
func FuzzCursorOps(f *testing.F) {
	f.Add([]byte("0123456789abcdefghijklmnopqrstuvwxyz"), []byte{3, 7}, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Add(bytes.Repeat([]byte{0xa5}, 64), []byte{4}, []byte{2, 2, 2, 2, 4, 4})
	f.Add([]byte{1, 2, 3, 4, 5}, []byte{0}, []byte{3, 3, 3})
	// A load ending exactly on a span boundary must move to the next span.
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{3}, []byte{2, 2, 0x45, 0})
	f.Fuzz(func(t *testing.T, data, cuts, ops []byte) {
		var spans []Span
		rest := data
		for i := 0; len(rest) > 0; i++ {
			n := len(rest)
			if len(cuts) > 0 {
				n = min(int(cuts[i%len(cuts)])+1, len(rest))
			}
			spans = append(spans, Borrow(rest[:n]))
			if i%3 == 1 {
				spans = append(spans, Span{}) // empty descriptors are dropped
			}
			rest = rest[n:]
		}
		c, err := NewChain(spans...)
		if err != nil {
			t.Fatal(err)
		}
		cur := NewCursor(c)
		pos := 0
		for i, op := range ops {
			left := len(data) - pos
			arg := int(op >> 4)
			switch op % 6 {
			case 0, 1, 2, 3: // scalar widths 1, 2, 4, 8
				width := 1 << (op % 4)
				var got uint64
				var err error
				switch width {
				case 1:
					v, e := cur.Uint8()
					got, err = uint64(v), e
				case 2:
					v, e := cur.Uint16()
					got, err = uint64(v), e
				case 4:
					v, e := cur.Uint32()
					got, err = uint64(v), e
				case 8:
					got, err = cur.Uint64()
				}
				if width > left {
					if !errors.Is(err, io.ErrUnexpectedEOF) || cur.Remaining() != int64(left) {
						t.Fatalf("op %d: short scalar %d: %v remaining %d", i, width, err, cur.Remaining())
					}
					continue
				}
				var want uint64
				var buf [8]byte
				copy(buf[8-width:], data[pos:pos+width])
				want = binary.BigEndian.Uint64(buf[:])
				if err != nil || got != want {
					t.Fatalf("op %d: scalar %d = %x, %v want %x", i, width, got, err, want)
				}
				pos += width
			case 4: // Take or TakeNested
				take := cur.Take
				if arg%2 == 1 {
					take = cur.TakeNested
					arg *= 5
				}
				view, err := take(int64(arg))
				if arg > left {
					if err == nil || cur.Remaining() != int64(left) {
						t.Fatalf("op %d: short take advanced", i)
					}
					continue
				}
				if err != nil || !bytes.Equal(payload(t, view), data[pos:pos+arg]) {
					t.Fatalf("op %d: take %d: %v", i, arg, err)
				}
				pos += arg
			case 5: // Skip
				err := cur.Skip(int64(arg))
				if arg > left {
					if err == nil || cur.Remaining() != int64(left) {
						t.Fatalf("op %d: short skip advanced", i)
					}
					continue
				}
				if err != nil {
					t.Fatalf("op %d: skip %d: %v", i, arg, err)
				}
				pos += arg
			}
			if cur.Remaining() != int64(len(data)-pos) {
				t.Fatalf("op %d: remaining %d want %d", i, cur.Remaining(), len(data)-pos)
			}
		}
		tail, err := cur.Take(cur.Remaining())
		if err != nil || !bytes.Equal(payload(t, tail), data[pos:]) || cur.RequireEnd() != nil {
			t.Fatalf("tail mismatch: %v", err)
		}
	})
}
