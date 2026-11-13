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
// including loads that end exactly on a span boundary. A scalar load with an
// even arg, and a Skip at an even op index, first tries the call-free
// head-span fast path (HeadUint32, HeadUint64, HeadBytes, PeekHeadUint32,
// HeadSkip), which must either succeed with the reference result or miss
// without moving the cursor.
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
				hit := false
				if arg%2 == 0 {
					got, hit = headScalar(t, cur, width, i)
				}
				switch {
				case hit:
				case width == 1:
					v, e := cur.Uint8()
					got, err = uint64(v), e
				case width == 2:
					v, e := cur.Uint16()
					got, err = uint64(v), e
				case width == 4:
					v, e := cur.Uint32()
					got, err = uint64(v), e
				default:
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
				var err error
				before := *cur
				if i%2 == 0 && cur.HeadSkip(int64(arg)) {
					if arg >= len(data)-pos {
						t.Fatalf("op %d: head skip %d reached the end", i, arg)
					}
				} else {
					if moved(cur, before) {
						t.Fatalf("op %d: missed head skip moved the cursor", i)
					}
					err = cur.Skip(int64(arg))
				}
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

// headScalar loads a width-byte scalar through the head-span fast paths. On a
// miss it checks that the cursor did not move and reports false.
func headScalar(t *testing.T, cur *Cursor, width, op int) (uint64, bool) {
	before := *cur
	var got uint64
	hit := false
	switch width {
	case 4:
		peek, peeked := cur.PeekHeadUint32()
		if peeked && moved(cur, before) {
			t.Fatalf("op %d: PeekHeadUint32 moved the cursor", op)
		}
		v, ok := cur.HeadUint32()
		if ok != peeked || ok && v != peek {
			t.Fatalf("op %d: HeadUint32 %x %v, peeked %x %v", op, v, ok, peek, peeked)
		}
		got, hit = uint64(v), ok
	case 8:
		got, hit = cur.HeadUint64()
	default:
		body, ok := cur.HeadBytes(width)
		if ok {
			if moved(cur, before) || len(body) != width {
				t.Fatalf("op %d: HeadBytes(%d) moved the cursor or returned %d bytes", op, width, len(body))
			}
			for _, b := range body {
				got = got<<8 | uint64(b)
			}
			cur.AdvanceHead(width)
		}
		hit = ok
	}
	if !hit && moved(cur, before) {
		t.Fatalf("op %d: missed head load of %d moved the cursor", op, width)
	}
	return got, hit
}

func moved(cur *Cursor, before Cursor) bool {
	return cur.index != before.index || cur.offset != before.offset || cur.remaining != before.remaining
}
