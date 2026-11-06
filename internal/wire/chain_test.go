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

package wire

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
)

func TestChain(t *testing.T) {
	t.Run("Slice", func(t *testing.T) {
		t.Run("NestedPreservesEndpointTrims", func(t *testing.T) {
			data := []byte("0123456789abcdef")
			var spans []Span
			for i := range data {
				spans = append(spans, Borrow(data[i:i+1]))
			}
			chain, err := NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			for off := 0; off <= len(data); off++ {
				for n := 0; n <= len(data)-off; n++ {
					outer, err := chain.Slice(int64(off), int64(n))
					if err != nil {
						t.Fatal(err)
					}
					for innerOff := 0; innerOff <= n; innerOff++ {
						for innerN := 0; innerN <= n-innerOff; innerN++ {
							inner, err := outer.Slice(int64(innerOff), int64(innerN))
							if err != nil {
								t.Fatal(err)
							}
							var got []byte
							inner.Each(func(s Span) error { got = append(got, s.Bytes()...); return nil })
							if !bytes.Equal(got, data[off+innerOff:off+innerOff+innerN]) {
								t.Fatalf("off=%d n=%d inner=%d,%d got=%q", off, n, innerOff, innerN, got)
							}
							cursor := NewCursor(outer)
							if err := cursor.Skip(int64(innerOff)); err != nil {
								t.Fatal(err)
							}
							part, err := cursor.Take(int64(innerN))
							if err != nil {
								t.Fatal(err)
							}
							copyOut := make([]byte, innerN)
							if _, err := part.CopyTo(copyOut); err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(got, copyOut) {
								t.Fatal("cursor range differs")
							}
						}
					}
				}
			}
		})
		t.Run("SegmentedViewsDoNotAllocate", func(t *testing.T) {
			data := make([]byte, 256)
			spans := make([]Span, len(data))
			for i := range data {
				spans[i] = Borrow(data[i : i+1])
			}
			chain, err := NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			allocs := testing.AllocsPerRun(1000, func() {
				view, err := chain.Slice(1, 254)
				if err != nil {
					panic(err)
				}
				cursor := NewCursor(view)
				for cursor.Remaining() >= 7 {
					part, err := cursor.Take(7)
					if err != nil || part.Len() != 7 {
						panic("range")
					}
				}
			})
			if allocs != 0 {
				t.Fatalf("descriptor-only ranges allocate: %v", allocs)
			}
		})
		t.Run("IndexSharedAndOrdered", func(t *testing.T) {
			data := make([]byte, 65536)
			spans := make([]Span, len(data))
			for i := range spans {
				spans[i] = Borrow(data[i : i+1])
			}
			c, _ := NewChain(spans...)
			for i, p := range c.tail {
				if p.end != int64(i+2) {
					t.Fatal("bad prefix index")
				}
			}
			v, e := c.Slice(32000, 10000)
			if e != nil {
				t.Fatal(e)
			}
			if &v.tail[0] != &c.tail[32000] {
				t.Fatal("child copied index")
			}
			for _, off := range []int64{0, 1, 1024, 9998, 9999} {
				part, e := v.Slice(off, 1)
				if e != nil || &part.head.data[0] != &data[32000+off] {
					t.Fatal("indexed range changed backing")
				}
			}
		})
		t.Run("AllBoundaries", func(t *testing.T) {
			data := []byte("abcdefghijk")
			c := chain(t, Borrow(data[:2]), Span{}, Borrow(data[2:3]), Borrow(data[3:8]), Borrow(data[8:]))
			for start := 0; start <= len(data); start++ {
				for end := start; end <= len(data); end++ {
					s, err := c.Slice(int64(start), int64(end-start))
					if err != nil || !bytes.Equal(payload(t, s), data[start:end]) {
						t.Fatalf("Slice(%d,%d): %v", start, end-start, err)
					}
					pos := start
					_ = s.Each(func(span Span) error {
						if &span.Bytes()[0] != &data[pos] {
							t.Fatalf("Slice(%d,%d) copied payload", start, end-start)
						}
						pos += span.Len()
						return nil
					})
				}
			}
			for _, pair := range [][2]int64{{-1, 0}, {0, -1}, {12, 0}, {10, 2}, {math.MaxInt64, 1}, {1, math.MaxInt64}} {
				if _, err := c.Slice(pair[0], pair[1]); !errors.Is(err, ErrBounds) {
					t.Fatalf("Slice%v: %v", pair, err)
				}
			}
			if _, err := addLength(math.MaxInt64, 1); !errors.Is(err, ErrOverflow) {
				t.Fatalf("length overflow: %v", err)
			}
		})
	})
	t.Run("NewChain", func(t *testing.T) {
		a, b := []byte{1, 2}, []byte{3, 4, 5}
		descriptors := []Span{{}, Borrow(a), Borrow(nil), Borrow(b), {}}
		c := chain(t, descriptors...)
		clear(descriptors)
		if c.Len() != 5 || !bytes.Equal(payload(t, c), []byte{1, 2, 3, 4, 5}) {
			t.Fatal("chain did not retain its descriptors")
		}
		index := 0
		if err := c.Each(func(s Span) error {
			want := [][]byte{a, b}[index]
			if s.Len() == 0 || &s.Bytes()[0] != &want[0] {
				t.Fatal("chain copied payload or retained an empty descriptor")
			}
			index++
			return nil
		}); err != nil || index != 2 {
			t.Fatalf("Each: %v, %d spans", err, index)
		}
		stop := errors.New("stop")
		calls := 0
		if err := c.Each(func(Span) error { calls++; return stop }); err != stop || calls != 1 {
			t.Fatal("Each did not stop on visitor error")
		}
		if err := (Chain{}).Each(nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("CopyTo", func(t *testing.T) {
		c := chain(t, BorrowString("abc"), BorrowString("def"))
		for size := 0; size <= 8; size++ {
			dst := bytes.Repeat([]byte{'!'}, size)
			n, err := c.CopyTo(dst)
			want := min(size, 6)
			if n != want || string(dst[:n]) != "abcdef"[:want] {
				t.Fatalf("size %d: copied %d bytes: %q", size, n, dst)
			}
			if size < 6 && !errors.Is(err, io.ErrShortBuffer) || size >= 6 && err != nil {
				t.Fatalf("size %d: %v", size, err)
			}
			if size > 6 && string(dst[6:]) != string(bytes.Repeat([]byte{'!'}, size-6)) {
				t.Fatal("CopyTo overwrote destination tail")
			}
		}
	})
}
