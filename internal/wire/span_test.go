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
	"math"
	"testing"
	"unsafe"
)

func chain(t testing.TB, spans ...Span) Chain {
	t.Helper()
	c, err := NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func payload(t testing.TB, c Chain) []byte {
	t.Helper()
	b := make([]byte, c.Len())
	if n, err := c.CopyTo(b); err != nil || int64(n) != c.Len() {
		t.Fatalf("CopyTo: %d, %v; length %d", n, err, c.Len())
	}
	return b
}

var allocationChain Chain
var allocationSpan Span

func TestSpan(t *testing.T) {
	t.Run("Borrow", func(t *testing.T) {
		data := []byte{1, 2, 3, 4}
		s := Borrow(data)
		if s.Len() != 4 || &s.Bytes()[0] != &data[0] || cap(s.Bytes()) != 4 {
			t.Fatal("Borrow did not retain a bounded view of the original bytes")
		}
		sub, err := s.Slice(1, 2)
		if err != nil || sub.Len() != 2 || &sub.Bytes()[0] != &data[1] || cap(sub.Bytes()) != 2 {
			t.Fatalf("Slice did not preserve payload identity: %v", err)
		}
		for _, pair := range [][2]int{{-1, 0}, {0, -1}, {5, 0}, {3, 2}, {math.MaxInt, 1}, {1, math.MaxInt}} {
			if _, err := s.Slice(pair[0], pair[1]); !errors.Is(err, ErrBounds) {
				t.Fatalf("Slice%v: %v", pair, err)
			}
		}
		if empty, err := s.Slice(4, 0); err != nil || empty.Len() != 0 {
			t.Fatalf("end slice: %v", err)
		}
	})
	t.Run("BorrowString", func(t *testing.T) {
		for _, original := range []string{"", "literal storage", string(bytes.Repeat([]byte{42}, 256))} {
			s := BorrowString(original)
			if s.Len() != len(original) || string(s.Bytes()) != original {
				t.Fatal("incorrect string view")
			}
			if len(original) > 0 && unsafe.SliceData(s.Bytes()) != unsafe.StringData(original) {
				t.Fatal("BorrowString copied string storage")
			}
			b := NewBuilder(4)
			mark := b.Mark()
			if err := b.Borrow(s); err != nil {
				t.Fatal(err)
			}
			if len(original) >= 4 && !errors.Is(b.PatchUint32(mark, 123), ErrBorrowedPatch) {
				t.Fatal("builder allowed patching string storage")
			}
			frozen, err := b.Freeze()
			if err != nil || string(payload(t, frozen)) != original {
				t.Fatalf("builder changed string storage: %v", err)
			}
		}
	})
	t.Run("SmallViewsDoNotAllocate", func(t *testing.T) {
		data := []byte("some existing payload")
		if n := testing.AllocsPerRun(1000, func() {
			s := Borrow(data)
			c, _ := NewChain(s)
			allocationChain, _ = c.Slice(2, 8)
			allocationSpan, _ = s.Slice(2, 8)
		}); n != 0 {
			t.Fatalf("small borrowed views allocated %v times", n)
		}
		if n := testing.AllocsPerRun(1000, func() { allocationSpan = BorrowString("read-only literal") }); n != 0 {
			t.Fatalf("string view allocated %v times", n)
		}
	})
}
