// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"bytes"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestPartitionKey(t *testing.T) {
	t.Run("Murmur3Token", func(t *testing.T) {
		t.Run("SplitChains", func(t *testing.T) {
			for length := 0; length < 96; length++ {
				input := make([]byte, length)
				for i := range input {
					input[i] = byte(i*131 + 177)
				}
				expected := murmur.Murmur3H1(input)
				if expected == math.MinInt64 {
					expected = math.MaxInt64
				}
				for split := 0; split <= length; split++ {
					chain, err := wire.NewChain(wire.Borrow(input[:split]), wire.Borrow(input[split:]))
					if err != nil {
						t.Fatal(err)
					}
					got, err := Murmur3TokenPartitioner.Token(*NewPartitionKey(PartitionKeyState{chain: chain}))
					if err != nil || got != expected {
						t.Fatalf("length=%d split=%d: got %d,%v want %d", length, split, got, err, expected)
					}
				}
			}
			for _, test := range []struct {
				input    []byte
				expected int64
			}{{[]byte{1, 2}, math.MinInt64}, {[]byte{0x80, 0, 0, 0, 0, 0, 0, 0}, math.MaxInt64}, {[]byte{0, 0, 0, 0, 0, 0, 0, 3, 9}, 3}} {
				var spans []wire.Span
				for i := range test.input {
					spans = append(spans, wire.Borrow(test.input[i:i+1]))
				}
				chain, _ := wire.NewChain(spans...)
				got, err := CDCTokenPartitioner.Token(*NewPartitionKey(PartitionKeyState{chain: chain}))
				if err != nil || got != test.expected {
					t.Fatalf("CDC=%d,%v want %d", got, err, test.expected)
				}
			}
		})
	})

	t.Run("CanonicalStream", func(t *testing.T) {
		first := []byte{1, 2}
		second := []byte{3}
		a, _ := wire.NewChain(wire.Borrow(first))
		b, _ := wire.NewChain(wire.Borrow(second))
		cells := []model.EncodedCell{{State: cqltype.Present, Body: a}, {State: cqltype.Present, Body: b}}
		key, complete, err := InternalNewPartitionKey(cells, []int{1, 0})
		if err != nil || !complete {
			t.Fatal(complete, err)
		}
		var output bytes.Buffer
		err = key.ForEach(func(b cqltype.Bytes) error { _, err := b.WriteTo(&output); return err })
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(output.Bytes(), []byte{0, 1, 3, 0, 0, 2, 1, 2, 0}) {
			t.Fatal(output.Bytes())
		}
		// Input payload spans are reused; the caller's borrowing contract is real.
		first[0] = 9
		output.Reset()
		_ = key.ForEach(func(b cqltype.Bytes) error { _, err := b.WriteTo(&output); return err })
		if output.Bytes()[6] != 9 {
			t.Fatal("partition payload copied")
		}
		cells[0].State = cqltype.Null
		if _, ok, err := InternalNewPartitionKey(cells, []int{0, 1}); err != nil || ok {
			t.Fatal("null key must disable token routing")
		}
		if _, _, err := InternalNewPartitionKey(cells, []int{3}); err == nil {
			t.Fatal("invalid key index accepted")
		}
		large, _ := wire.NewChain(wire.Borrow(make([]byte, 65536)))
		cells[0] = model.EncodedCell{State: cqltype.Present, Body: large}
		if _, _, err := InternalNewPartitionKey(cells, []int{0, 1}); err == nil {
			t.Fatal("oversized composite accepted")
		}
	})
}
