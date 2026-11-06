// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing_test

import (
	"encoding/binary"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestRoutingTokenAPI(t *testing.T) {
	t.Run("Hash", func(t *testing.T) {
		t.Run("SegmentedCanonicalBytes", func(t *testing.T) {
			parts := [][]byte{{0xff, 1, 2, 3, 4}, {9, 8, 7}, {0, 0, 0, 42}}
			cells := make([]encodedCell, len(parts))
			for i, p := range parts {
				c, err := wire.NewChain(wire.Borrow(p[:1]), wire.Borrow(p[1:]))
				if err != nil {
					t.Fatal(err)
				}
				cells[i] = encodedCell{State: cql.Present, Body: c}
			}
			for _, indexes := range [][]int{{0}, {2}, {2, 0, 1}, {1, 2}} {
				var materialized []byte
				for _, index := range indexes {
					if len(indexes) > 1 {
						materialized = binary.BigEndian.AppendUint16(materialized, uint16(len(parts[index])))
					}
					materialized = append(materialized, parts[index]...)
					if len(indexes) > 1 {
						materialized = append(materialized, 0)
					}
				}
				got, err := routingToken(cells, indexes)
				if err != nil || !got.Set || got.Value != murmur.Murmur3H1(materialized) {
					t.Fatalf("indexes=%v token=%+v err=%v", indexes, got, err)
				}
			}
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Run("RejectsInvalidIndexWithNullKey", func(t *testing.T) {
			cells := []encodedCell{{State: cql.Null}, {State: cql.Present}}
			if _, err := routingToken(cells, []int{0, 9}); err == nil {
				t.Fatal("null key hid invalid partition index")
			}
			got, err := routingToken(cells, []int{0, 1})
			if err != nil || got.Set {
				t.Fatalf("nullable key token=%+v err=%v", got, err)
			}
			big, _ := wire.NewChain(wire.Borrow(make([]byte, 65536)))
			cells[0] = encodedCell{State: cql.Present, Body: big}
			if _, err := routingToken(cells, []int{0, 1}); err == nil {
				t.Fatal("oversized composite component accepted")
			}
			if got, err := routingToken(cells, []int{0}); err != nil || !got.Set {
				t.Fatalf("single key not subject to composite prefix limit: %v", err)
			}
		})
	})
}
