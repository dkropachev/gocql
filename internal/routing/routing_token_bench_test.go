// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func BenchmarkRoutingTokenAPI(b *testing.B) {
	b.Run("CompositeSpans", func(b *testing.B) {
		data := make([]byte, 4096)
		chain, _ := wire.NewChain(wire.Borrow(data[:15]), wire.Borrow(data[15:]))
		cells := []encodedCell{{State: cql.Present, Body: chain}, {State: cql.Present, Body: chain}}
		indexes := []int{0, 1}
		b.ReportAllocs()
		b.SetBytes(8192)
		for i := 0; i < b.N; i++ {
			if _, err := routingToken(cells, indexes); err != nil {
				b.Fatal(err)
			}
		}
	})
}
