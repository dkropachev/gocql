// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

var step15DynamicSink DynamicRow

var step15ValueSink Value

func BenchmarkStep15DynamicRows(b *testing.B) {
	for _, cells := range []int{1, 32, 128} {
		for _, fragments := range []int{1, 1024} {
			b.Run(fmt.Sprintf("cells%d/fragments%d", cells, fragments), func(b *testing.B) {
				typ := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
				cols := make([]cqltype.ColumnSpec, cells)
				values := make([]Value, cells)
				for i := range cols {
					cols[i] = cqltype.ColumnSpec{Name: fmt.Sprint("v", i), Type: typ}
					values[i] = BlobValue(bytes.Repeat([]byte{'x'}, 32))
				}
				columns := cqltype.NewColumns(cols...)
				raw := cqltype.BytesView(encodeTestParams(b, columns, DynamicParams, values), wire.Ref{}).OwnedBytes()
				spans := make([]wire.Span, fragments)
				for i := range spans {
					spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(spans...))
				plan := CodecTestMust(DynamicRows.Compile(columns))
				var dst DynamicRow
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for b.Loop() {
					reader := NewRowReader(columns, chain, nil)
					if e := plan.DecodeInto(reader, &dst); e != nil {
						b.Fatal(e)
					}
					v, e := dst.At(cells - 1)
					if e != nil {
						b.Fatal(e)
					}
					step15ValueSink = v
				}
				step15DynamicSink = dst
			})
		}
	}
}

// The one-time row-owned physical index is setup here. Its allocation/build cost
// is measured separately by BenchmarkStep15DynamicRows. This measures actual J
// repeated late-column accesses at independently varied N and S.
func BenchmarkStep15DynamicRepeatedAccess(b *testing.B) {
	blob := CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	for _, n := range []int{1, 128, 4096} {
		for _, fragments := range []int{1, 256, 65536} {
			b.Run(fmt.Sprintf("N%d/S%d", n, fragments), func(b *testing.B) {
				cols := make([]cqltype.ColumnSpec, n)
				values := make([]Value, n)
				for i := range cols {
					cols[i] = cqltype.ColumnSpec{Name: fmt.Sprint("v", i), Type: blob}
					values[i] = BlobValue(make([]byte, (1<<20)/n))
				}
				columns := cqltype.NewColumns(cols...)
				raw := cqltype.BytesView(encodeTestParams(b, columns, DynamicParams, values), wire.Ref{}).OwnedBytes()
				parts := make([]wire.Span, fragments)
				for i := range parts {
					parts[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(parts...))
				plan := CodecTestMust(DynamicRows.Compile(columns))
				row := CodecTestMust(plan.Decode(NewRowReader(columns, chain, nil)))
				CodecTestMust(row.At(n - 1))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					v, e := row.At(n - 1)
					if e != nil {
						b.Fatal(e)
					}
					step15ValueSink = v
				}
			})
		}
	}
}
