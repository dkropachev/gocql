// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// FuzzDirectRowsMatchCodecs decodes arbitrary, possibly corrupt row bodies,
// cut into arbitrary spans, through the direct-op plan and a reference plan on
// the generic ValueReader path, and requires the same row, error, column index and cursor
// position, with a live and with a released lease. It gates fast paths in
// Cursor scalars and RowReader.scalarColumn: they must decline every cell
// whose slow path would report an error or cross a span boundary.
func FuzzDirectRowsMatchCodecs(f *testing.F) {
	full := func(lengths ...int32) []byte {
		bodies := [][]byte{{0xf8}, {0xf9, 0xc0}, {0xff, 0xfb, 0x1e, 0x00}, {0xff, 0xff, 0xc0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 7}, []byte("view"), []byte("owned"), []byte("t"), []byte("a"), {1}, {0, 0, 0, 9}}
		var out []byte
		for i, b := range bodies {
			out = cell(out, b, i < len(lengths) && lengths[i] < 0)
		}
		return out
	}
	f.Add(full(), uint8(0), false)
	f.Add(full(), uint8(2), false)
	f.Add(full(1, 1, -1), uint8(4), false)
	f.Add(full(), uint8(0), true)
	f.Add(full()[:20], uint8(5), false)
	f.Add(full()[:17], uint8(0), false)                                           // a complete prefix with a truncated body
	f.Add(full()[:9], uint8(0), false)                                            // the prefix ends exactly on the body end
	f.Add(full(0, 0, 0, 0, 0, -1), uint8(0), false)                               // a null blob
	f.Add(append(cell(nil, []byte{1, 2}, false), full()[5:]...), uint8(0), false) // a tinyint of the wrong width
	columns := directColumns(f)
	plan := CodecTestMust(directRows.Compile(columns))
	// The reference decodes every cell through the generic ValueReader path
	// (nextValue then Decode), which shares no code with scalarColumn.
	reference := CodecTestMust(RowInto(func(r *directRow) { r.Owned = r.Owned[:0] },
		Column("i8", Decoder[int8](genericRowScalar[int8]{Int8}), func(r *directRow) *int8 { return &r.I8 }),
		Column("i16", Decoder[int16](genericRowScalar[int16]{Int16}), func(r *directRow) *int16 { return &r.I16 }),
		Column("i32", Decoder[int32](genericRowScalar[int32]{Int32}), func(r *directRow) *int32 { return &r.I32 }),
		Column("i64", Decoder[int64](genericRowScalar[int64]{Int64}), func(r *directRow) *int64 { return &r.I64 }),
		Column("count", Decoder[Counter](genericRowScalar[Counter]{CounterCodec}), func(r *directRow) *Counter { return &r.Count }),
		Column("view", Decoder[cqltype.Bytes](genericRowScalar[cqltype.Bytes]{BlobCodec}), func(r *directRow) *cqltype.Bytes { return &r.View }),
		Column("owned", Decoder[[]byte](genericRowScalar[[]byte]{ByteSliceCodec}), func(r *directRow) *[]byte { return &r.Owned }),
		IgnoreColumn[directRow]("text"),
		IgnoreColumn[directRow]("ascii"),
		IgnoreColumn[directRow]("blob"),
		Column("skipped", NullableCodec(Int32), func(r *directRow) *cqltype.Nullable[int32] { return &r.Opt }),
	).Compile(columns))
	f.Fuzz(func(t *testing.T, body []byte, width uint8, released bool) {
		var spans []wire.Span
		for rest, i := body, 0; len(rest) > 0; i++ {
			n := len(rest)
			if width > 0 {
				n = min(int(width)+i%3, len(rest))
			}
			spans = append(spans, wire.Borrow(rest[:n]))
			rest = rest[n:]
		}
		chain, err := wire.NewChain(spans...)
		if err != nil {
			t.Fatal(err)
		}
		// Both plans share one lease, so borrowed views print identically.
		lease := wire.NewLease(chain, nil)
		if released {
			lease.Release()
		} else {
			defer lease.Release()
		}
		run := func(p RowPlan[directRow]) string {
			dst := directRow{Owned: make([]byte, 0, 64)}
			reader := NewRowReader(columns, chain, lease)
			err := p.DecodeInto(reader, &dst)
			if err == nil {
				err = FinishRowReader(reader)
			}
			return fmt.Sprintf("%+v|%v|%d|%d", dst, err, reader.index, reader.cursor.Remaining())
		}
		if got, want := run(plan), run(reference); got != want {
			t.Fatalf("direct and reference plans differ:\n got %s\nwant %s", got, want)
		}
	})
}
