// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"

	"fmt"

	"github.com/scylladb/gocql/v2/internal/wire"

	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type fusedBenchRow struct {
	Key, Row int32
	Blob     cqltype.Bytes
}

type wideRow struct {
	Numbers [32]int32
	Texts   [32]cqltype.Text
	Blobs   [32]cqltype.Bytes
	Lists   [32]Sequence[int32]
}

func BenchmarkRowsCodec(b *testing.B) {
	b.Run("DestinationFragments", func(b *testing.B) {
		type row struct {
			Number  int32
			Payload cqltype.Bytes
			Pad     [256]int64
		}
		integer, blob := cqltype.IntType, cqltype.BlobType
		cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "number", Type: integer}, cqltype.ColumnSpec{Name: "payload", Type: blob})
		decoder := Row(Column("number", Int32, func(r *row) *int32 { return &r.Number }), Column("payload", BlobCodec, func(r *row) *cqltype.Bytes { return &r.Payload }))
		plan := CodecTestMust(decoder.Compile(cols))
		payload := bytes.Repeat([]byte{0x31}, 512000)
		framed := append(streamEncodedFixture(b, integer, Int32, int32(1)), streamEncodedFixture(b, blob, BytesEncoder, payload)...)
		for _, fragments := range []int{1, 1024, 65536} {
			parts := make([]wire.Span, 0, fragments)
			for start := 0; start < len(framed); {
				end := min(len(framed), start+(len(framed)+fragments-1)/fragments)
				parts = append(parts, wire.Borrow(framed[start:end]))
				start = end
			}
			chain := CodecTestMust(wire.NewChain(parts...))
			for _, direct := range []bool{false, true} {
				b.Run(fmt.Sprintf("fragments=%d/into=%v", fragments, direct), func(b *testing.B) {
					var cursor wire.Cursor
					reader := RowReader{columns: cols, cursor: &cursor}
					var dst row
					cursor = *wire.NewCursor(chain)
					if err := plan.DecodeInto(&reader, &dst); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						cursor = *wire.NewCursor(chain)
						reader.index = 0
						if direct {
							if err := plan.DecodeInto(&reader, &dst); err != nil {
								b.Fatal(err)
							}
						} else {
							value, err := plan.Decode(&reader)
							if err != nil {
								b.Fatal(err)
							}
							dst = value
						}
					}
				})
			}
		}
	})
	// BenchmarkRowsCodec/DirectFields encodes and decodes a ten-column row with built-in
	// codecs, through direct ops and through the fields' interfaces.
	b.Run("DirectFields", func(b *testing.B) {
		columns := directColumns(b)
		params := CodecTestMust(directParams.Compile(columns))
		rows := CodecTestMust(directRows.Compile(columns))
		row := directRow{I8: 1, I16: 2, I32: 3, I64: 4, Count: 5, View: cqltype.BorrowBytes([]byte("view")), Owned: []byte("owned"), Text: "text", ASCII: "ascii", Blob: []byte("blob"), Skipped: 6}
		builder := wire.NewBuilder(256)
		w := NewParamWriter(builder, columns, true)
		if err := params.Encode(w, row); err != nil {
			b.Fatal(err)
		}
		body, _ := builder.Freeze()
		for _, mode := range []string{"direct", "interface"} {
			p, r := params, rows
			if mode == "interface" {
				p, r = interfaceOnlyParams(params), interfaceOnlyRows(rows)
			}
			b.Run("encode/"+mode, func(b *testing.B) {
				var storage []byte
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					builder.BeginOwned(storage)
					ResetParamWriter(w, builder, columns, true)
					if err := p.Encode(w, row); err != nil {
						b.Fatal(err)
					}
					storage = builder.OwnedBytes()[:0]
				}
			})
			b.Run("decode/"+mode, func(b *testing.B) {
				dst := directRow{Owned: make([]byte, 0, 16)}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := r.DecodeInto(NewRowReader(columns, body, nil), &dst); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
	b.Run("FusedRowPaged", func(b *testing.B) {
		columns := cqltype.NewColumns(
			cqltype.ColumnSpec{Name: "key", Type: rowTestType(b, cqltype.TypeInt)},
			cqltype.ColumnSpec{Name: "row", Type: rowTestType(b, cqltype.TypeInt)},
			cqltype.ColumnSpec{Name: "blob", Type: rowTestType(b, cqltype.TypeBlob)},
		)
		var data []byte
		for i := 0; i < 32; i++ {
			data = append(data, rowScalarFrame(4, []byte{0, 0, 0, 42})...)
			data = append(data, rowScalarFrame(4, []byte{0, 0, 0, byte(i)})...)
			data = append(data, rowScalarFrame(4096, make([]byte, 4096))...)
		}
		chain, _ := wire.NewChain(wire.Borrow(data))
		lease := wire.NewLease(chain, nil)
		defer lease.Release()
		for _, direct := range []bool{false, true} {
			name := "generic"
			if direct {
				name = "fused"
			}
			b.Run(name, func(b *testing.B) {
				var integer Decoder[int32] = Int32
				var blob Decoder[cqltype.Bytes] = BlobCodec
				if !direct {
					integer = genericRowScalar[int32]{integer}
					blob = genericRowScalar[cqltype.Bytes]{blob}
				}
				plan, err := Row(
					Column("key", integer, func(r *fusedBenchRow) *int32 { return &r.Key }),
					Column("row", integer, func(r *fusedBenchRow) *int32 { return &r.Row }),
					Column("blob", blob, func(r *fusedBenchRow) *cqltype.Bytes { return &r.Blob }),
				).Compile(columns)
				if err != nil {
					b.Fatal(err)
				}
				var dst fusedBenchRow
				b.ReportAllocs()
				b.SetBytes(32 * 4096)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					reader := NewRowReader(columns, chain, lease)
					for row := 0; row < 32; row++ {
						reader.index = 0
						if err := plan.DecodeInto(reader, &dst); err != nil {
							b.Fatal(err)
						}
					}
					if err := reader.cursor.RequireEnd(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
	b.Run("NarrowWideRows", func(b *testing.B) {
		integer := cqltype.IntType
		text := cqltype.TextType
		blob := cqltype.BlobType
		list := CodecTestMust(cqltype.ListType(integer))
		for _, groups := range []int{1, 8, 32} {
			for _, fragments := range []int{1, 1024} {
				fields := make([]OutputField[wideRow], 0, groups*4)
				specs := make([]cqltype.ColumnSpec, 0, groups*4)
				values := make([]Value, 0, groups*4)
				for i := 0; i < groups; i++ {
					index := i
					names := [4]string{fmt.Sprint("n", i), fmt.Sprint("t", i), fmt.Sprint("b", i), fmt.Sprint("l", i)}
					specs = append(specs, cqltype.ColumnSpec{Name: names[0], Type: integer}, cqltype.ColumnSpec{Name: names[1], Type: text}, cqltype.ColumnSpec{Name: names[2], Type: blob}, cqltype.ColumnSpec{Name: names[3], Type: list})
					fields = append(fields, Column(names[0], Int32, func(r *wideRow) *int32 { return &r.Numbers[index] }), Column(names[1], TextCodec, func(r *wideRow) *cqltype.Text { return &r.Texts[index] }), Column(names[2], BlobCodec, func(r *wideRow) *cqltype.Bytes { return &r.Blobs[index] }), Column(names[3], ListDecoder(Int32), func(r *wideRow) *Sequence[int32] { return &r.Lists[index] }))
					values = append(values, IntValue(int32(i)), CodecTestMust(TextValue("safe-🙂")), BlobValue(make([]byte, 4096)), CodecTestMust(ListValue(list, []Value{IntValue(1), IntValue(2)})))
				}
				columns := cqltype.NewColumns(specs...)
				decoder := Row(fields...)
				plan := CodecTestMust(decoder.Compile(columns))
				raw := cqltype.BytesView(encodeTestParams(b, columns, DynamicParams, values), wire.Ref{}).OwnedBytes()
				spans := make([]wire.Span, fragments)
				for i := range spans {
					spans[i] = wire.Borrow(raw[len(raw)*i/fragments : len(raw)*(i+1)/fragments])
				}
				chain := CodecTestMust(wire.NewChain(spans...))
				lease := wire.NewLease(chain, nil)
				for _, into := range []bool{false, true} {
					b.Run(fmt.Sprintf("columns%d/fragments%d/into%t", 4*groups, fragments, into), func(b *testing.B) {
						var cursor wire.Cursor
						reader := RowReader{columns: columns, cursor: &cursor, lease: wire.RefOf(lease)}
						var dst wideRow
						cursor = *wire.NewCursor(chain)
						if e := plan.DecodeInto(&reader, &dst); e != nil {
							b.Fatal(e)
						}
						b.ReportAllocs()
						b.SetBytes(int64(len(raw)))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							cursor = *wire.NewCursor(chain)
							reader.index = 0
							if into {
								if e := plan.DecodeInto(&reader, &dst); e != nil {
									b.Fatal(e)
								}
							} else {
								value, e := plan.Decode(&reader)
								if e != nil {
									b.Fatal(e)
								}
								dst = value
							}
							if dst.Numbers[groups-1] != int32(groups-1) || dst.Blobs[groups-1].Len() != 4096 {
								b.Fatal("row")
							}
						}
					})
				}
				lease.Release()
			}
		}
	})
	b.Run("ScalarPlan", func(b *testing.B) {
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: rowTestType(b, cqltype.TypeInt)})
		plan, err := OneColumn(Int32).Compile(columns)
		if err != nil {
			b.Fatal(err)
		}
		chain := encodeTestParams(b, columns, OneParam(Int32), int32(42))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := plan.Decode(NewRowReader(columns, chain, nil))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
