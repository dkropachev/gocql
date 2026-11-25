// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/binary"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type directRow struct {
	I8      int8
	I16     int16
	I32     int32
	I64     int64
	Count   int64
	View    Bytes
	Owned   []byte
	Text    string
	ASCII   string
	Blob    []byte
	Skipped int32 // encoded through a codec without a direct op
	Opt     Nullable[int32]
}

func directColumns(t testing.TB) Columns {
	typ := func(kind Type) DataType { return CodecTestMust(ScalarType(kind)) }
	return NewColumns(
		ColumnSpec{Name: "i8", Type: typ(TypeTinyInt)},
		ColumnSpec{Name: "i16", Type: typ(TypeSmallInt)},
		ColumnSpec{Name: "i32", Type: typ(TypeInt)},
		ColumnSpec{Name: "i64", Type: typ(TypeBigInt)},
		ColumnSpec{Name: "count", Type: typ(TypeCounter)},
		ColumnSpec{Name: "view", Type: typ(TypeBlob)},
		ColumnSpec{Name: "owned", Type: typ(TypeBlob)},
		ColumnSpec{Name: "text", Type: typ(TypeText)},
		ColumnSpec{Name: "ascii", Type: typ(TypeASCII)},
		ColumnSpec{Name: "blob", Type: typ(TypeBlob)},
		ColumnSpec{Name: "skipped", Type: typ(TypeInt)},
	)
}

var directParams = Params(
	Param("i8", Int8Codec, func(r directRow) int8 { return r.I8 }),
	Param("i16", Int16Codec, func(r directRow) int16 { return r.I16 }),
	Param("i32", Int32Codec, func(r directRow) int32 { return r.I32 }),
	Param("i64", Int64Codec, func(r directRow) int64 { return r.I64 }),
	Param("count", Int64Codec, func(r directRow) int64 { return r.Count }),
	Param("view", BytesCodec, func(r directRow) Bytes { return r.View }),
	Param("owned", ByteSliceCodec, func(r directRow) []byte { return r.Owned }),
	Param("text", StringCodec, func(r directRow) string { return r.Text }),
	Param("ascii", StringCodec, func(r directRow) string { return r.ASCII }),
	Param("blob", ByteSliceCodec, func(r directRow) []byte { return r.Blob }),
	Param("skipped", NullableCodec(Int32Codec), func(r directRow) Nullable[int32] {
		return Nullable[int32]{Value: r.Skipped, Valid: true}
	}),
)

var directRows = RowInto(func(r *directRow) { r.Owned = r.Owned[:0] },
	Column("i8", Int8Codec, func(r *directRow) *int8 { return &r.I8 }),
	Column("i16", Int16Codec, func(r *directRow) *int16 { return &r.I16 }),
	Column("i32", Int32Codec, func(r *directRow) *int32 { return &r.I32 }),
	Column("i64", Int64Codec, func(r *directRow) *int64 { return &r.I64 }),
	Column("count", Int64Codec, func(r *directRow) *int64 { return &r.Count }),
	Column("view", BytesCodec, func(r *directRow) *Bytes { return &r.View }),
	Column("owned", ByteSliceCodec, func(r *directRow) *[]byte { return &r.Owned }),
	IgnoreColumn[directRow]("text"),
	IgnoreColumn[directRow]("ascii"),
	IgnoreColumn[directRow]("blob"),
	Column("skipped", NullableCodec(Int32Codec), func(r *directRow) *Nullable[int32] { return &r.Opt }),
)

// interfaceOnly returns plan with every field on its interface path, the
// behaviour a direct op must reproduce.
func interfaceOnlyParams[P any](plan CompiledParams[P]) CompiledParams[P] {
	p := plan.(compiledParams[P])
	p.ops = make([]paramOp[P], len(p.ordered))
	for i, field := range p.ordered {
		p.ops[i] = paramOp[P]{field: field}
	}
	return p
}
func interfaceOnlyRows[R any](plan CompiledRow[R]) CompiledRow[R] {
	p := plan.(compiledRow[R])
	p.ops = make([]rowOp[R], len(p.ordered))
	for i, field := range p.ordered {
		p.ops[i] = rowOp[R]{field: field}
	}
	return p
}

type encoded struct {
	bytes []byte
	cells []paramCell
	err   string
	index int
}

func encodeWith[P any](t *testing.T, plan CompiledParams[P], columns Columns, value P, owned bool) encoded {
	t.Helper()
	builder := wire.NewBuilder(16)
	if owned {
		builder.BeginOwned(nil)
	}
	w := newParamWriter(builder, columns, true)
	setParamOwnership(w, owned)
	cells := make([]paramCell, columns.Len())
	captureParamCells(w, cells)
	var out encoded
	if err := plan.Encode(w, value); err != nil {
		out.err = err.Error()
	}
	out.index = w.index
	chain, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	out.bytes = make([]byte, chain.Len())
	if _, err := chain.CopyTo(out.bytes); err != nil {
		t.Fatal(err)
	}
	out.cells = cells
	return out
}

// cell appends one framed cell: a body, or a null with length -1.
func cell(dst []byte, body []byte, null bool) []byte {
	if null {
		return binary.BigEndian.AppendUint32(dst, 0xffffffff)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(body)))
	return append(dst, body...)
}
