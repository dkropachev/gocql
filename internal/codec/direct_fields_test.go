// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"encoding/binary"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type directRow struct {
	I8      int8
	I16     int16
	I32     int32
	I64     int64
	Count   Counter
	View    cqltype.Bytes
	Owned   []byte
	Text    string
	ASCII   string
	Blob    []byte
	Skipped int32 // encoded through a codec without a direct op
	Opt     cqltype.Nullable[int32]
}

func directColumns(t testing.TB) cqltype.Columns {
	typ := func(kind cqltype.Type) cqltype.DataType { return CodecTestMust(cqltype.ScalarType(kind)) }
	return cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "i8", Type: typ(cqltype.TypeTinyInt)},
		cqltype.ColumnSpec{Name: "i16", Type: typ(cqltype.TypeSmallInt)},
		cqltype.ColumnSpec{Name: "i32", Type: typ(cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "i64", Type: typ(cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "count", Type: typ(cqltype.TypeCounter)},
		cqltype.ColumnSpec{Name: "view", Type: typ(cqltype.TypeBlob)},
		cqltype.ColumnSpec{Name: "owned", Type: typ(cqltype.TypeBlob)},
		cqltype.ColumnSpec{Name: "text", Type: typ(cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "ascii", Type: typ(cqltype.TypeAscii)},
		cqltype.ColumnSpec{Name: "blob", Type: typ(cqltype.TypeBlob)},
		cqltype.ColumnSpec{Name: "skipped", Type: typ(cqltype.TypeInt)},
	)
}

var directParams = Params(
	Param("i8", Int8, func(r directRow) int8 { return r.I8 }),
	Param("i16", Int16, func(r directRow) int16 { return r.I16 }),
	Param("i32", Int32, func(r directRow) int32 { return r.I32 }),
	Param("i64", Int64, func(r directRow) int64 { return r.I64 }),
	Param("count", CounterCodec, func(r directRow) Counter { return r.Count }),
	Param("view", BlobCodec, func(r directRow) cqltype.Bytes { return r.View }),
	Param("owned", ByteSliceCodec, func(r directRow) []byte { return r.Owned }),
	Param("text", StringCodec, func(r directRow) string { return r.Text }),
	Param("ascii", ASCIIStringCodec, func(r directRow) string { return r.ASCII }),
	Param("blob", ByteSliceCodec, func(r directRow) []byte { return r.Blob }),
	Param("skipped", NullableCodec(Int32), func(r directRow) cqltype.Nullable[int32] {
		return cqltype.Nullable[int32]{Value: r.Skipped, Valid: true}
	}),
)

var directRows = RowInto(func(r *directRow) { r.Owned = r.Owned[:0] },
	Column("i8", Int8, func(r *directRow) *int8 { return &r.I8 }),
	Column("i16", Int16, func(r *directRow) *int16 { return &r.I16 }),
	Column("i32", Int32, func(r *directRow) *int32 { return &r.I32 }),
	Column("i64", Int64, func(r *directRow) *int64 { return &r.I64 }),
	Column("count", CounterCodec, func(r *directRow) *Counter { return &r.Count }),
	Column("view", BlobCodec, func(r *directRow) *cqltype.Bytes { return &r.View }),
	Column("owned", ByteSliceCodec, func(r *directRow) *[]byte { return &r.Owned }),
	IgnoreColumn[directRow]("text"),
	IgnoreColumn[directRow]("ascii"),
	IgnoreColumn[directRow]("blob"),
	Column("skipped", NullableCodec(Int32), func(r *directRow) *cqltype.Nullable[int32] { return &r.Opt }),
)

// interfaceOnly returns plan with every field on its interface path, the
// behaviour a direct op must reproduce.
func interfaceOnlyParams[P any](plan ParamPlan[P]) ParamPlan[P] {
	p := plan.(paramsPlan[P])
	p.ops = make([]paramOp[P], len(p.ordered))
	for i, field := range p.ordered {
		p.ops[i] = paramOp[P]{field: field}
	}
	return p
}
func interfaceOnlyRows[R any](plan RowPlan[R]) RowPlan[R] {
	p := plan.(rowPlan[R])
	p.ops = make([]rowOp[R], len(p.ordered))
	for i, field := range p.ordered {
		p.ops[i] = rowOp[R]{field: field}
	}
	return p
}

type encoded struct {
	bytes []byte
	cells []ParamCell
	err   string
	index int
}

func encodeWith[P any](t *testing.T, plan ParamPlan[P], columns cqltype.Columns, value P, owned bool) encoded {
	t.Helper()
	builder := wire.NewBuilder(16)
	if owned {
		builder.BeginOwned(nil)
	}
	w := NewParamWriter(builder, columns, true)
	SetParamOwnership(w, owned)
	cells := make([]ParamCell, columns.Len())
	CaptureParamCells(w, cells)
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
