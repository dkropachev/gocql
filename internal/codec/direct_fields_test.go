// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
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
	Param("owned", BytesEncoder, func(r directRow) []byte { return r.Owned }),
	Param("text", StringEncoder, func(r directRow) string { return r.Text }),
	Param("ascii", ASCIIEncoder, func(r directRow) string { return r.ASCII }),
	Param("blob", BytesEncoder, func(r directRow) []byte { return r.Blob }),
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
	Column("owned", OwnedBlobCodec, func(r *directRow) *[]byte { return &r.Owned }),
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

// Built-in codecs compile into direct ops that write the same cells,
// framing captures and errors as their codecs, owned or borrowed.
func TestDirectParamOpsMatchCodecs(t *testing.T) {
	columns := directColumns(t)
	plan := CodecTestMust(directParams.Compile(columns))
	want := []directKind{directInt, directInt, directInt, directInt, directInt, directNone, directBytes, directString, directString, directBytes, directNone}
	for i, op := range plan.(paramsPlan[directRow]).ops {
		if op.kind != want[i] {
			t.Fatalf("parameter %d: op kind %d, want %d", i, op.kind, want[i])
		}
	}
	reference := interfaceOnlyParams(plan)
	view := cqltype.BorrowBytes([]byte("view"))
	rows := map[string]directRow{
		"values":  {I8: -8, I16: -1600, I32: -320000, I64: -64 << 40, Count: 7, View: view, Owned: []byte("owned"), Text: "tëxt", ASCII: "ascii", Blob: []byte{0, 1, 2}, Skipped: 9},
		"zero":    {View: view},
		"utf8":    {View: view, Text: string([]byte{0xff, 0xfe})},
		"ascii":   {View: view, ASCII: "ä"},
		"extreme": {I8: -128, I16: 32767, I32: -1 << 31, I64: 1<<63 - 1, Count: -1, View: view, Blob: make([]byte, 300)},
	}
	for name, row := range rows {
		for _, owned := range []bool{false, true} {
			got := encodeWith(t, plan, columns, row, owned)
			ref := encodeWith(t, reference, columns, row, owned)
			if !bytes.Equal(got.bytes, ref.bytes) || !reflect.DeepEqual(got.cells, ref.cells) || got.err != ref.err || got.index != ref.index {
				t.Fatalf("%s owned=%v:\n got %x %v %q %d\nwant %x %v %q %d", name, owned, got.bytes, got.cells, got.err, got.index, ref.bytes, ref.cells, ref.err, ref.index)
			}
		}
	}
	if got := encodeWith(t, plan, columns, rows["utf8"], true); got.err == "" {
		t.Fatal("invalid UTF-8 was encoded")
	}
}

// cell appends one framed cell: a body, or a null with length -1.
func cell(dst []byte, body []byte, null bool) []byte {
	if null {
		return binary.BigEndian.AppendUint32(dst, 0xffffffff)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(body)))
	return append(dst, body...)
}

// Built-in codecs compile into direct ops that decode the same values and
// fail with the same errors as their codecs, keeping the owned blob's
// capacity.
func TestDirectRowOpsMatchCodecs(t *testing.T) {
	columns := directColumns(t)
	plan := CodecTestMust(directRows.Compile(columns))
	want := []directKind{directInt, directInt, directInt, directInt, directInt, directBlob, directOwnedBlob, directNone, directNone, directNone, directNone}
	for i, op := range plan.(rowPlan[directRow]).ops {
		if op.kind != want[i] {
			t.Fatalf("column %d: op kind %d, want %d", i, op.kind, want[i])
		}
	}
	reference := interfaceOnlyRows(plan)
	full := func(edit func(i int, b []byte) ([]byte, bool)) []byte {
		bodies := [][]byte{{0xf8}, {0xf9, 0xc0}, {0xff, 0xfb, 0x1e, 0x00}, {0xff, 0xff, 0xc0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 7}, []byte("view"), []byte("owned"), []byte("t"), []byte("a"), {1}, {0, 0, 0, 9}}
		var out []byte
		for i, b := range bodies {
			null := false
			if edit != nil {
				b, null = edit(i, b)
			}
			out = cell(out, b, null)
		}
		return out
	}
	cases := map[string][]byte{
		"values":    full(nil),
		"null int":  full(func(i int, b []byte) ([]byte, bool) { return b, i == 2 }),
		"short int": full(func(i int, b []byte) ([]byte, bool) { return map[bool][]byte{true: {1, 2}, false: b}[i == 3], false }),
		"null blob": full(func(i int, b []byte) ([]byte, bool) { return b, i == 6 }),
		"empty blobs": full(func(i int, b []byte) ([]byte, bool) {
			return map[bool][]byte{true: {}, false: b}[i == 5 || i == 6], false
		}),
		"truncated": full(nil)[:20],
	}
	for name, body := range cases {
		chain, _ := wire.NewChain(wire.Borrow(body))
		run := func(p RowPlan[directRow]) (directRow, string, int) {
			dst := directRow{Owned: make([]byte, 0, 64)}
			reader := NewRowReader(columns, chain, nil)
			err := p.DecodeInto(reader, &dst)
			msg := ""
			if err != nil {
				msg = err.Error()
			}
			return dst, msg, reader.index
		}
		got, gotErr, gotIndex := run(plan)
		ref, refErr, refIndex := run(reference)
		if gotErr != refErr || gotIndex != refIndex || fmt.Sprint(got) != fmt.Sprint(ref) {
			t.Fatalf("%s:\n got %+v %q %d\nwant %+v %q %d", name, got, gotErr, gotIndex, ref, refErr, refIndex)
		}
		if name == "values" && (gotErr != "" || got.I32 != -320000 || got.Count != 7 || string(got.Owned) != "owned" || cap(got.Owned) != 64) {
			t.Fatalf("values decoded as %+v (owned capacity %d)", got, cap(got.Owned))
		}
	}
}

// A writer that owns its input copies blobs into the frame even on a
// borrowing builder, as BytesEncoder does, so the caller may reuse them.
func TestDirectParamOpsCopyOwnedInput(t *testing.T) {
	columns := directColumns(t)
	plan := CodecTestMust(directParams.Compile(columns))
	for _, p := range []ParamPlan[directRow]{plan, interfaceOnlyParams(plan)} {
		row := directRow{View: cqltype.BorrowBytes([]byte("view")), Owned: []byte("owned"), Blob: []byte("blob")}
		builder := wire.NewBuilder(16)
		w := NewParamWriter(builder, columns, true)
		SetParamOwnership(w, true)
		if err := p.Encode(w, row); err != nil {
			t.Fatal(err)
		}
		chain, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		copy(row.Owned, "XXXXX")
		copy(row.Blob, "YYYY")
		out := make([]byte, chain.Len())
		if _, err := chain.CopyTo(out); err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(out, []byte("owned")) || !bytes.Contains(out, []byte("blob")) {
			t.Fatalf("owned input was borrowed: %q", out)
		}
	}
}
