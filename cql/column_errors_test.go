// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// wantColumnError fails t unless err is a CodecError for column index at
// stage.
func wantColumnError(t *testing.T, err error, stage CodecStage, index int, column string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want a %s error for column %s", stage, column)
	}
	codecErr, ok := errors.AsType[*CodecError](err)
	if !ok {
		t.Fatalf("error %q (%T) is not a CodecError", err, err)
	}
	if codecErr.Stage != stage || codecErr.Index != index || codecErr.Column != column {
		t.Fatalf("error %q: got stage %q column %d (%s), want stage %q column %d (%s)",
			err, codecErr.Stage, codecErr.Index, codecErr.Column, stage, index, column)
	}
}

func matrixType(kind Kind) DataType { return mustMatrixType(ScalarType(kind)) }

func mustMatrixType(t DataType, err error) DataType {
	if err != nil {
		panic(err)
	}
	return t
}

// TestColumnErrorPaths checks that failures inside a typed plan, a struct
// mapping, a custom plan and a tuple or UDT member name the outer column,
// with the member's path.
func TestColumnErrorPaths(t *testing.T) {
	intType, textType := matrixType(KindInt), matrixType(KindText)
	t.Run("Struct", func(t *testing.T) {
		type record struct {
			ID   int32
			Name string
		}
		columns := NewColumns(ColumnSpec{Name: "name", Type: textType}, ColumnSpec{Name: "id", Type: intType})
		rows := MustRowOf[record]()
		chain := matrixRaw([]byte("x"), nil)
		plan, err := rows.Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRow(plan, newRowReader(columns, chain, nil))
		wantColumnError(t, err, "decode", 1, "id")

		wrong := NewColumns(ColumnSpec{Name: "name", Type: textType}, ColumnSpec{Name: "id", Type: textType})
		_, err = rows.Compile(wrong)
		wantColumnError(t, err, "compile", 1, "id")
		_, err = MustParamsOf[record]().Compile(wrong)
		wantColumnError(t, err, "compile", 1, "id")

		params, err := MustParamsOf[record]().Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		err = params.Encode(newParamWriter(wire.NewBuilder(8), columns, false), record{Name: "\xff"})
		wantColumnError(t, err, "encode", 0, "name")
	})
	t.Run("Unmapped", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "a", Type: intType}, ColumnSpec{Name: "b", Type: intType})
		_, err := Row(Column("a", Int32Codec, func(v *int32) *int32 { return v })).Compile(columns)
		wantColumnError(t, err, "compile", 1, "b")
	})
	t.Run("CustomPlan", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "a", Type: intType}, ColumnSpec{Name: "b", Type: intType})
		chain := matrixRaw([]byte{0, 0, 0, 1}, nil)
		r := newRowReader(columns, chain, nil)
		if _, err := r.ReadColumn(Int32Codec); err != nil {
			t.Fatal(err)
		}
		_, err := r.ReadColumn(Int32Codec)
		wantColumnError(t, err, "decode", 1, "b")
	})
	t.Run("CAS", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "[applied]", Type: matrixType(KindBoolean)}, ColumnSpec{Name: "v", Type: intType})
		chain := matrixRaw([]byte{0}, []byte{1, 2, 3})
		plan, err := CASRows(OneColumn(Int32Codec)).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRow(plan, newRowReader(columns, chain, nil))
		wantColumnError(t, err, "decode", 1, "v")
		_, err = CASRows(OneColumn(StringCodec)).Compile(columns)
		wantColumnError(t, err, "compile", 1, "v")
	})
	t.Run("Tuple", func(t *testing.T) {
		tupleType := mustMatrixType(TupleType(intType, textType))
		columns := NewColumns(ColumnSpec{Name: "k", Type: intType}, ColumnSpec{Name: "t", Type: tupleType})
		// t = (NULL, "x")
		chain := matrixRaw([]byte{0, 0, 0, 1}, []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1, 'x'})
		decoder := TupleDecoder(Row(
			ColumnAt(0, Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First }),
			ColumnAt(1, StringCodec, func(p *Pair[int32, string]) *string { return &p.Second })))
		plan, err := PairRows(OneColumn(Int32Codec), OneColumn(decoder)).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRow(plan, newRowReader(columns, chain, nil))
		wantColumnError(t, err, "decode", 1, "t")
		if e, _ := errors.AsType[*CodecError](err); e.Path != "[0]" {
			t.Fatalf("path %q, want [0]", e.Path)
		}
		wrong := TupleDecoder(Row(
			ColumnAt(0, Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First }),
			ColumnAt(1, Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First })))
		_, err = PairRows(OneColumn(Int32Codec), OneColumn(wrong)).Compile(columns)
		wantColumnError(t, err, "compile", 1, "t")
		if e, _ := errors.AsType[*CodecError](err); e.Path != "[1]" {
			t.Fatalf("path %q, want [1]", e.Path)
		}
	})
	t.Run("PairParams", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "a", Type: intType}, ColumnSpec{Name: "b", Type: textType})
		plan, err := PairParams(OneParam(Int32Codec), OneParam(StringCodec)).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		err = plan.Encode(newParamWriter(wire.NewBuilder(8), columns, false), Pair[int32, string]{Second: "\xff"})
		wantColumnError(t, err, "encode", 1, "b")
		_, err = PairParams(OneParam(Int32Codec), OneParam(Int32Codec)).Compile(columns)
		wantColumnError(t, err, "compile", 1, "b")
	})
	t.Run("Stream", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "a", Type: textType})
		_, err := StreamOneColumn(StreamInt32).Compile(columns)
		wantColumnError(t, err, "stream compile", 0, "a")
		plan, err := StreamOneColumn(StreamText).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		// A negative length and a body shorter than its length. A failed
		// read of the length itself is not yet the column's failure.
		for _, data := range [][]byte{{0xff, 0xff, 0xff, 0xfe}, {0, 0, 0, 9, 'x'}} {
			r, _ := streamReaderFixture(t, columns, &streamChunkSource{data: data}, len(data))
			var dst []byte
			wantColumnError(t, plan.DecodeInto(r, &dst), "stream decode", 0, "a")
		}
	})
	t.Run("DynamicCompile", func(t *testing.T) {
		deep := intType
		for range 130 { // past the 128 levels a dynamic type may nest
			deep = mustMatrixType(ListType(deep))
		}
		columns := NewColumns(ColumnSpec{Name: "a", Type: intType}, ColumnSpec{Name: "b", Type: deep})
		_, err := DynamicRows.Compile(columns)
		wantColumnError(t, err, "compile", 1, "b")
		_, err = DynamicParams.Compile(columns)
		wantColumnError(t, err, "compile", 1, "b")
	})
	t.Run("MalformedTupleMember", func(t *testing.T) {
		tupleType := mustMatrixType(TupleType(intType, textType))
		columns := NewColumns(ColumnSpec{Name: "t", Type: tupleType})
		// t's second member claims 9 bytes but has 1.
		chain := matrixRaw([]byte{0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 9, 'x'})
		decoder := TupleDecoder(Row(
			ColumnAt(0, Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First }),
			ColumnAt(1, StringCodec, func(p *Pair[int32, string]) *string { return &p.Second })))
		plan, err := OneColumn(decoder).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRow(plan, newRowReader(columns, chain, nil))
		wantColumnError(t, err, "decode", 0, "t")
		if e, _ := errors.AsType[*CodecError](err); e.Path != "[1]" {
			t.Fatalf("path %q, want [1]", e.Path)
		}
		if _, ok := errors.AsType[*ProtocolError](err); !ok {
			t.Fatalf("%v does not report the malformed member as a ProtocolError", err)
		}
	})
	t.Run("TrailingTupleBytes", func(t *testing.T) {
		tupleType := mustMatrixType(TupleType(intType, textType))
		columns := NewColumns(ColumnSpec{Name: "t", Type: tupleType})
		// t = (1, "x") and one byte more.
		chain := matrixRaw([]byte{0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 1, 'x', 0xaa})
		decoder := TupleDecoder(Row(
			ColumnAt(0, Int32Codec, func(p *Pair[int32, string]) *int32 { return &p.First }),
			ColumnAt(1, StringCodec, func(p *Pair[int32, string]) *string { return &p.Second })))
		plan, err := OneColumn(decoder).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRow(plan, newRowReader(columns, chain, nil))
		wantColumnError(t, err, "decode", 0, "t")
		if _, ok := errors.AsType[*ProtocolError](err); !ok {
			t.Fatalf("%v does not report the trailing bytes as a ProtocolError", err)
		}
	})
	t.Run("UDT", func(t *testing.T) {
		inner := mustMatrixType(UDTType("ks", "inner", ColumnSpec{Name: "n", Type: intType}))
		outer := mustMatrixType(UDTType("ks", "outer", ColumnSpec{Name: "in", Type: inner}))
		columns := NewColumns(ColumnSpec{Name: "u", Type: outer})
		innerOpts, err := UDTOptionsFor(inner)
		if err != nil {
			t.Fatal(err)
		}
		outerOpts, err := UDTOptionsFor(outer)
		if err != nil {
			t.Fatal(err)
		}
		decoder := UDTDecoder(Row(Column("in", UDTDecoder(Row(Column("n", StringCodec, func(s *string) *string { return s })), innerOpts),
			func(s *string) *string { return s })), outerOpts)
		_, err = OneColumn(decoder).Compile(columns)
		wantColumnError(t, err, "compile", 0, "u")
		if e, _ := errors.AsType[*CodecError](err); e.Path != ".in.n" {
			t.Fatalf("path %q, want .in.n", e.Path)
		}
	})
}

// matrixRaw builds a row of the given cell bodies; a nil body is NULL.
func matrixRaw(cells ...[]byte) wire.Chain {
	var b []byte
	for _, cell := range cells {
		if cell == nil {
			b = append(b, 0xff, 0xff, 0xff, 0xff)
			continue
		}
		b = append(b, byte(len(cell)>>24), byte(len(cell)>>16), byte(len(cell)>>8), byte(len(cell)))
		b = append(b, cell...)
	}
	return wire.ChainOf(b)
}

// TestMemberError checks the error DecodeValue returns for a failing tuple
// member when no column encloses it: the message names the member's path,
// and the member's own error stays reachable through Unwrap.
func TestMemberError(t *testing.T) {
	tuple := mustMatrixType(TupleType(matrixType(KindInt), matrixType(KindText)))
	decoder := TupleDecoder(PairRows(OneColumn(Int32Codec), OneColumn(TextCodec)))
	for name, test := range map[string]struct {
		body []byte
		want string
		is   error
	}{
		// The second member is null, which a Text cannot hold.
		"NullMember": {[]byte{0, 0, 0, 4, 0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff}, "gocql: member [1]: gocql: nonnullable text cannot decode cell state null", nil},
		// The second member claims one byte the body does not have.
		"TruncatedMember": {[]byte{0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 1}, "gocql: member [1]: ", io.ErrUnexpectedEOF},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeValue(decoder, containerValue(t, tuple, test.body))
			member, ok := errors.AsType[*memberError](err)
			if !ok {
				t.Fatalf("DecodeValue = %T %v, want a member error", err, err)
			}
			if !strings.HasPrefix(err.Error(), test.want) || member.path != "[1]" {
				t.Fatalf("Error() = %q, want prefix %q", err.Error(), test.want)
			}
			if inner := errors.Unwrap(err); inner == nil || inner != member.err || strings.HasPrefix(inner.Error(), "gocql: member") {
				t.Fatalf("Unwrap() = %v, want the member's own error", inner)
			}
			if test.is != nil && !errors.Is(err, test.is) {
				t.Fatalf("errors.Is(%v, %v) = false", err, test.is)
			}
		})
	}
}
