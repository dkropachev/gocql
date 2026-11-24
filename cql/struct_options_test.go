// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type timeRow struct {
	ID       int32
	At       time.Time
	Maybe    Nullable[time.Time]
	History  []time.Time
	Versions map[string]time.Time
}

func timeColumns(t *testing.T) Columns {
	t.Helper()
	ts := rowTestType(t, TypeTimestamp)
	return NewColumns(
		ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)},
		ColumnSpec{Name: "at", Type: ts},
		ColumnSpec{Name: "maybe", Type: ts},
		ColumnSpec{Name: "history", Type: CodecTestMust(ListType(ts))},
		ColumnSpec{Name: "versions", Type: CodecTestMust(MapType(rowTestType(t, TypeText), ts))},
	)
}

// TestTimestampTimeCodec maps time.Time fields, their Nullable, list and map
// value forms, as timestamps by default.
func TestTimestampTimeCodec(t *testing.T) {
	at := time.Date(2026, 11, 24, 10, 30, 0, 123_456_789, time.FixedZone("x", 3600))
	millis := at.Truncate(time.Millisecond).UTC()
	value := timeRow{ID: 1, At: at, Maybe: Nullable[time.Time]{Value: at, Valid: true}, History: []time.Time{at, at.Add(time.Second)}, Versions: map[string]time.Time{"v1": at}}
	columns := timeColumns(t)
	chain := encodeTestParams(t, columns, MustParamsOf[timeRow](), value)
	got := decodeTestRow(t, columns, MustRowOf[timeRow](), chain)
	want := timeRow{ID: 1, At: millis, Maybe: Nullable[time.Time]{Value: millis, Valid: true}, History: []time.Time{millis, millis.Add(time.Second)}, Versions: map[string]time.Time{"v1": millis}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	// The bytes are those of the Timestamp the time converts to.
	one := NewColumns(ColumnSpec{Name: "v", Type: rowTestType(t, TypeTimestamp)})
	ts := CodecTestMust(TimestampFromTime(at))
	if a, b := chainBytes(t, encodeTestParams(t, one, OneParam(TimestampTimeCodec), at)), chainBytes(t, encodeTestParams(t, one, OneParam(TimestampCodec), ts)); !bytes.Equal(a, b) {
		t.Fatalf("encoding %x, want %x", a, b)
	}
	// A time outside the millisecond range fails to encode.
	plan := CodecTestMust(OneParam(TimestampTimeCodec).Compile(one))
	if err := plan.Encode(newParamWriter(wire.NewBuilder(8), one, false), time.Date(400_000_000, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("encoded a time outside the millisecond range")
	}
	if err := TimestampTimeCodec.CheckType(rowTestType(t, TypeDate)); err == nil {
		t.Fatal("TimestampTimeCodec accepted a date column")
	}
}

type bindRow struct {
	ID    int32
	Name  BindValue[string]
	Point BindValue[structPoint]
}

// TestBindValueFields binds a BindValue field's value, NULL or UNSET, for a
// built-in type and for a type WithCodec or WithEncoder maps; RowOf rejects
// such a field.
func TestBindValueFields(t *testing.T) {
	columns := NewColumns(
		ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)},
		ColumnSpec{Name: "name", Type: rowTestType(t, TypeText)},
		ColumnSpec{Name: "point", Type: rowTestType(t, TypeBigInt)},
	)
	value := bindRow{ID: 1, Name: BindValue[string]{State: Unset}, Point: BindValue[structPoint]{State: Null}}
	for name, opt := range map[string]StructOption{"WithCodec": WithCodec(Codec[structPoint](pointCodec{})), "WithEncoder": WithEncoder(Encoder[structPoint](pointCodec{}))} {
		t.Run(name, func(t *testing.T) {
			got := chainBytes(t, encodeTestParams(t, columns, MustParamsOf[bindRow](opt), value))
			want := []byte{0, 0, 0, 4, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xff}
			if !bytes.Equal(got, want) {
				t.Fatalf("encoded %x, want %x", got, want)
			}
			present := bindRow{Name: BindValue[string]{Value: "n"}, Point: BindValue[structPoint]{Value: structPoint{X: 1, Y: 2}}}
			want = chainBytes(t, encodeTestParams(t, columns, Params(
				Param("id", Int32Codec, func(bindRow) int32 { return 0 }),
				Param("name", StringCodec, func(bindRow) string { return "n" }),
				Param("point", Encoder[structPoint](pointCodec{}), func(bindRow) structPoint { return structPoint{X: 1, Y: 2} }),
			), present))
			if got := chainBytes(t, encodeTestParams(t, columns, MustParamsOf[bindRow](opt), present)); !bytes.Equal(got, want) {
				t.Fatalf("encoded %x, want %x", got, want)
			}
		})
	}
	if _, err := ParamsOf[bindRow](); err == nil || !strings.Contains(err.Error(), "no codec for type cql.BindValue[") {
		t.Fatalf("ParamsOf without a codec for the point: %v", err)
	}
	if _, err := RowOf[bindRow](WithCodec(Codec[structPoint](pointCodec{}))); err == nil || !strings.Contains(err.Error(), "only binds parameters") {
		t.Fatalf("RowOf of a BindValue field: %v", err)
	}
}

// TestStructOptionTargets rejects an option the constructor does not apply,
// with a ValidationError naming the option.
func TestStructOptionTargets(t *testing.T) {
	type row struct{ ID int32 }
	codec := WithCodec(Codec[structPoint](pointCodec{}))
	for _, tc := range []struct {
		name, option string
		build        func() error
	}{
		{"ParamsOf ReuseBuffers", "ReuseBuffers", func() error { _, err := ParamsOf[row](ReuseBuffers()); return err }},
		{"ParamsOf IgnoreExtraColumns", "IgnoreExtraColumns", func() error { _, err := ParamsOf[row](IgnoreExtraColumns()); return err }},
		{"ParamsOf WithStreamCodec", "WithStreamCodec", func() error { _, err := ParamsOf[row](WithStreamCodec(StreamInt32)); return err }},
		{"RowOf WithEncoder", "WithEncoder", func() error { _, err := RowOf[row](WithEncoder(Int32Codec)); return err }},
		{"RowOf WithStreamCodec", "WithStreamCodec", func() error { _, err := RowOf[row](WithStreamCodec(StreamInt32)); return err }},
		{"StreamRowOf WithCodec", "WithCodec", func() error { _, err := StreamRowOf[row](codec); return err }},
		{"StreamRowOf ReuseBuffers", "ReuseBuffers", func() error { _, err := StreamRowOf[row](ReuseBuffers()); return err }},
		{"UDTCodecOf ReuseBuffers", "ReuseBuffers", func() error { _, err := UDTCodecOf[row](UDTOptions{Name: "u"}, ReuseBuffers()); return err }},
		{"UDTCodecOf IgnoreExtraColumns", "IgnoreExtraColumns", func() error {
			_, err := UDTCodecOf[row](UDTOptions{Name: "u"}, IgnoreExtraColumns())
			return err
		}},
		{"nil", "StructOption", func() error { _, err := RowOf[row](nil); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build()
			validation, ok := errors.AsType[*ValidationError](err)
			if !ok || validation.Field != tc.option {
				t.Fatalf("got %v, want a ValidationError for %s", err, tc.option)
			}
		})
	}
	// Every constructor applies its own options.
	if _, err := ParamsOf[row](codec, WithEncoder(Int32Codec)); err != nil {
		t.Fatal(err)
	}
	if _, err := RowOf[row](codec, ReuseBuffers(), IgnoreExtraColumns()); err != nil {
		t.Fatal(err)
	}
	if _, err := StreamRowOf[row](WithStreamCodec(StreamInt32), IgnoreExtraColumns()); err != nil {
		t.Fatal(err)
	}
	if _, err := UDTCodecOf[row](UDTOptions{Name: "u"}, codec); err != nil {
		t.Fatal(err)
	}
}

// TestStructOptionConcurrentEncode binds a WithCodec field from many
// goroutines: the field's accessor reads the parameter struct through a
// pooled copy, which must not leak between calls.
func TestStructOptionConcurrentEncode(t *testing.T) {
	type shape struct {
		ID     int64
		Origin structPoint
	}
	columns := NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, TypeBigInt)}, ColumnSpec{Name: "origin", Type: rowTestType(t, TypeBigInt)})
	params := MustParamsOf[shape](WithCodec(Codec[structPoint](pointCodec{})))
	rows := MustRowOf[shape](WithCodec(Codec[structPoint](pointCodec{})))
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 50 {
				value := shape{ID: int64(i*100 + j), Origin: structPoint{X: int32(i), Y: int32(j)}}
				if got := decodeTestRow(t, columns, rows, encodeTestParams(t, columns, params, value)); got != value {
					t.Errorf("got %+v, want %+v", got, value)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestIgnoreExtraColumns lets RowOf and StreamRowOf skip a column no field
// maps; without it, both reject the column.
func TestIgnoreExtraColumns(t *testing.T) {
	type row struct {
		ID   int32
		Name string
	}
	columns := NewColumns(
		ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)},
		ColumnSpec{Name: "added", Type: rowTestType(t, TypeBlob)},
		ColumnSpec{Name: "name", Type: rowTestType(t, TypeText)},
	)
	if _, err := MustRowOf[row]().Compile(columns); err == nil {
		t.Fatal("RowOf accepted a column without a field")
	}
	chain := encodeTestParams(t, columns, Params(
		Param("id", Int32Codec, func(row) int32 { return 7 }),
		Param("added", ByteSliceCodec, func(row) []byte { return []byte{1} }),
		Param("name", StringCodec, func(row) string { return "n" }),
	), row{})
	if got := decodeTestRow(t, columns, MustRowOf[row](IgnoreExtraColumns()), chain); got != (row{ID: 7, Name: "n"}) {
		t.Fatalf("RowOf: %+v", got)
	}

	type streamed struct {
		ID   int32
		Name []byte `cql:"name,stream=Text"`
	}
	if _, err := MustStreamRowOf[streamed]().Compile(columns); err == nil {
		t.Fatal("StreamRowOf accepted a column without a field")
	}
	data := append(append(streamCellBytes([]byte{0, 0, 0, 7}), streamCellBytes([]byte{1})...), streamCellBytes([]byte("n"))...)
	dst := streamed{Name: make([]byte, 0, 1)}
	streamDecodeRow(t, columns, MustStreamRowOf[streamed](IgnoreExtraColumns()), data, &dst)
	if dst.ID != 7 || string(dst.Name) != "n" {
		t.Fatalf("StreamRowOf: %+v", dst)
	}
}

// TestStreamRowIntoStrict maps every column of a StreamRowInto, as RowInto
// does, unless StreamIgnoreColumn skips it, and accepts a nil reset.
func TestStreamRowIntoStrict(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)}, ColumnSpec{Name: "extra", Type: rowTestType(t, TypeBlob)})
	id := StreamColumn("id", StreamInt32, func(v *int32) *int32 { return v })
	if _, err := StreamRowInto(nil, id).Compile(columns); err == nil {
		t.Fatal("StreamRowInto accepted a column without a field")
	}
	if _, err := StreamRowInto(nil, id, StreamColumn("missing", StreamInt32, func(v *int32) *int32 { return v })).Compile(NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)})); err == nil {
		t.Fatal("StreamRowInto accepted a field without a column")
	}
	data := append(streamCellBytes([]byte{0, 0, 0, 9}), streamCellBytes([]byte{1, 2})...)
	dst := int32(5)
	streamDecodeRow(t, columns, StreamRowInto(nil, id, StreamIgnoreColumn[int32]("extra")), data, &dst)
	if dst != 9 {
		t.Fatalf("decoded %d", dst)
	}
}

type udtAddress struct {
	Street  string
	ZipCode int32
}

type udtPerson struct {
	Name    string
	Address udtAddress
}

// TestUDTCodecOf maps a UDT to a struct, in any keyspace when the options
// name none, and inside an enclosing struct through WithCodec.
func TestUDTCodecOf(t *testing.T) {
	address := func(keyspace string) DataType {
		return CodecTestMust(UDTType(keyspace, "address",
			ColumnSpec{Name: "street", Type: rowTestType(t, TypeText)},
			ColumnSpec{Name: "zip_code", Type: rowTestType(t, TypeInt)}))
	}
	value := udtPerson{Name: "n", Address: udtAddress{Street: "s", ZipCode: 12345}}
	for _, keyspace := range []string{"ks1", "ks2"} {
		columns := NewColumns(ColumnSpec{Name: "name", Type: rowTestType(t, TypeText)}, ColumnSpec{Name: "address", Type: address(keyspace)})
		opt := WithCodec(MustUDTCodecOf[udtAddress](UDTOptions{Name: "address"}))
		chain := encodeTestParams(t, columns, MustParamsOf[udtPerson](opt), value)
		if got := decodeTestRow(t, columns, MustRowOf[udtPerson](opt), chain); got != value {
			t.Fatalf("%s: got %+v", keyspace, got)
		}
	}
	codec := MustUDTCodecOf[udtAddress](UDTOptions{Keyspace: "ks1", Name: "address"})
	if err := codec.CheckType(address("ks1")); err != nil {
		t.Fatal(err)
	}
	if err := codec.CheckType(address("ks2")); err == nil {
		t.Fatal("a codec naming ks1 accepted the UDT of ks2")
	}
	other := CodecTestMust(UDTType("ks1", "other", ColumnSpec{Name: "street", Type: rowTestType(t, TypeText)}, ColumnSpec{Name: "zip_code", Type: rowTestType(t, TypeInt)}))
	if err := MustUDTCodecOf[udtAddress](UDTOptions{Name: "address"}).CheckType(other); err == nil {
		t.Fatal("accepted a UDT of another name")
	}
	if _, err := UDTCodecOf[udtAddress](UDTOptions{Keyspace: "ks1"}); err == nil {
		t.Fatal("accepted options without a name")
	}
	// The struct's fields and the UDT's must match unless options relax it.
	narrow := CodecTestMust(UDTType("ks1", "address", ColumnSpec{Name: "street", Type: rowTestType(t, TypeText)}))
	if err := codec.CheckType(narrow); err == nil {
		t.Fatal("accepted a UDT without a struct field's field")
	}
	if err := MustUDTCodecOf[udtAddress](UDTOptions{Name: "address", AllowMissingFields: true}).CheckType(narrow); err != nil {
		t.Fatal(err)
	}
}

// TestTupleCodec encodes and decodes a tuple column of a struct field with
// one codec.
func TestTupleCodec(t *testing.T) {
	type row struct {
		ID   int32
		Pair Pair[int32, string]
	}
	tuple := CodecTestMust(TupleType(rowTestType(t, TypeInt), rowTestType(t, TypeText)))
	columns := NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)}, ColumnSpec{Name: "pair", Type: tuple})
	codec := TupleCodec(PairParams(OneParam(Int32Codec), OneParam(StringCodec)), PairRows(OneColumn(Int32Codec), OneColumn(StringCodec)))
	value := row{ID: 1, Pair: Pair[int32, string]{First: 2, Second: "x"}}
	chain := encodeTestParams(t, columns, MustParamsOf[row](WithCodec(codec)), value)
	if got := decodeTestRow(t, columns, MustRowOf[row](WithCodec(codec)), chain); got != value {
		t.Fatalf("got %+v", got)
	}
	if err := codec.CheckType(rowTestType(t, TypeInt)); err == nil {
		t.Fatal("TupleCodec accepted an int")
	}
}
