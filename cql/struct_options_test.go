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
	ts := rowTestType(t, KindTimestamp)
	return NewColumns(
		ColumnSpec{Name: "id", Type: rowTestType(t, KindInt)},
		ColumnSpec{Name: "at", Type: ts},
		ColumnSpec{Name: "maybe", Type: ts},
		ColumnSpec{Name: "history", Type: CodecTestMust(ListType(ts))},
		ColumnSpec{Name: "versions", Type: CodecTestMust(MapType(rowTestType(t, KindText), ts))},
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
	one := NewColumns(ColumnSpec{Name: "v", Type: rowTestType(t, KindTimestamp)})
	ts := CodecTestMust(TimestampFromTime(at))
	if a, b := chainBytes(t, encodeTestParams(t, one, OneParam(TimestampTimeCodec), at)), chainBytes(t, encodeTestParams(t, one, OneParam(TimestampCodec), ts)); !bytes.Equal(a, b) {
		t.Fatalf("encoding %x, want %x", a, b)
	}
	// A time outside the millisecond range fails to encode.
	plan := CodecTestMust(OneParam(TimestampTimeCodec).Compile(one))
	if err := plan.Encode(newParamWriter(wire.NewBuilder(8), one, false), time.Date(400_000_000, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("encoded a time outside the millisecond range")
	}
	if err := TimestampTimeCodec.CheckType(rowTestType(t, KindDate)); err == nil {
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
		ColumnSpec{Name: "id", Type: rowTestType(t, KindInt)},
		ColumnSpec{Name: "name", Type: rowTestType(t, KindText)},
		ColumnSpec{Name: "point", Type: rowTestType(t, KindBigInt)},
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
		{"ParamsOf WithStreamDecoder", "WithStreamDecoder", func() error { _, err := ParamsOf[row](WithStreamDecoder(StreamInt32)); return err }},
		{"RowOf WithEncoder", "WithEncoder", func() error { _, err := RowOf[row](WithEncoder(Int32Codec)); return err }},
		{"RowOf WithStreamDecoder", "WithStreamDecoder", func() error { _, err := RowOf[row](WithStreamDecoder(StreamInt32)); return err }},
		{"StreamRowOf WithCodec", "WithCodec", func() error { _, err := StreamRowOf[row](codec); return err }},
		{"StreamRowOf ReuseBuffers", "ReuseBuffers", func() error { _, err := StreamRowOf[row](ReuseBuffers()); return err }},
		{"UDTCodecOf ReuseBuffers", "ReuseBuffers", func() error { _, err := UDTCodecOf[row](UDTOptions{Name: "u"}, ReuseBuffers()); return err }},
		{"TupleCodecOf IgnoreExtraColumns", "IgnoreExtraColumns", func() error { _, err := TupleCodecOf[row](IgnoreExtraColumns()); return err }},
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
	if _, err := StreamRowOf[row](WithStreamDecoder(StreamInt32), IgnoreExtraColumns()); err != nil {
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
	columns := NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, KindBigInt)}, ColumnSpec{Name: "origin", Type: rowTestType(t, KindBigInt)})
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
		ColumnSpec{Name: "id", Type: rowTestType(t, KindInt)},
		ColumnSpec{Name: "added", Type: rowTestType(t, KindBlob)},
		ColumnSpec{Name: "name", Type: rowTestType(t, KindText)},
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
	columns := NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, KindInt)}, ColumnSpec{Name: "extra", Type: rowTestType(t, KindBlob)})
	id := StreamColumn("id", StreamInt32, func(v *int32) *int32 { return v })
	if _, err := StreamRowInto(nil, id).Compile(columns); err == nil {
		t.Fatal("StreamRowInto accepted a column without a field")
	}
	if _, err := StreamRowInto(nil, id, StreamColumn("missing", StreamInt32, func(v *int32) *int32 { return v })).Compile(NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, KindInt)})); err == nil {
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
			ColumnSpec{Name: "street", Type: rowTestType(t, KindText)},
			ColumnSpec{Name: "zip_code", Type: rowTestType(t, KindInt)}))
	}
	value := udtPerson{Name: "n", Address: udtAddress{Street: "s", ZipCode: 12345}}
	for _, keyspace := range []string{"ks1", "ks2"} {
		columns := NewColumns(ColumnSpec{Name: "name", Type: rowTestType(t, KindText)}, ColumnSpec{Name: "address", Type: address(keyspace)})
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
	other := CodecTestMust(UDTType("ks1", "other", ColumnSpec{Name: "street", Type: rowTestType(t, KindText)}, ColumnSpec{Name: "zip_code", Type: rowTestType(t, KindInt)}))
	if err := MustUDTCodecOf[udtAddress](UDTOptions{Name: "address"}).CheckType(other); err == nil {
		t.Fatal("accepted a UDT of another name")
	}
	// Without a name, the options match a UDT of any name.
	if err := MustUDTCodecOf[udtAddress](UDTOptions{Keyspace: "ks1"}).CheckType(other); err != nil {
		t.Fatal("options without a name rejected a UDT:", err)
	}
	if err := MustUDTCodecOf[udtAddress](UDTOptions{}).CheckType(address("ks2")); err != nil {
		t.Fatal("zero options rejected a UDT:", err)
	}
	// The struct's fields and the UDT's must match unless options relax it.
	narrow := CodecTestMust(UDTType("ks1", "address", ColumnSpec{Name: "street", Type: rowTestType(t, KindText)}))
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
	tuple := CodecTestMust(TupleType(rowTestType(t, KindInt), rowTestType(t, KindText)))
	columns := NewColumns(ColumnSpec{Name: "id", Type: rowTestType(t, KindInt)}, ColumnSpec{Name: "pair", Type: tuple})
	codec := TupleCodec(PairParams(OneParam(Int32Codec), OneParam(StringCodec)), PairRows(OneColumn(Int32Codec), OneColumn(StringCodec)))
	value := row{ID: 1, Pair: Pair[int32, string]{First: 2, Second: "x"}}
	chain := encodeTestParams(t, columns, MustParamsOf[row](WithCodec(codec)), value)
	if got := decodeTestRow(t, columns, MustRowOf[row](WithCodec(codec)), chain); got != value {
		t.Fatalf("got %+v", got)
	}
	if err := codec.CheckType(rowTestType(t, KindInt)); err == nil {
		t.Fatal("TupleCodec accepted an int")
	}
}

// WithCodec of a UDT codec also maps []T, *T and map values of T, and
// UDTCodecOf takes IgnoreExtraColumns for extra UDT fields.
func TestUDTCodecCollections(t *testing.T) {
	address := CodecTestMust(UDTType("ks", "address",
		ColumnSpec{Name: "street", Type: rowTestType(t, KindText)},
		ColumnSpec{Name: "zip_code", Type: rowTestType(t, KindInt)}))
	type holder struct {
		List []udtAddress
		Set  []udtAddress
		Ptr  *udtAddress
		None *udtAddress
		ByID map[int32]udtAddress
	}
	columns := NewColumns(
		ColumnSpec{Name: "list", Type: CodecTestMust(ListType(address))},
		ColumnSpec{Name: "set", Type: CodecTestMust(SetType(address.WithFrozen(true)))},
		ColumnSpec{Name: "ptr", Type: address},
		ColumnSpec{Name: "none", Type: address},
		ColumnSpec{Name: "by_id", Type: CodecTestMust(MapType(rowTestType(t, KindInt), address))},
	)
	a, b := udtAddress{Street: "a", ZipCode: 1}, udtAddress{Street: "b", ZipCode: 2}
	value := holder{List: []udtAddress{a, b}, Set: []udtAddress{b}, Ptr: &a, ByID: map[int32]udtAddress{7: b}}
	opt := WithCodec(MustUDTCodecOf[udtAddress](UDTOptions{}))
	got := decodeTestRow(t, columns, MustRowOf[holder](opt), encodeTestParams(t, columns, MustParamsOf[holder](opt), value))
	if len(got.List) != 2 || got.List[1] != b || len(got.Set) != 1 || got.Ptr == nil || *got.Ptr != a || got.None != nil || got.ByID[7] != b {
		t.Fatalf("got %+v", got)
	}

	// A UDT with a field the struct has not.
	wide := CodecTestMust(UDTType("ks", "address",
		ColumnSpec{Name: "street", Type: rowTestType(t, KindText)},
		ColumnSpec{Name: "zip_code", Type: rowTestType(t, KindInt)},
		ColumnSpec{Name: "country", Type: rowTestType(t, KindText)}))
	if err := MustUDTCodecOf[udtAddress](UDTOptions{}).CheckType(wide); err == nil {
		t.Fatal("accepted an extra UDT field")
	}
	if err := MustUDTCodecOf[udtAddress](UDTOptions{}, IgnoreExtraColumns()).CheckType(wide); err != nil {
		t.Fatal("IgnoreExtraColumns:", err)
	}
}

// TupleCodecOf maps tuple members to fields by position, and PairCodec a
// two-member tuple to a Pair.
func TestTupleCodecOf(t *testing.T) {
	type point struct {
		X     int32
		Label string `cql:"whatever"`
		Skip  int    `cql:"-"`
		Tags  []string
	}
	typ := CodecTestMust(TupleType(rowTestType(t, KindInt), rowTestType(t, KindText), CodecTestMust(SetType(rowTestType(t, KindText)))))
	codec := MustTupleCodecOf[point]()
	encoded, err := EncodeValue(typ, codec, point{X: 3, Label: "l", Skip: 9, Tags: []string{"t"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeValue(codec, encoded)
	if err != nil || got.X != 3 || got.Label != "l" || got.Skip != 0 || len(got.Tags) != 1 {
		t.Fatalf("got %+v, %v", got, err)
	}
	short := CodecTestMust(TupleType(rowTestType(t, KindInt), rowTestType(t, KindText)))
	if err := codec.CheckType(short); err == nil {
		t.Fatal("accepted a tuple without a member for every field")
	}

	pair := PairCodec(Int32Codec, StringCodec)
	encoded, err = EncodeValue(short, pair, Pair[int32, string]{First: 1, Second: "s"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := DecodeValue(pair, encoded)
	if err != nil || p.First != 1 || p.Second != "s" {
		t.Fatalf("pair %+v, %v", p, err)
	}
}
