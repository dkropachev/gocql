// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"reflect"
	"testing"
)

func TestNullAsZero(t *testing.T) {
	integer, text := rowTestType(t, KindInt), rowTestType(t, KindText)
	columns := NewColumns(ColumnSpec{Name: "id", Type: integer}, ColumnSpec{Name: "name", Type: text})
	type source struct {
		ID   int32
		Name *string
	}
	chain := encodeTestParams(t, columns, MustParamsOf[source](), source{ID: 7})

	// A top-level column keeps ErrNull by default.
	type strict struct {
		ID   int32
		Name string
	}
	plan := CodecTestMust(MustRowOf[strict]().Compile(columns))
	if _, err := decodeRow(plan, newRowReader(columns, chain, nil)); !errors.Is(err, ErrNull) {
		t.Fatalf("NULL into a string field: %v", err)
	}
	// NullAsZero and the nullzero tag leave the field zero.
	if got := decodeTestRow(t, columns, MustRowOf[strict](NullAsZero()), chain); got != (strict{ID: 7}) {
		t.Fatalf("NullAsZero: %+v", got)
	}
	type tagged struct {
		ID   int32
		Name string `cql:"name,nullzero"`
	}
	if got := decodeTestRow(t, columns, MustRowOf[tagged](), chain); got != (tagged{ID: 7}) {
		t.Fatalf("nullzero tag: %+v", got)
	}
	// The tag applies to its field only.
	nullID := encodeTestParams(t, columns, Params(
		ParamAt(0, NullableEncoder[int32](Int32Codec), func(source) Nullable[int32] { return Nullable[int32]{} }),
		ParamAt(1, StringCodec, func(source) string { return "x" })), source{})
	plan2 := CodecTestMust(MustRowOf[tagged]().Compile(columns))
	if _, err := decodeRow(plan2, newRowReader(columns, nullID, nil)); !errors.Is(err, ErrNull) {
		t.Fatalf("NULL into an untagged field: %v", err)
	}
	if _, err := ParamsOf[strict](NullAsZero()); err == nil {
		t.Fatal("ParamsOf accepted NullAsZero")
	}
}

// TestUDTAddedField is the ALTER TYPE ADD regression: a value written before
// a field was added has no cell for it, and a NULL field is NULL; both decode
// as the struct field's zero value.
func TestUDTAddedField(t *testing.T) {
	integer, text := rowTestType(t, KindInt), rowTestType(t, KindText)
	old := CodecTestMust(UDTType("ks", "person", ColumnSpec{Name: "name", Type: text}))
	grown := CodecTestMust(UDTType("ks", "person", ColumnSpec{Name: "name", Type: text}, ColumnSpec{Name: "age", Type: integer}, ColumnSpec{Name: "nick", Type: text}))
	type before struct{ Name string }
	type person struct {
		Name string
		Age  int32
		Nick string
	}
	written := CodecTestMust(EncodeValue(old, MustUDTCodecOf[before](), before{Name: "ann"}))
	// The server sends the value as written, under the grown type.
	value := CodecTestMust(newEncodedValue(grown, CellPresent, written.Bytes()))
	got, err := DecodeValue(MustUDTCodecOf[person](UDTName("ks", "person")), value)
	if err != nil || got != (person{Name: "ann"}) {
		t.Fatalf("value predating ALTER TYPE ADD: %+v, %v", got, err)
	}
	// A NULL field decodes as zero too, and a pointer field as nil.
	withNull := CodecTestMust(EncodeValue(grown, MustUDTCodecOf[struct {
		Name string
		Age  *int32
		Nick *string
	}](), struct {
		Name string
		Age  *int32
		Nick *string
	}{Name: "bob"}))
	got, err = DecodeValue(MustUDTCodecOf[person](), withNull)
	if err != nil || got != (person{Name: "bob"}) {
		t.Fatalf("NULL UDT fields: %+v, %v", got, err)
	}
	type pointers struct {
		Name string
		Age  *int32
		Nick *string
	}
	ptrs, err := DecodeValue(MustUDTCodecOf[pointers](), value)
	if err != nil || ptrs.Age != nil || ptrs.Nick != nil {
		t.Fatalf("missing fields into pointers: %+v, %v", ptrs, err)
	}

	t.Run("Stream", func(t *testing.T) {
		type streamed struct {
			Name []byte
			Age  int32
		}
		grown2 := CodecTestMust(UDTType("ks", "person", ColumnSpec{Name: "name", Type: text}, ColumnSpec{Name: "age", Type: integer}))
		row := StreamRowInto(nil,
			StreamColumn("name", StreamTextBytes, func(v *streamed) *[]byte { return &v.Name }),
			StreamColumn("age", StreamInt32, func(v *streamed) *int32 { return &v.Age }))
		dst := streamed{Name: make([]byte, 0, 8), Age: 99}
		body := streamCellBytes([]byte("ann"))
		if err := StreamDecodeFixture(t, grown2, StreamUDT(row, UDTName("ks", "person")), streamCellBytes(body), &dst, 1); err != nil || string(dst.Name) != "ann" || dst.Age != 0 {
			t.Fatalf("streamed value predating ALTER TYPE ADD: %+v, %v", dst, err)
		}
	})
}

func TestStreamNullAsZero(t *testing.T) {
	integer := rowTestType(t, KindInt)
	type row struct {
		ID   int32
		Hits int32 `cql:"hits,nullzero"`
	}
	columns := NewColumns(ColumnSpec{Name: "id", Type: integer}, ColumnSpec{Name: "hits", Type: integer})
	data := append(streamCellBytes([]byte{0, 0, 0, 7}), 0xff, 0xff, 0xff, 0xff)
	plan := CodecTestMust(MustStreamRowOf[row]().Compile(columns))
	r, scope := streamReaderFixture(t, columns, &streamChunkSource{data: data, step: 1}, len(data))
	dst := row{Hits: 5}
	plan.Reset(&dst)
	if err := plan.DecodeInto(r, &dst); err != nil || !reflect.DeepEqual(dst, row{ID: 7}) {
		t.Fatalf("nullzero stream field: %+v, %v", dst, err)
	}
	if err := scope.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	type strict struct{ ID, Hits int32 }
	strictPlan := CodecTestMust(MustStreamRowOf[strict]().Compile(columns))
	r, _ = streamReaderFixture(t, columns, &streamChunkSource{data: data, step: 1}, len(data))
	var s strict
	if err := strictPlan.DecodeInto(r, &s); !errors.Is(err, ErrNull) {
		t.Fatalf("NULL into a strict stream field: %v", err)
	}
	zeroPlan := CodecTestMust(MustStreamRowOf[strict](NullAsZero()).Compile(columns))
	r, _ = streamReaderFixture(t, columns, &streamChunkSource{data: data, step: 1}, len(data))
	s = strict{Hits: 3}
	if err := zeroPlan.DecodeInto(r, &s); err != nil || s != (strict{ID: 7}) {
		t.Fatalf("NullAsZero stream: %+v, %v", s, err)
	}
}
