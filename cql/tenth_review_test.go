// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// cloneCounting is a row decoder that counts its Clone calls.
type cloneCounting struct {
	RowDecoder[string]
	clones *int
}

func (c cloneCounting) Clone(v string) string { *c.clones++; return c.RowDecoder.Clone(v) }

// TestConvertMappingErrors checks that a failing ConvertParams or ConvertRows
// mapping fails the encode or decode with a whole-row CodecError, and that a
// nil cloneValue detaches each row's A before the mapping.
func TestConvertMappingErrors(t *testing.T) {
	columns := NewColumns(ColumnSpec{Name: "v", Type: rowTestType(t, KindText)})
	boom := errors.New("boom")

	params := ConvertParams(OneParam(StringCodec), func(v int) (string, error) {
		if v < 0 {
			return "", boom
		}
		return "x", nil
	})
	plan := CodecTestMust(params.Compile(columns))
	w := newParamWriter(wire.NewBuilder(8), columns, true)
	err := plan.Encode(w, -1)
	var codecErr *CodecError
	if !errors.As(err, &codecErr) || !errors.Is(err, boom) || codecErr.Index != -1 || codecErr.Stage != CodecStageEncode {
		t.Fatalf("ConvertParams error = %v", err)
	}
	if got := err.Error(); got != "gocql: encode parameters: boom" {
		t.Fatalf("Error() = %q", got)
	}

	chain := encodeTestParams(t, columns, OneParam(StringCodec), "hello")
	failing := ConvertRows(OneColumn(StringCodec), func(string) (int, error) { return 0, boom }, nil)
	_, err = decodeRow(CodecTestMust(failing.Compile(columns)), newRowReader(columns, chain, nil))
	if !errors.As(err, &codecErr) || !errors.Is(err, boom) || codecErr.Index != -1 || codecErr.Stage != CodecStageDecode {
		t.Fatalf("ConvertRows error = %v", err)
	}
	if got := err.Error(); got != "gocql: decode row: boom" {
		t.Fatalf("Error() = %q", got)
	}

	clones := 0
	detaching := ConvertRows(RowDecoder[string](cloneCounting{OneColumn(StringCodec), &clones}), func(s string) (int, error) { return len(s), nil }, nil)
	got, err := decodeRow(CodecTestMust(detaching.Compile(columns)), newRowReader(columns, chain, nil))
	if err != nil || got != 5 || clones != 1 {
		t.Fatalf("decode = %d, %v after %d clones, want 5 after one", got, err, clones)
	}
	if detaching.Clone(7) != 7 || clones != 1 {
		t.Fatal("Clone without cloneValue must return the value as it is")
	}
}

// TestValueText checks the text forms, Cmp and conversions of the value
// types.
func TestValueText(t *testing.T) {
	type doc struct {
		D  Decimal
		V  Varint
		Dt Date
		Ts Timestamp
		T  TimeOfDay
		Du Duration
	}
	in := doc{
		D:  CodecTestMust(ParseDecimal("-12.30")),
		V:  VarintFromInt64(-1234567890123),
		Dt: CodecTestMust(ParseDate("2026-01-02")),
		Ts: CodecTestMust(ParseTimestamp("2026-01-02T15:04:05.123Z")),
		T:  CodecTestMust(ParseTimeOfDay("15:04:05.000000001")),
		Du: Duration{Months: 14, Days: 3, Nanoseconds: int64(time.Hour)},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"D":"-12.30","V":"-1234567890123","Dt":"2026-01-02","Ts":"2026-01-02T15:04:05.123Z","T":"15:04:05.000000001","Du":"1y2mo3d1h"}`
	if string(b) != want {
		t.Fatalf("json = %s, want %s", b, want)
	}
	var out doc
	if err := json.Unmarshal(b, &out); err != nil || out != in {
		t.Fatalf("round trip = %+v, %v", out, err)
	}
	if _, err := TimeOfDay(-1).MarshalText(); err == nil {
		t.Fatal("TimeOfDay outside one day marshaled")
	}
	if _, err := (Duration{Months: 1, Days: -1}).MarshalText(); err == nil {
		t.Fatal("mixed-sign Duration marshaled")
	}
	if err := out.V.UnmarshalText([]byte("1x")); err == nil {
		t.Fatal("Varint parsed 1x")
	}

	for _, c := range []struct {
		a, b int64
		want int
	}{{0, 0, 0}, {1, 2, -1}, {-1, 1, -1}, {255, 128, 1}, {-129, -128, -1}, {-1 << 40, 1 << 40, -1}, {1 << 40, 127, 1}, {-1, -256, 1}} {
		if got := VarintFromInt64(c.a).Cmp(VarintFromInt64(c.b)); got != c.want {
			t.Errorf("Cmp(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := VarintFromInt64(c.b).Cmp(VarintFromInt64(c.a)); got != -c.want {
			t.Errorf("Cmp(%d, %d) = %d, want %d", c.b, c.a, got, -c.want)
		}
	}
	if VarintFromInt64(5) != VarintFromBigInt(big.NewInt(5)) || CodecTestMust(VarintFromBytes([]byte{0, 0, 5})) != VarintFromInt64(5) {
		t.Fatal("Varints of one integer are not ==")
	}
	if CodecTestMust(VarintFromBytes([]byte{0})) != (Varint{}) {
		t.Fatal("zero Varint is not the zero value")
	}
	one, oneHundredths := CodecTestMust(ParseDecimal("1.0")), CodecTestMust(ParseDecimal("1.00"))
	if one.Cmp(oneHundredths) != 0 || one.Cmp(CodecTestMust(ParseDecimal("1.01"))) != -1 || CodecTestMust(ParseDecimal("2E+1")).Cmp(one) != 1 {
		t.Fatal("Decimal.Cmp")
	}

	if d, err := (Duration{Nanoseconds: 5}).TimeDuration(); err != nil || d != 5 {
		t.Fatalf("TimeDuration = %v, %v", d, err)
	}
	if _, err := (Duration{Days: 1}).TimeDuration(); err == nil {
		t.Fatal("TimeDuration accepted days")
	}
	if DurationFromTime(time.Minute) != (Duration{Nanoseconds: int64(time.Minute)}) {
		t.Fatal("DurationFromTime")
	}
	v1 := NewTimeUUID()
	if got, err := TimeUUIDFromBytes(v1.Bytes()); err != nil || got != v1 {
		t.Fatalf("TimeUUIDFromBytes = %v, %v", got, err)
	}
	if _, err := TimeUUIDFromBytes(RandomUUID().Bytes()); err == nil {
		t.Fatal("TimeUUIDFromBytes accepted version 4")
	}
	if _, err := TimeUUIDFromBytes([]byte{1}); err == nil {
		t.Fatal("TimeUUIDFromBytes accepted one byte")
	}
}

// TestBytesFormatAndWrite checks Bytes.Format, ValueWriter.Write and the
// owned Value.Bytes.
func TestBytesFormatAndWrite(t *testing.T) {
	b := BorrowBytes([]byte{0xca, 0xfe})
	if got := fmt.Sprintf("%v %x %X %d", b, b, b, b); got != "cafe cafe CAFE [202 254]" {
		t.Fatalf("Format = %q", got)
	}
	lease := wire.NewLease(CodecTestMust(wire.NewChain(wire.Borrow([]byte{1}))), nil)
	released := bytesView(CodecTestMust(wire.NewChain(wire.Borrow([]byte{1}))), wire.RefOf(lease))
	lease.Release()
	if got := fmt.Sprint(released); got != "<released>" {
		t.Fatalf("released Format = %q", got)
	}

	enc := writeEncoder{}
	encoded := CodecTestMust(EncodeValue(BlobType, Encoder[[]byte](enc), []byte("abc")))
	value := CodecTestMust(DecodeValue(DynamicCodec, encoded))
	owned := CodecTestMust(value.Bytes())
	if string(owned) != "abcabc" {
		t.Fatalf("Write body = %q", owned)
	}
	view := CodecTestMust(value.BytesView())
	if view.Len() != 6 {
		t.Fatalf("BytesView len = %d", view.Len())
	}
}

// writeEncoder writes its value twice through io.Writer, changing the
// buffer between writes to check that Write copies.
type writeEncoder struct{}

func (writeEncoder) CheckType(t DataType) error { return ByteSliceCodec.CheckType(t) }
func (writeEncoder) Encode(w *ValueWriter, v []byte) error {
	buf := append([]byte(nil), v...)
	if _, err := w.Write(buf); err != nil {
		return err
	}
	copy(buf, "zzz")
	_, err := fmt.Fprintf(w, "%s", v)
	return err
}

// TestDynamicBindingRules checks that a dynamic value binds a cell by the
// typed codecs' rules.
func TestDynamicBindingRules(t *testing.T) {
	encode := func(typ DataType, v Value) (EncodedValue, error) { return EncodeValue(typ, DynamicCodec, v) }
	if _, err := encode(ASCIIType, TextValue("plain")); err != nil {
		t.Fatalf("text into ascii: %v", err)
	}
	if _, err := encode(ASCIIType, TextValue("é")); err == nil {
		t.Fatal("non-ASCII text bound ascii")
	}
	v := CodecTestMust(encode(BigIntType, IntValue(-7)))
	if n := CodecTestMust(DecodeValue(Int64Codec, v)); n != -7 {
		t.Fatalf("int into bigint = %d", n)
	}
	if _, err := encode(IntType, BigIntValue(1<<40)); err == nil {
		t.Fatal("out-of-range bigint bound int")
	}
	v = CodecTestMust(encode(SmallIntType, BigIntValue(300)))
	if n := CodecTestMust(DecodeValue(Int16Codec, v)); n != 300 {
		t.Fatalf("bigint into smallint = %d", n)
	}
	v = CodecTestMust(encode(DoubleType, FloatValue(1.5)))
	if f := CodecTestMust(DecodeValue(Float64Codec, v)); f != 1.5 {
		t.Fatalf("float into double = %v", f)
	}
	if _, err := encode(IntType, TextValue("1")); err == nil || !strings.Contains(err.Error(), "does not bind") {
		t.Fatalf("text into int: %v", err)
	}
	list := CodecTestMust(ListValue(IntType, IntValue(1), IntValue(2)))
	v = CodecTestMust(encode(CodecTestMust(ListType(BigIntType)), list))
	if got := CodecTestMust(DecodeValue(ListCodec(Int64Codec), v)); len(got) != 2 || got[1] != 2 {
		t.Fatalf("list<int> into list<bigint> = %v", got)
	}

	// A UDT value built before ALTER TYPE ADD binds the altered type, the
	// added field as NULL; UDTValue may name a subset of the fields.
	before := CodecTestMust(UDTType("ks", "addr", ColumnSpec{Name: "street", Type: TextType}))
	after := CodecTestMust(UDTType("ks", "addr", ColumnSpec{Name: "street", Type: TextType}, ColumnSpec{Name: "zip", Type: IntType}))
	old := CodecTestMust(UDTValue(before, ValueField{Name: "street", Value: TextValue("Main")}))
	v = CodecTestMust(encode(after, old))
	decoded := CodecTestMust(DecodeValue(DynamicCodec, v))
	if zip := CodecTestMust(decoded.Field("zip")); zip.State() != CellNull {
		t.Fatalf("added field = %v, want NULL", zip)
	}
	subset := CodecTestMust(UDTValue(after, ValueField{Name: "zip", Value: IntValue(5)}))
	decoded = CodecTestMust(DecodeValue(DynamicCodec, CodecTestMust(encode(after, subset))))
	if street := CodecTestMust(decoded.Field("street")); street.State() != CellNull {
		t.Fatalf("unnamed field = %v, want NULL", street)
	}
	if _, err := encode(before, CodecTestMust(UDTValue(after, ValueField{Name: "zip", Value: IntValue(5)}))); err == nil || !strings.Contains(err.Error(), `"zip"`) {
		t.Fatalf("superset value into narrower UDT: %v", err)
	}
	other := CodecTestMust(UDTType("ks", "other", ColumnSpec{Name: "street", Type: TextType}))
	if _, err := encode(other, old); err == nil {
		t.Fatal("UDT bound another UDT")
	}
	if _, err := UDTValue(after, ValueField{Name: "nope", Value: IntValue(1)}); err == nil {
		t.Fatal("UDTValue accepted an unknown field")
	}

	m := CodecTestMust(MapValue(TextType, IntType, ValueEntry{Key: TextValue("k"), Value: IntValue(1)}))
	if m.Len() != 1 || !m.Type().Equal(CodecTestMust(MapType(TextType, IntType))) {
		t.Fatalf("MapValue = %v", m)
	}
	tuple := CodecTestMust(TupleValue(CodecTestMust(TupleType(IntType, TextType)), IntValue(1), TextValue("a")))
	if tuple.Len() != 2 {
		t.Fatalf("TupleValue = %v", tuple)
	}
}

// TestCodecOf checks that CodecOf, ParamOf and ColumnOf derive a value's
// codec as a struct field's.
func TestCodecOf(t *testing.T) {
	if c := MustCodecOf[int32](); fmt.Sprintf("%T", c) != fmt.Sprintf("%T", Int32Codec) {
		t.Fatalf("CodecOf[int32] = %T, want Int32Codec", c)
	}
	list := CodecTestMust(ListType(IntType))
	listCodec := MustCodecOf[[]int32]()
	encoded := CodecTestMust(EncodeValue(list, listCodec, []int32{1, 2, 3}))
	got := CodecTestMust(DecodeValue(listCodec, encoded))
	if len(got) != 3 || got[2] != 3 {
		t.Fatalf("[]int32 round trip = %v", got)
	}
	if clone := listCodec.Clone(got); len(clone) != 3 || &clone[0] == &got[0] {
		t.Fatalf("Clone = %v, want a copy", clone)
	}
	mapType := CodecTestMust(MapType(TextType, list))
	mapCodec := MustCodecOf[map[string][]int32]()
	encoded = CodecTestMust(EncodeValue(mapType, mapCodec, map[string][]int32{"a": {4}}))
	if m := CodecTestMust(DecodeValue(mapCodec, encoded)); len(m["a"]) != 1 || m["a"][0] != 4 {
		t.Fatalf("map round trip = %v", m)
	}
	if err := listCodec.CheckType(TextType); err == nil {
		t.Fatal("[]int32 codec accepted text")
	}
	if _, err := CodecOf[BindValue[int32]](); err == nil {
		t.Fatal("CodecOf accepted an encode-only type")
	}
	if _, err := CodecOf[chan int](); err == nil {
		t.Fatal("CodecOf accepted a channel")
	}
	if _, err := CodecOf[int32](NullAsZero()); err == nil {
		t.Fatal("CodecOf accepted an option it does not apply")
	}
	type status string
	statusCodec := ConvertCodec(StringCodec, func(s status) (string, error) { return "s:" + string(s), nil }, func(s string) (status, error) { return status(s), nil })
	sc := MustCodecOf[[]status](WithCodec(statusCodec))
	encoded = CodecTestMust(EncodeValue(CodecTestMust(ListType(TextType)), sc, []status{"x"}))
	if s := CodecTestMust(DecodeValue(sc, encoded)); len(s) != 1 || s[0] != "s:x" {
		t.Fatalf("WithCodec element = %v", s)
	}

	columns := NewColumns(ColumnSpec{Name: "v", Type: list})
	if _, err := ParamOf[BindValue[[]int32]]().Compile(columns); err != nil {
		t.Fatalf("ParamOf[BindValue]: %v", err)
	}
	chain := encodeTestParams(t, columns, ParamOf[[]int32](), []int32{7, 8})
	row, err := decodeRow(CodecTestMust(ColumnOf[[]int32]().Compile(columns)), newRowReader(columns, chain, nil))
	if err != nil || len(row) != 2 || row[1] != 8 {
		t.Fatalf("ColumnOf = %v, %v", row, err)
	}
	if _, err := ColumnOf[BindValue[int32]]().Compile(columns); err == nil {
		t.Fatal("ColumnOf accepted an encode-only type")
	}
	if _, err := ParamOf[chan int]().Compile(columns); err == nil {
		t.Fatal("ParamOf accepted a channel")
	}
}
