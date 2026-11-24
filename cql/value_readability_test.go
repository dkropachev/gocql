// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"math"
	"math/big"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestErrNull(t *testing.T) {
	null := []byte{0xff, 0xff, 0xff, 0xff}
	t.Run("Decoder", func(t *testing.T) {
		_, err := DecodeValue(Int64Codec, CodecTestMust(NewEncodedValue(BigIntType, Null, Bytes{})))
		if !errors.Is(err, ErrNull) || err.Error() != "gocql: nonnullable bigint cannot decode cell state null" {
			t.Fatalf("got %v", err)
		}
		if _, err := DecodeValue(NullableDecoder(Int64Codec), CodecTestMust(NewEncodedValue(BigIntType, Null, Bytes{}))); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Row", func(t *testing.T) {
		chain := CodecTestMust(wire.NewChain(wire.Borrow(null)))
		reader := newRowReader(NewColumns(ColumnSpec{Name: "v", Type: BigIntType}), chain, nil)
		var got int64
		if err := readColumnInto(reader, Int64Codec, &got); !errors.Is(err, ErrNull) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("Stream", func(t *testing.T) {
		var got int64
		if err := StreamDecodeFixture(t, BigIntType, StreamInt64, null, &got, 1); !errors.Is(err, ErrNull) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("Accessor", func(t *testing.T) {
		if _, err := NullValue(BigIntType).Signed(); !errors.Is(err, ErrNull) || !hasErrorType[*ValidationError](err) {
			t.Fatalf("got %v", err)
		}
		if _, err := UnsetValue(BigIntType).Signed(); err == nil || errors.Is(err, ErrNull) {
			t.Fatalf("unset: %v", err)
		}
	})
}

func TestCounterIsInt64(t *testing.T) {
	encoded, err := EncodeValue(CounterType, DynamicCodec, BigIntValue(-5))
	if err != nil {
		t.Fatal("BigIntValue does not bind a counter:", err)
	}
	decoded, err := DecodeValue(DynamicCodec, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := decoded.Signed(); err != nil || n != -5 || decoded.Type().Kind() != TypeCounter {
		t.Fatalf("decoded %v (%s), %v", decoded, decoded.Type(), err)
	}
	if n, err := DecodeValue(Int64Codec, encoded); err != nil || n != -5 {
		t.Fatalf("Int64Codec: %d, %v", n, err)
	}
	if _, err := StreamInt64.Compile(CounterType); err != nil {
		t.Fatal("StreamInt64 rejects counter:", err)
	}
	if _, err := EncodeValue(IntType, DynamicCodec, BigIntValue(1)); err == nil {
		t.Fatal("BigIntValue bound an int")
	}
}

func TestScalarString(t *testing.T) {
	varint := func(s string) Varint {
		n, ok := new(big.Int).SetString(s, 10)
		if !ok {
			t.Fatal(s)
		}
		return VarintFromBigInt(n)
	}
	for _, test := range []struct {
		got  interface{ String() string }
		want string
	}{
		{Varint{}, "0"},
		{varint("-123456789012345678901234567890"), "-123456789012345678901234567890"},
		{Decimal{Unscaled: varint("123"), Scale: 0}, "123"},
		{Decimal{Unscaled: varint("1230"), Scale: 2}, "12.30"},
		{Decimal{Unscaled: varint("-5"), Scale: 3}, "-0.005"},
		{Decimal{Unscaled: varint("1"), Scale: 6}, "0.000001"},
		{Decimal{Unscaled: varint("1"), Scale: 7}, "1E-7"},
		{Decimal{Unscaled: varint("123"), Scale: -5}, "1.23E+7"},
		{Decimal{Unscaled: varint("-123"), Scale: 10}, "-1.23E-8"},
		{Decimal{}, "0"},
		{Date(0), "1970-01-01"},
		{Date(-1), "1969-12-31"},
		{Timestamp(1_700_000_000_123), "2023-11-14T22:13:20.123Z"},
		{Timestamp(-1), "1969-12-31T23:59:59.999Z"},
		{TimeOfDay(0), "00:00:00.000000000"},
		{TimeOfDay(13*time.Hour + 30*time.Minute + 54*time.Second + 234*time.Millisecond), "13:30:54.234000000"},
		{TimeOfDay(nanosecondsPerDay), "TimeOfDay(86400000000000)"},
		{Duration{}, "0s"},
		{Duration{Months: 14, Days: 3, Nanoseconds: int64(4*time.Hour + 5*time.Minute + 6*time.Second + 789*time.Millisecond + 10*time.Microsecond + 11)}, "1y2mo3d4h5m6s789ms10us11ns"},
		{Duration{Days: -2, Nanoseconds: -int64(time.Hour)}, "-2d1h"},
		{Duration{Nanoseconds: math.MinInt64}, "-2562047h47m16s854ms775us808ns"},
		{Duration{Months: 1, Days: -1}, "{1 -1 0}"},
	} {
		if s := test.got.String(); s != test.want {
			t.Errorf("%#v: got %q, want %q", test.got, s, test.want)
		}
	}
	t.Run("Released", func(t *testing.T) {
		lease := wire.NewLease(CodecTestMust(wire.NewChain(wire.Borrow([]byte{1}))), nil)
		v := varintView(bytesView(CodecTestMust(wire.NewChain(wire.Borrow([]byte{1}))), wire.RefOf(lease)))
		lease.Release()
		if v.String() != "<released>" || (Decimal{Unscaled: v}).String() != "<released>" {
			t.Fatal(v.String())
		}
	})
}

func TestValueString(t *testing.T) {
	list := CodecTestMust(ListType(IntType))
	set := CodecTestMust(SetType(TextType))
	mapping := CodecTestMust(MapType(TextType, IntType))
	tuple := CodecTestMust(TupleType(IntType, TextType))
	udt := CodecTestMust(UDTType("ks", "point", ColumnSpec{Name: "x", Type: IntType}, ColumnSpec{Name: "label", Type: TextType}))
	vector := CodecTestMust(VectorType(FloatType, 2))
	uuid := CodecTestMust(ParseUUID("00112233-4455-6677-8899-aabbccddeeff"))
	for _, test := range []struct {
		value Value
		want  string
	}{
		{Value{}, "<invalid>"},
		{NullValue(IntType), "null"},
		{UnsetValue(IntType), "unset"},
		{CodecTestMust(EmptyValue(IntType)), "empty"},
		{BooleanValue(true), "true"},
		{BigIntValue(-7), "-7"},
		{FloatValue(1.5), "1.5"},
		{DoubleValue(0.1), "0.1"},
		{TextValue("it's"), "'it''s'"},
		{BlobValue([]byte{1, 0xab}), "0x01ab"},
		{UUIDValue(uuid), "00112233-4455-6677-8899-aabbccddeeff"},
		{InetValue(netip.MustParseAddr("::1")), "'::1'"},
		{DateValue(0), "'1970-01-01'"},
		{TimeValue(TimeOfDay(time.Second)), "'00:00:01.000000000'"},
		{TimestampValue(0), "'1970-01-01T00:00:00.000Z'"},
		{DurationValue(Duration{Days: 1}), "1d"},
		{DecimalValue(Decimal{Unscaled: VarintFromBigInt(big.NewInt(15)), Scale: 1}), "1.5"},
		{CodecTestMust(ListValue(list, []Value{IntValue(1), IntValue(2)})), "[1, 2]"},
		{CodecTestMust(SetValue(set, []Value{TextValue("a")})), "{'a'}"},
		{CodecTestMust(MapValue(mapping, []ValueEntry{{Key: TextValue("k"), Value: IntValue(1)}})), "{'k': 1}"},
		{CodecTestMust(TupleValue(tuple, []Value{IntValue(1), NullValue(TextType)})), "(1, null)"},
		{CodecTestMust(UDTValue(udt, []ValueField{{Name: "x", Value: IntValue(3)}, {Name: "label", Value: TextValue("p")}})), "{x: 3, label: 'p'}"},
		{CodecTestMust(VectorValue(vector, []Value{FloatValue(1), FloatValue(-2)})), "[1, -2]"},
	} {
		if got := test.value.String(); got != test.want {
			t.Errorf("got %q, want %q", got, test.want)
		}
		if test.value.State() != Present || test.value.Type().Kind() < TypeList {
			continue
		}
		// A decoded aggregate prints as the one it was encoded from.
		encoded := CodecTestMust(EncodeValue(test.value.Type(), DynamicCodec, test.value))
		if got := CodecTestMust(DecodeValue(DynamicCodec, encoded)).String(); got != test.want {
			t.Errorf("decoded: got %q, want %q", got, test.want)
		}
	}
}

func TestValueMembers(t *testing.T) {
	list := CodecTestMust(ListType(IntType))
	udt := CodecTestMust(UDTType("ks", "point", ColumnSpec{Name: "x", Type: IntType}, ColumnSpec{Name: "label", Type: TextType}))
	tuple := CodecTestMust(TupleType(IntType, TextType))
	built := map[string]Value{
		"list":  CodecTestMust(ListValue(list, []Value{IntValue(10), IntValue(20), IntValue(30)})),
		"udt":   CodecTestMust(UDTValue(udt, []ValueField{{Name: "label", Value: TextValue("p")}, {Name: "x", Value: IntValue(3)}})),
		"tuple": CodecTestMust(TupleValue(tuple, []Value{IntValue(1), TextValue("a")})),
	}
	for name, v := range built {
		decoded := CodecTestMust(DecodeValue(DynamicCodec, CodecTestMust(EncodeValue(v.Type(), DynamicCodec, v))))
		for form, v := range map[string]Value{"built": v, "decoded": decoded} {
			t.Run(name+"/"+form, func(t *testing.T) {
				switch name {
				case "list":
					if v.Len() != 3 {
						t.Fatal(v.Len())
					}
					if n, err := CodecTestMust(v.At(2)).Signed(); err != nil || n != 30 {
						t.Fatal(n, err)
					}
					if _, err := v.At(3); !hasErrorType[*ValidationError](err) {
						t.Fatal(err)
					}
					if _, err := v.Field("x"); err == nil {
						t.Fatal("Field on a list")
					}
				case "udt":
					if v.Len() != 2 {
						t.Fatal(v.Len())
					}
					if text, err := CodecTestMust(v.Field("label")).Text(); err != nil || text.String() != "p" {
						t.Fatal(text, err)
					}
					if n, err := CodecTestMust(v.At(0)).Signed(); err != nil || n != 3 {
						t.Fatal("At(0) is not the first field in type order:", n, err)
					}
					if _, err := v.Field("y"); !hasErrorType[*ValidationError](err) {
						t.Fatal(err)
					}
				case "tuple":
					if text, err := CodecTestMust(v.At(1)).Text(); err != nil || text.String() != "a" {
						t.Fatal(text, err)
					}
				}
			})
		}
	}
	if IntValue(1).Len() != 0 || NullValue(list).Len() != 0 {
		t.Fatal("Len of a scalar or a null list")
	}
	if _, err := NullValue(list).At(0); !errors.Is(err, ErrNull) {
		t.Fatal(err)
	}
}

func TestValueIteratorAll(t *testing.T) {
	list := CodecTestMust(ListType(IntType))
	v := CodecTestMust(ListValue(list, []Value{IntValue(1), IntValue(2), IntValue(3)}))
	var got []int64
	for e, err := range CodecTestMust(v.Elements()).All() {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, CodecTestMust(e.Signed()))
	}
	if len(got) != 3 || got[2] != 3 {
		t.Fatal(got)
	}
	it := CodecTestMust(v.Elements())
	for range it.All() {
		break
	}
	if !it.Next() || CodecTestMust(it.Value().Signed()) != 2 {
		t.Fatal("break did not leave the iterator at the next member")
	}
	// A failure is yielded once, last.
	failing := newValueIterator(2, func(i int) (Value, error) {
		if i == 1 {
			return Value{}, errors.New("broken")
		}
		return IntValue(1), nil
	}, nil)
	var pairs []string
	for e, err := range failing.All() {
		pairs = append(pairs, e.String()+"/"+errString(err))
	}
	if strings.Join(pairs, ",") != "1/,<invalid>/broken" || failing.Err() == nil {
		t.Fatal(pairs)
	}
	var zero *ValueIterator[Value]
	for _, err := range zero.All() {
		if !hasErrorType[*ValidationError](err) {
			t.Fatal("nil iterator yielded a member")
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestValueTimeAccessors(t *testing.T) {
	ts := Timestamp(1_700_000_000_123)
	if got, err := TimestampValue(ts).Time(); err != nil || !got.Equal(time.UnixMilli(int64(ts))) || got.Location() != time.UTC {
		t.Fatal(got, err)
	}
	if got, err := DateValue(1).Time(); err != nil || !got.Equal(time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatal(got, err)
	}
	if _, err := BigIntValue(1).Time(); err == nil {
		t.Fatal("Time of a bigint")
	}
	if got, err := TimeValue(42).TimeOfDay(); err != nil || got != 42 {
		t.Fatal(got, err)
	}
	id := NewTimeUUID()
	if got, err := TimeUUIDValue(id).TimeUUID(); err != nil || got != id {
		t.Fatal(got, err)
	}
	if _, err := UUIDValue(UUID(id)).TimeUUID(); err == nil {
		t.Fatal("TimeUUID of a uuid")
	}
}

func TestDecodeValueEmptyShape(t *testing.T) {
	var seen ValueReader
	calls := 0
	empty := CodecTestMust(NewEncodedValue(IntType, EmptyCell, Bytes{}))
	if _, err := DecodeValue(Decoder[int32](capturedRowInt{Decoder: Int32Codec, seen: &seen, calls: &calls}), empty); err == nil {
		t.Fatal("Int32Codec decoded an empty int")
	}
	if calls != 1 || seen.State() != Present || seen.Bytes().Len() != 0 {
		t.Fatalf("decoder saw %v with %d bytes", seen.State(), seen.Bytes().Len())
	}
	if v, err := DecodeValue(EmptyCodec(Int32Codec), empty); err != nil || !v.Empty {
		t.Fatal(v, err)
	}
	if v, err := DecodeValue(DynamicCodec, empty); err != nil || v.State() != EmptyCell {
		t.Fatal(v, err)
	}
}

func TestTypeVector(t *testing.T) {
	vector := CodecTestMust(VectorType(FloatType, 3))
	if vector.Kind() != TypeVector || TypeVector.String() != "vector" || vector.String() != "vector<float,3>" {
		t.Fatal(vector.Kind(), vector)
	}
	if (DataType{}).Kind() != TypeCustom {
		t.Fatal("zero DataType kind")
	}
}

func TestNonNullAndIsZero(t *testing.T) {
	if v := NonNull(int32(0)); !v.Valid || v.Value != 0 {
		t.Fatal(v)
	}
	if !(UUID{}).IsZero() || !(TimeUUID{}).IsZero() || NewTimeUUID().IsZero() {
		t.Fatal("IsZero")
	}
}
