// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestIntegerWidening(t *testing.T) {
	tiny, small, integer := rowTestType(t, KindTinyInt), rowTestType(t, KindSmallInt), rowTestType(t, KindInt)
	float, double := rowTestType(t, KindFloat), rowTestType(t, KindDouble)

	// Decoding a narrower column is lossless.
	for _, c := range []struct {
		typ  DataType
		enc  Encoder[int64]
		want int64
	}{
		{tiny, ConvertEncoder(Int8Codec, func(v int64) (int8, error) { return int8(v), nil }), -100},
		{small, ConvertEncoder(Int16Codec, func(v int64) (int16, error) { return int16(v), nil }), -30000},
		{integer, ConvertEncoder(Int32Codec, func(v int64) (int32, error) { return int32(v), nil }), -2_000_000_000},
	} {
		value := CodecTestMust(EncodeValue(c.typ, c.enc, c.want))
		if got, err := DecodeValue(Int64Codec, value); err != nil || got != c.want {
			t.Fatalf("Int64Codec from %s: %d, %v", c.typ, got, err)
		}
		if got, err := DecodeValue(IntCodec, value); err != nil || int64(got) != c.want {
			t.Fatalf("IntCodec from %s: %d, %v", c.typ, got, err)
		}
	}
	if got, err := DecodeValue(Int32Codec, CodecTestMust(EncodeValue(small, Int16Codec, int16(-5)))); err != nil || got != -5 {
		t.Fatalf("Int32Codec from smallint: %d, %v", got, err)
	}
	// A narrower codec still rejects a wider column.
	if err := Int16Codec.CheckType(integer); err == nil {
		t.Fatal("Int16Codec accepted int")
	}

	// Encoding into a narrower column checks the range.
	if v, err := EncodeValue(tiny, Int64Codec, int64(127)); err != nil || v.Bytes().Len() != 1 {
		t.Fatalf("127 into tinyint: %v", err)
	}
	if _, err := EncodeValue(tiny, Int64Codec, int64(128)); err == nil {
		t.Fatal("128 encoded into tinyint")
	}

	// Struct fields widen too, through the direct ops, and a range error is
	// a CodecError naming the column.
	type row struct{ N int64 }
	columns := NewColumns(ColumnSpec{Name: "n", Type: small})
	chain := encodeTestParams(t, columns, MustParamsOf[row](), row{N: -7})
	if got := decodeTestRow(t, columns, MustRowOf[row](), chain); got.N != -7 {
		t.Fatalf("struct field from smallint: %d", got.N)
	}
	plan := CodecTestMust(MustParamsOf[row]().Compile(columns))
	w := newParamWriter(wire.NewBuilder(8), columns, true)
	var ce *CodecError
	if err := plan.Encode(w, row{N: math.MaxInt16 + 1}); !errors.As(err, &ce) || ce.Column != "n" {
		t.Fatalf("out-of-range struct field: %v", err)
	}

	// Float64Codec widens float; encoding checks float32's range.
	if got, err := DecodeValue(Float64Codec, CodecTestMust(EncodeValue(float, Float32Codec, float32(1.5)))); err != nil || got != 1.5 {
		t.Fatalf("Float64Codec from float: %v, %v", got, err)
	}
	if _, err := EncodeValue(float, Float64Codec, math.MaxFloat64); err == nil {
		t.Fatal("MaxFloat64 encoded into float")
	}
	if err := Float32Codec.CheckType(double); err == nil {
		t.Fatal("Float32Codec accepted double")
	}

	t.Run("Stream", func(t *testing.T) {
		var v int64
		if err := StreamDecodeFixture(t, small, StreamInt64, streamCellBytes([]byte{0xff, 0xfe}), &v, 1); err != nil || v != -2 {
			t.Fatalf("StreamInt64 from smallint: %d, %v", v, err)
		}
		var f float64
		bits := math.Float32bits(2.5)
		if err := StreamDecodeFixture(t, float, StreamFloat64, streamCellBytes([]byte{byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits)}), &f, 1); err != nil || f != 2.5 {
			t.Fatalf("StreamFloat64 from float: %v, %v", f, err)
		}
	})
}
