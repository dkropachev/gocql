// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// decodeAny adapts a typed codec for a table of mixed types.
func decodeAny[T any](c Codec[T]) func(ValueReader) (any, error) {
	return func(r ValueReader) (any, error) { return c.Decode(r) }
}

// fixedWidthCase decodes one body with a fixed-width codec. An empty err means
// the body must decode to want; otherwise the decode must fail with err.
type fixedWidthCase struct {
	body []byte
	want any
	err  string
}

// TestFixedWidthSplitBodies checks that a fixed-width cell decodes to the
// expected value or error both when its body is one span, which takes the
// fast path, and when it is split across two spans, which takes the cursor.
func TestFixedWidthSplitBodies(t *testing.T) {
	const (
		badBool = "gocql: boolean must be 0 or 1"
		badTime = "gocql: time outside one day"
	)
	cases := []struct {
		kind   Kind
		decode func(ValueReader) (any, error)
		cases  []fixedWidthCase
	}{
		{KindTinyInt, decodeAny(Int8Codec), []fixedWidthCase{
			{body: []byte{0x80}, want: int8(-128)},
			{body: []byte{1, 2}, err: "gocql: tinyint requires 1 bytes, got 2"},
		}},
		{KindSmallInt, decodeAny(Int16Codec), []fixedWidthCase{
			{body: []byte{0xff, 0xfe}, want: int16(-2)},
			{body: []byte{1}, err: "gocql: smallint requires 2 bytes, got 1"},
		}},
		{KindInt, decodeAny(Int32Codec), []fixedWidthCase{
			{body: []byte{0x80, 0, 0, 1}, want: int32(-0x7fffffff)},
			{body: []byte{1, 2, 3}, err: "gocql: int requires 4 bytes, got 3"},
		}},
		{KindBigInt, decodeAny(Int64Codec), []fixedWidthCase{
			{body: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf9}, want: int64(-7)},
			{body: []byte{1, 2, 3, 4}, err: "gocql: bigint requires 8 bytes, got 4"},
		}},
		{KindCounter, decodeAny(Int64Codec), []fixedWidthCase{
			{body: []byte{0, 0, 0, 0, 0, 0, 1, 0}, want: int64(256)},
			{body: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, err: "gocql: counter requires 8 bytes, got 9"},
		}},
		{KindTimestamp, decodeAny(TimestampCodec), []fixedWidthCase{
			{body: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, want: Timestamp(-1)},
			{body: []byte{1, 2, 3, 4, 5, 6, 7}, err: "gocql: timestamp requires 8 bytes, got 7"},
		}},
		{KindFloat, decodeAny(Float32Codec), []fixedWidthCase{
			{body: []byte{0x3f, 0xc0, 0, 0}, want: float32(1.5)},
			{body: []byte{1, 2}, err: "gocql: float requires 4 bytes, got 2"},
		}},
		{KindDouble, decodeAny(Float64Codec), []fixedWidthCase{
			{body: []byte{0xc0, 0x04, 0, 0, 0, 0, 0, 0}, want: float64(-2.5)},
			{body: []byte{1, 2, 3, 4}, err: "gocql: double requires 8 bytes, got 4"},
		}},
		{KindBoolean, decodeAny(BoolCodec), []fixedWidthCase{
			{body: []byte{0}, want: false},
			{body: []byte{1}, want: true},
			{body: []byte{2}, err: badBool},
			{body: []byte{1, 1}, err: "gocql: boolean requires 1 bytes, got 2"},
		}},
		{KindDate, decodeAny(DateCodec), []fixedWidthCase{
			{body: []byte{0x80, 0, 0, 0}, want: Date(0)},
			{body: []byte{0x7f, 0xff, 0xff, 0xff}, want: Date(-1)},
			{body: []byte{1, 2, 3, 4, 5}, err: "gocql: date requires 4 bytes, got 5"},
		}},
		{KindTime, decodeAny(TimeOfDayCodec), []fixedWidthCase{
			{body: []byte{0, 0, 0, 0, 0, 0, 0, 9}, want: TimeOfDay(9)},
			{body: []byte{0xff, 0, 0, 0, 0, 0, 0, 0}, err: badTime},
			{body: []byte{1, 2}, err: "gocql: time requires 8 bytes, got 2"},
		}},
	}
	check := func(t *testing.T, label string, got any, err error, c fixedWidthCase) {
		t.Helper()
		if c.err != "" {
			if err == nil || err.Error() != c.err {
				t.Fatalf("%s %x: got (%v, %v), want error %q", label, c.body, got, err, c.err)
			}
			return
		}
		if err != nil || got != c.want {
			t.Fatalf("%s %x: got (%v, %v), want %v", label, c.body, got, err, c.want)
		}
	}
	for _, tc := range cases {
		decode := tc.decode
		for _, c := range tc.cases {
			got, err := decode(ScalarReader(t, tc.kind, Present, c.body))
			check(t, tc.kind.String()+" contiguous", got, err, c)
			// A one-byte body is always one span, so the cursor read of a
			// one-byte codec is unreachable; its wrong widths stop at
			// fixedCursor's width check.
			for cut := 1; cut < len(c.body); cut++ {
				got, err := decode(ScalarReader(t, tc.kind, Present, c.body[:cut], c.body[cut:]))
				check(t, tc.kind.String()+" split", got, err, c)
			}
		}
	}
}

// TestFixedWidthNullCell checks that a null cell still fails before the fast
// path, with requirePresent's error.
func TestFixedWidthNullCell(t *testing.T) {
	_, err := Int32Codec.Decode(ScalarReader(t, KindInt, Null))
	if err == nil || err.Error() != "gocql: nonnullable int cannot decode cell state null" {
		t.Fatalf("Decode null: %v", err)
	}
}

// TestFixedWidthReleasedLease checks that a contiguous fixed-width cell,
// which the fast path would read, is rejected once its page is released.
func TestFixedWidthReleasedLease(t *testing.T) {
	body := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	lease := wire.NewLease(wire.ChainOf(body), nil)
	r := valueReaderFromLease(scalarType(t, KindBigInt), Present, wire.ChainOf(body), lease)
	if v, err := Int64Codec.Decode(r); err != nil || v != 1 {
		t.Fatalf("Decode before release: (%v, %v), want 1", v, err)
	}
	lease.Release()
	if _, err := Int64Codec.Decode(r); !errors.Is(err, ErrReleased) {
		t.Fatalf("Decode after release: %v, want ErrReleased", err)
	}
}
