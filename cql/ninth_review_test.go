// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestUnsetBinding(t *testing.T) {
	integer, text := rowTestType(t, KindInt), rowTestType(t, KindText)
	list := CodecTestMust(ListType(text))
	columns := NewColumns(ColumnSpec{Name: "id", Type: integer}, ColumnSpec{Name: "name", Type: text}, ColumnSpec{Name: "tags", Type: list})
	states := func(t *testing.T, params ParamEncoder[unsetRow], value unsetRow) []CellState {
		t.Helper()
		plan := CodecTestMust(params.Compile(columns))
		capture := make([]paramCell, columns.Len())
		w := newParamWriter(wire.NewBuilder(8), columns, true)
		captureParamCells(w, capture)
		if err := plan.Encode(w, value); err != nil {
			t.Fatal(err)
		}
		out := make([]CellState, len(capture))
		for i, c := range capture {
			out[i] = c.State
		}
		return out
	}
	value := unsetRow{ID: 1}
	if got := states(t, MustParamsOf[unsetRow](), value); got[1] != Null || got[2] != Present {
		t.Fatalf("nil fields without options: %v", got)
	}
	if got := states(t, MustParamsOf[unsetRow](NilAsUnset()), value); got[1] != Unset || got[2] != Unset {
		t.Fatalf("NilAsUnset: %v", got)
	}
	name := "n"
	if got := states(t, MustParamsOf[unsetRow](NilAsUnset()), unsetRow{ID: 1, Name: &name, Tags: []string{}}); got[1] != Present || got[2] != Present {
		t.Fatalf("NilAsUnset with set fields: %v", got)
	}
	type tagged struct {
		ID   int32
		Name *string  `cql:"name,nilunset"`
		Tags []string `cql:"tags"`
	}
	plan := CodecTestMust(MustParamsOf[tagged]().Compile(columns))
	capture := make([]paramCell, columns.Len())
	w := newParamWriter(wire.NewBuilder(8), columns, true)
	captureParamCells(w, capture)
	if err := plan.Encode(w, tagged{ID: 1}); err != nil || capture[1].State != Unset || capture[2].State != Present {
		t.Fatalf("nilunset tag: %v %v", capture, err)
	}
	type bad struct {
		ID int32 `cql:"id,nilunset"`
	}
	if _, err := ParamsOf[bad](); err == nil || !strings.Contains(err.Error(), "cannot be nil") {
		t.Fatalf("nilunset on an int32: %v", err)
	}
	if _, err := RowOf[unsetRow](NilAsUnset()); err == nil {
		t.Fatal("RowOf accepted NilAsUnset")
	}

	// Bind, BindNull and BindUnset build the three states.
	if Bind(int32(3)) != (BindValue[int32]{Value: 3, State: Present}) || BindNull[int32]().State != Null || BindUnset[int32]().State != Unset {
		t.Fatal("Bind helpers")
	}
}

type unsetRow struct {
	ID   int32
	Name *string
	Tags []string
}

func TestRepeatedPositionalMarkers(t *testing.T) {
	integer := rowTestType(t, KindInt)
	// The server names both markers of WHERE a > ? AND a < ? "a".
	markers := NewColumns(ColumnSpec{Name: "a", Type: integer}, ColumnSpec{Name: "a", Type: integer})
	type bounds struct{ A int32 }
	check := func(params any) error {
		return params.(interface{ repeatedMarkers(Columns) error }).repeatedMarkers(markers)
	}
	for name, params := range map[string]ParamEncoder[bounds]{
		"ParamsOf": MustParamsOf[bounds](),
		"Params":   Params(Param("a", Int32Codec, func(v bounds) int32 { return v.A })),
	} {
		// Compile binds the field to both, as named markers (:a twice) ask;
		// the driver runs the check for a statement of ? markers.
		if _, err := params.Compile(markers); err != nil {
			t.Fatal(err)
		}
		var ce *CodecError
		if err := check(params); !errors.As(err, &ce) || !strings.Contains(err.Error(), ":from and :to") {
			t.Fatalf("%s: repeated ? marker names: %v", name, err)
		}
	}
	// Positional fields are not affected.
	positional := Params(ParamAt(0, Int32Codec, func(v bounds) int32 { return v.A }), ParamAt(1, Int32Codec, func(v bounds) int32 { return v.A }))
	if err := check(positional); err != nil {
		t.Fatal(err)
	}
}

func TestConvertErrors(t *testing.T) {
	integer := rowTestType(t, KindInt)
	type even int32
	codec := ConvertCodec(Int32Codec,
		func(v even) (int32, error) {
			if v%2 != 0 {
				return 0, fmt.Errorf("%d is odd", v)
			}
			return int32(v), nil
		},
		func(v int32) (even, error) {
			if v%2 != 0 {
				return 0, fmt.Errorf("%d is odd", v)
			}
			return even(v), nil
		})
	if _, err := EncodeValue(integer, codec, 3); err == nil || !strings.Contains(err.Error(), "odd") {
		t.Fatalf("encode error: %v", err)
	}
	if _, err := DecodeValue(codec, CodecTestMust(EncodeValue(integer, Int32Codec, 5))); err == nil {
		t.Fatal("decode error not returned")
	}
	if got, err := DecodeValue(codec, CodecTestMust(EncodeValue(integer, codec, 4))); err != nil || got != 4 {
		t.Fatalf("round trip: %v, %v", got, err)
	}
	enc := ConvertEncoder(Int32Codec, func(v string) (int32, error) { return int32(len(v)), nil })
	dec := ConvertDecoder(Int32Codec, func(v int32) (string, error) { return strings.Repeat("x", int(v)), nil })
	if got, err := DecodeValue(dec, CodecTestMust(EncodeValue(integer, enc, "abc"))); err != nil || got != "xxx" {
		t.Fatalf("ConvertEncoder/ConvertDecoder: %q, %v", got, err)
	}
	if dec.Clone("yy") != "yy" {
		t.Fatal("ConvertDecoder.Clone")
	}
}

func TestTimeValues(t *testing.T) {
	timestamp := rowTestType(t, KindTimestamp)
	if _, err := EncodeValue(timestamp, TimestampTimeCodec, time.Time{}); !errors.Is(err, errZeroTime) {
		t.Fatalf("zero time.Time: %v", err)
	}
	var unset Nullable[time.Time]
	if v, err := EncodeValue(timestamp, NullableEncoder(TimestampTimeCodec), unset); err != nil || v.State() != Null {
		t.Fatalf("invalid Nullable[time.Time]: %v", err)
	}
	// DateFromTime takes the date in the time's own location.
	east := time.Date(2026, 1, 2, 1, 0, 0, 0, time.FixedZone("east", 3*3600))
	if d := CodecTestMust(DateFromTime(east)); d.String() != "2026-01-02" {
		t.Fatalf("DateFromTime: %s", d)
	}
	if d := CodecTestMust(DateFromTime(east.UTC())); d.String() != "2026-01-01" {
		t.Fatalf("DateFromTime UTC: %s", d)
	}
}

func TestParseTimeValues(t *testing.T) {
	for in, want := range map[string]string{
		"2026-01-02": "2026-01-02", "-0001-12-31": "-0001-12-31", "+12345-06-07": "12345-06-07",
	} {
		if d, err := ParseDate(in); err != nil || d.String() != want {
			t.Errorf("ParseDate(%q) = %s, %v", in, d, err)
		}
	}
	for _, in := range []string{"2026-02-30", "2026-1-2", "x"} {
		if _, err := ParseDate(in); err == nil {
			t.Errorf("ParseDate(%q) accepted", in)
		}
	}
	for in, want := range map[string]string{
		"01:02:03": "01:02:03.000000000", "23:59:59.5": "23:59:59.500000000", "12:30": "12:30:00.000000000",
	} {
		if v, err := ParseTimeOfDay(in); err != nil || v.String() != want {
			t.Errorf("ParseTimeOfDay(%q) = %s, %v", in, v, err)
		}
	}
	for _, in := range []string{"24:00:00", "1:02:03", "01:02:03.1234567890"} {
		if _, err := ParseTimeOfDay(in); err == nil {
			t.Errorf("ParseTimeOfDay(%q) accepted", in)
		}
	}
	for in, want := range map[string]string{
		"2026-01-02T15:04:05.123Z":    "2026-01-02T15:04:05.123Z",
		"2026-01-02 15:04:05+0100":    "2026-01-02T14:04:05.000Z",
		"2026-01-02T15:04:05.5-01:30": "2026-01-02T16:34:05.500Z",
		"2026-01-02":                  "2026-01-02T00:00:00.000Z",
		"2026-01-02T15:04":            "2026-01-02T15:04:00.000Z",
		CodecTestMust(TimestampFromTime(time.UnixMilli(1234567).UTC())).String(): "1970-01-01T00:20:34.567Z",
	} {
		if v, err := ParseTimestamp(in); err != nil || v.String() != want {
			t.Errorf("ParseTimestamp(%q) = %s, %v", in, v, err)
		}
	}
	if _, err := ParseTimestamp("2026-01-02T15:04:05.0001Z"); err == nil {
		t.Error("ParseTimestamp accepted a sub-millisecond fraction")
	}
	for in, want := range map[string]Duration{
		"1y2mo3d4h5m6s7ms8us9ns": {Months: 14, Days: 3, Nanoseconds: 4*3600e9 + 5*60e9 + 6e9 + 7e6 + 8e3 + 9},
		"-2w":                    {Days: -14},
		"P1Y2M3DT4H5M6S":         {Months: 14, Days: 3, Nanoseconds: 4*3600e9 + 5*60e9 + 6e9},
		"P2W":                    {Days: 14},
		"P0001-02-03T04:05:06":   {Months: 14, Days: 3, Nanoseconds: 4*3600e9 + 5*60e9 + 6e9},
		"3µs":                    {Nanoseconds: 3000},
	} {
		if d, err := ParseDuration(in); err != nil || d != want {
			t.Errorf("ParseDuration(%q) = %+v, %v", in, d, err)
		}
	}
	for _, in := range []string{"", "5", "1x", "P", "PT", "99999999999y"} {
		if _, err := ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) accepted", in)
		}
	}
	// A duration's String parses back.
	d := Duration{Months: 25, Days: 4, Nanoseconds: 3600e9 + 1}
	if got, err := ParseDuration(d.String()); err != nil || got != d {
		t.Errorf("ParseDuration(String()) = %+v, %v", got, err)
	}
	if _, err := ParseDuration(fmt.Sprint(math.MaxInt64) + "ns"); err != nil {
		t.Errorf("max nanoseconds: %v", err)
	}
}

func TestResultDiagnosticsAreCopies(t *testing.T) {
	var r Result
	r.addWarnings("a", "b")
	r.addCQLTraces(CQLTraceRef{Attempt: 1})
	r.addCustomPayload(PayloadEntry{Key: "k"})
	r.Warnings()[0] = "changed"
	r.CQLTraces()[0].Attempt = 9
	r.CustomPayload()[0].Key = "changed"
	if r.Warnings()[0] != "a" || r.CQLTraces()[0].Attempt != 1 || r.CustomPayload()[0].Key != "k" {
		t.Fatal("a caller changed the result's diagnostics")
	}
}

func TestTextFormat(t *testing.T) {
	text := CodecTestMust(BorrowText("héllo"))
	if got := fmt.Sprintf("%s|%q|%v|%7s", text, text, text, CodecTestMust(BorrowText("ab"))); got != `héllo|"héllo"|héllo|     ab` {
		t.Fatalf("Format: %s", got)
	}
}

func TestUntypedNullValue(t *testing.T) {
	for _, kind := range []Kind{KindInt, KindText, KindBlob} {
		typ := rowTestType(t, kind)
		v, err := EncodeValue(typ, DynamicCodec, NullValue())
		if err != nil || v.State() != Null {
			t.Fatalf("NullValue as %s: %v", typ, err)
		}
	}
	tuple := CodecTestMust(TupleType(rowTestType(t, KindInt), rowTestType(t, KindText)))
	if _, err := TupleValue(tuple, []Value{IntValue(1), NullValue()}); err != nil {
		t.Fatal(err)
	}
	if _, err := NullValue().Int64(); !errors.Is(err, ErrNull) {
		t.Fatalf("NullValue().Int64(): %v", err)
	}
	if NullValue().String() != "null" || UnsetValue().String() != "unset" {
		t.Fatal("String")
	}
}

func TestStreamAllocatingDecoders(t *testing.T) {
	text, timestamp := rowTestType(t, KindText), rowTestType(t, KindTimestamp)
	var s string
	if err := StreamDecodeFixture(t, text, StreamString, streamCellBytes([]byte("héllo")), &s, 1); err != nil || s != "héllo" {
		t.Fatalf("StreamString: %q, %v", s, err)
	}
	var p *int32
	if err := StreamDecodeFixture(t, rowTestType(t, KindInt), StreamPointer(StreamInt32), streamCellBytes([]byte{0, 0, 0, 9}), &p, 1); err != nil || p == nil || *p != 9 {
		t.Fatalf("StreamPointer: %v, %v", p, err)
	}
	if err := StreamDecodeFixture(t, rowTestType(t, KindInt), StreamPointer(StreamInt32), []byte{0xff, 0xff, 0xff, 0xff}, &p, 1); err != nil || p != nil {
		t.Fatalf("StreamPointer NULL: %v, %v", p, err)
	}
	var at time.Time
	if err := StreamDecodeFixture(t, timestamp, StreamTimestampTime, streamCellBytes([]byte{0, 0, 0, 0, 0, 0, 0x03, 0xe8}), &at, 1); err != nil || !at.Equal(time.UnixMilli(1000)) {
		t.Fatalf("StreamTimestampTime: %v, %v", at, err)
	}
	m := CodecTestMust(MapType(text, rowTestType(t, KindInt)))
	body := append([]byte{0, 0, 0, 1}, streamCellBytes([]byte("k"))...)
	body = append(body, streamCellBytes([]byte{0, 0, 0, 5})...)
	var got map[string]int32
	if err := StreamDecodeFixture(t, m, StreamGoMap(StreamString, StreamInt32), streamCellBytes(body), &got, 1); err != nil || len(got) != 1 || got["k"] != 5 {
		t.Fatalf("StreamGoMap: %v, %v", got, err)
	}
	type row struct {
		Name string
		At   time.Time
		Hits *int32
	}
	columns := NewColumns(ColumnSpec{Name: "name", Type: text}, ColumnSpec{Name: "at", Type: timestamp}, ColumnSpec{Name: "hits", Type: rowTestType(t, KindInt)})
	if _, err := MustStreamRowOf[row]().Compile(columns); err != nil {
		t.Fatalf("StreamRowOf of allocating fields: %v", err)
	}
}
