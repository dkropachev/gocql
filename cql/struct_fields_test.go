// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"net/netip"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type structFixture struct {
	Bool      bool
	Tiny      int8  `cql:"tiny"`
	Small     int16 `cql:"small"`
	Int       int32
	Big       int64 `cql:"big_value"`
	Float     float32
	Double    float64
	Text      string
	ASCII     string `cql:"ascii,codec=ASCIIStringCodec"`
	Blob      []byte
	ID        UUID `cql:"id"`
	TimeID    TimeUUID
	Addr      netip.Addr
	Hits      int64
	Day       Date
	Clock     TimeOfDay
	At        Timestamp
	Span      Duration
	MaybeInt  Nullable[int32]
	MaybeText Nullable[string]
	Skipped   string `cql:"-"`
	internal  int
}

func structFixtureColumns(t testing.TB) Columns {
	t.Helper()
	spec := func(name string, kind Type) ColumnSpec {
		return ColumnSpec{Name: name, Type: rowTestType(t, kind)}
	}
	// Server order differs from field order: mapping is by name.
	return NewColumns(
		spec("id", TypeUUID),
		spec("text", TypeText),
		spec("bool", TypeBoolean),
		spec("tiny", TypeTinyInt),
		spec("small", TypeSmallInt),
		spec("int", TypeInt),
		spec("big_value", TypeBigInt),
		spec("float", TypeFloat),
		spec("double", TypeDouble),
		spec("ascii", TypeASCII),
		spec("blob", TypeBlob),
		spec("time_id", TypeTimeUUID),
		spec("addr", TypeInet),
		spec("hits", TypeCounter),
		spec("day", TypeDate),
		spec("clock", TypeTime),
		spec("at", TypeTimestamp),
		spec("span", TypeDuration),
		spec("maybe_int", TypeInt),
		spec("maybe_text", TypeText),
	)
}

func structFixtureValue() structFixture {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	return structFixture{
		Bool: true, Tiny: -8, Small: 1600, Int: -32000, Big: 1 << 40,
		Float: 1.5, Double: -2.25, Text: "héllo", ASCII: "plain", Blob: []byte{1, 2, 3},
		ID: UUID{1, 2, 3, 4, 5, 6, 0x40, 8, 0x80, 10, 11, 12, 13, 14, 15, 16}, TimeID: TimeUUIDFromTime(now),
		Addr: netip.MustParseAddr("10.0.0.1"), Hits: 7, Day: 1<<31 - 5, Clock: 3600e9, At: Timestamp(now.UnixMilli()),
		Span:     Duration{Months: 1, Days: 2, Nanoseconds: 3},
		MaybeInt: Nullable[int32]{Value: 9, Valid: true},
	}
}

// handWrittenFixture is the mapping an application writes by hand today.
func handWrittenFixture() (ParamEncoder[structFixture], RowDecoder[structFixture]) {
	params := Params(
		Param("bool", BoolCodec, func(v structFixture) bool { return v.Bool }),
		Param("tiny", Int8Codec, func(v structFixture) int8 { return v.Tiny }),
		Param("small", Int16Codec, func(v structFixture) int16 { return v.Small }),
		Param("int", Int32Codec, func(v structFixture) int32 { return v.Int }),
		Param("big_value", Int64Codec, func(v structFixture) int64 { return v.Big }),
		Param("float", Float32Codec, func(v structFixture) float32 { return v.Float }),
		Param("double", Float64Codec, func(v structFixture) float64 { return v.Double }),
		Param("text", StringCodec, func(v structFixture) string { return v.Text }),
		Param("ascii", StringCodec, func(v structFixture) string { return v.ASCII }),
		Param("blob", ByteSliceCodec, func(v structFixture) []byte { return v.Blob }),
		Param("id", UUIDCodec, func(v structFixture) UUID { return v.ID }),
		Param("time_id", TimeUUIDCodec, func(v structFixture) TimeUUID { return v.TimeID }),
		Param("addr", InetCodec, func(v structFixture) netip.Addr { return v.Addr }),
		Param("hits", Int64Codec, func(v structFixture) int64 { return v.Hits }),
		Param("day", DateCodec, func(v structFixture) Date { return v.Day }),
		Param("clock", TimeCodec, func(v structFixture) TimeOfDay { return v.Clock }),
		Param("at", TimestampCodec, func(v structFixture) Timestamp { return v.At }),
		Param("span", DurationCodec, func(v structFixture) Duration { return v.Span }),
		Param("maybe_int", NullableEncoder(Int32Codec), func(v structFixture) Nullable[int32] { return v.MaybeInt }),
		Param("maybe_text", NullableEncoder(StringCodec), func(v structFixture) Nullable[string] { return v.MaybeText }),
	)
	rows := Row(
		Column("bool", BoolCodec, func(v *structFixture) *bool { return &v.Bool }),
		Column("tiny", Int8Codec, func(v *structFixture) *int8 { return &v.Tiny }),
		Column("small", Int16Codec, func(v *structFixture) *int16 { return &v.Small }),
		Column("int", Int32Codec, func(v *structFixture) *int32 { return &v.Int }),
		Column("big_value", Int64Codec, func(v *structFixture) *int64 { return &v.Big }),
		Column("float", Float32Codec, func(v *structFixture) *float32 { return &v.Float }),
		Column("double", Float64Codec, func(v *structFixture) *float64 { return &v.Double }),
		Column("text", StringCodec, func(v *structFixture) *string { return &v.Text }),
		Column("ascii", StringCodec, func(v *structFixture) *string { return &v.ASCII }),
		Column("blob", ByteSliceCodec, func(v *structFixture) *[]byte { return &v.Blob }),
		Column("id", UUIDCodec, func(v *structFixture) *UUID { return &v.ID }),
		Column("time_id", TimeUUIDCodec, func(v *structFixture) *TimeUUID { return &v.TimeID }),
		Column("addr", InetCodec, func(v *structFixture) *netip.Addr { return &v.Addr }),
		Column("hits", Int64Codec, func(v *structFixture) *int64 { return &v.Hits }),
		Column("day", DateCodec, func(v *structFixture) *Date { return &v.Day }),
		Column("clock", TimeCodec, func(v *structFixture) *TimeOfDay { return &v.Clock }),
		Column("at", TimestampCodec, func(v *structFixture) *Timestamp { return &v.At }),
		Column("span", DurationCodec, func(v *structFixture) *Duration { return &v.Span }),
		Column("maybe_int", NullableDecoder(Int32Codec), func(v *structFixture) *Nullable[int32] { return &v.MaybeInt }),
		Column("maybe_text", NullableDecoder(StringCodec), func(v *structFixture) *Nullable[string] { return &v.MaybeText }),
	)
	return params, rows
}

func chainBytes(t testing.TB, chain wire.Chain) []byte {
	t.Helper()
	buf := make([]byte, chain.Len())
	if _, err := chain.CopyTo(buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func decodeTestRow[R any](t testing.TB, columns Columns, rows RowDecoder[R], chain wire.Chain) R {
	t.Helper()
	plan, err := rows.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	reader := newRowReader(columns, chain, wire.NewLease(chain, nil))
	row, err := decodeRow(plan, reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := finishRowReader(reader); err != nil {
		t.Fatal(err)
	}
	return row
}

type structPoint struct{ X, Y int32 }

type EmbeddedFixture struct{ X int32 }

type pointCodec struct{}

func (pointCodec) CheckType(t DataType) error { return Int64Codec.CheckType(t) }
func (pointCodec) Encode(w *ValueWriter, v structPoint) error {
	return Int64Codec.Encode(w, int64(v.X)<<32|int64(uint32(v.Y)))
}
func (pointCodec) Decode(r ValueReader) (structPoint, error) {
	v, err := Int64Codec.Decode(r)
	return structPoint{X: int32(v >> 32), Y: int32(v)}, err
}
func (pointCodec) Clone(v structPoint) structPoint { return v }

type auditFields struct {
	CreatedAt Timestamp
	UpdatedAt Timestamp
	ID        int64 // hidden by embeddedRow.ID
}

type EmbeddedOwner struct {
	Owner string
}

type embeddedRow struct {
	ID int64
	auditFields
	EmbeddedOwner
	Skipped EmbeddedFixture `cql:"-"`
	Text    string
}

// Inner embeds a CreatedAt at the same depth as auditFields'.
type Inner struct{ CreatedAt Timestamp }
