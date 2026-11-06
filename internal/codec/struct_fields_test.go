// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"net/netip"
	"testing"
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
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
	ID        cqltype.UUID `cql:"id"`
	TimeID    cqltype.TimeUUID
	Addr      netip.Addr
	Hits      cqltype.Counter
	Day       cqltype.Date
	Clock     cqltype.TimeOfDay
	At        cqltype.Timestamp
	Span      cqltype.Duration
	MaybeInt  cqltype.Nullable[int32]
	MaybeText cqltype.Nullable[string]
	Skipped   string `cql:"-"`
	internal  int
}

func structFixtureColumns(t testing.TB) cqltype.Columns {
	t.Helper()
	spec := func(name string, kind cqltype.Type) cqltype.ColumnSpec {
		return cqltype.ColumnSpec{Name: name, Type: rowTestType(t, kind)}
	}
	// Server order differs from field order: mapping is by name.
	return cqltype.NewColumns(
		spec("id", cqltype.TypeUUID),
		spec("text", cqltype.TypeText),
		spec("bool", cqltype.TypeBoolean),
		spec("tiny", cqltype.TypeTinyInt),
		spec("small", cqltype.TypeSmallInt),
		spec("int", cqltype.TypeInt),
		spec("big_value", cqltype.TypeBigInt),
		spec("float", cqltype.TypeFloat),
		spec("double", cqltype.TypeDouble),
		spec("ascii", cqltype.TypeAscii),
		spec("blob", cqltype.TypeBlob),
		spec("timeid", cqltype.TypeTimeUUID),
		spec("addr", cqltype.TypeInet),
		spec("hits", cqltype.TypeCounter),
		spec("day", cqltype.TypeDate),
		spec("clock", cqltype.TypeTime),
		spec("at", cqltype.TypeTimestamp),
		spec("span", cqltype.TypeDuration),
		spec("maybeint", cqltype.TypeInt),
		spec("maybetext", cqltype.TypeText),
	)
}

func structFixtureValue() structFixture {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	return structFixture{
		Bool: true, Tiny: -8, Small: 1600, Int: -32000, Big: 1 << 40,
		Float: 1.5, Double: -2.25, Text: "héllo", ASCII: "plain", Blob: []byte{1, 2, 3},
		ID: cqltype.UUID{1, 2, 3, 4, 5, 6, 0x40, 8, 0x80, 10, 11, 12, 13, 14, 15, 16}, TimeID: cqltype.TimeUUID(cqltype.UUIDFromTime(now)),
		Addr: netip.MustParseAddr("10.0.0.1"), Hits: 7, Day: 1<<31 - 5, Clock: 3600e9, At: cqltype.Timestamp(now.UnixMilli()),
		Span:     cqltype.Duration{Months: 1, Days: 2, Nanoseconds: 3},
		MaybeInt: cqltype.Nullable[int32]{Value: 9, Valid: true},
	}
}

// handWrittenFixture is the mapping an application writes by hand today.
func handWrittenFixture() (ParamEncoder[structFixture], RowDecoder[structFixture]) {
	params := Params(
		Param("bool", Bool, func(v structFixture) bool { return v.Bool }),
		Param("tiny", Int8, func(v structFixture) int8 { return v.Tiny }),
		Param("small", Int16, func(v structFixture) int16 { return v.Small }),
		Param("int", Int32, func(v structFixture) int32 { return v.Int }),
		Param("big_value", Int64, func(v structFixture) int64 { return v.Big }),
		Param("float", Float32, func(v structFixture) float32 { return v.Float }),
		Param("double", Float64, func(v structFixture) float64 { return v.Double }),
		Param("text", StringCodec, func(v structFixture) string { return v.Text }),
		Param("ascii", ASCIIStringCodec, func(v structFixture) string { return v.ASCII }),
		Param("blob", BytesEncoder, func(v structFixture) []byte { return v.Blob }),
		Param("id", UUIDCodec, func(v structFixture) cqltype.UUID { return v.ID }),
		Param("timeid", TimeUUIDCodec, func(v structFixture) cqltype.TimeUUID { return v.TimeID }),
		Param("addr", InetCodec, func(v structFixture) netip.Addr { return v.Addr }),
		Param("hits", CounterCodec, func(v structFixture) cqltype.Counter { return v.Hits }),
		Param("day", DateCodec, func(v structFixture) cqltype.Date { return v.Day }),
		Param("clock", TimeCodec, func(v structFixture) cqltype.TimeOfDay { return v.Clock }),
		Param("at", TimestampCodec, func(v structFixture) cqltype.Timestamp { return v.At }),
		Param("span", DurationCodec, func(v structFixture) cqltype.Duration { return v.Span }),
		Param("maybeint", NullableEncoder(Int32), func(v structFixture) cqltype.Nullable[int32] { return v.MaybeInt }),
		Param("maybetext", NullableEncoder(StringCodec), func(v structFixture) cqltype.Nullable[string] { return v.MaybeText }),
	)
	rows := Row(
		Column("bool", Bool, func(v *structFixture) *bool { return &v.Bool }),
		Column("tiny", Int8, func(v *structFixture) *int8 { return &v.Tiny }),
		Column("small", Int16, func(v *structFixture) *int16 { return &v.Small }),
		Column("int", Int32, func(v *structFixture) *int32 { return &v.Int }),
		Column("big_value", Int64, func(v *structFixture) *int64 { return &v.Big }),
		Column("float", Float32, func(v *structFixture) *float32 { return &v.Float }),
		Column("double", Float64, func(v *structFixture) *float64 { return &v.Double }),
		Column("text", StringCodec, func(v *structFixture) *string { return &v.Text }),
		Column("ascii", ASCIIStringCodec, func(v *structFixture) *string { return &v.ASCII }),
		Column("blob", OwnedBlobCodec, func(v *structFixture) *[]byte { return &v.Blob }),
		Column("id", UUIDCodec, func(v *structFixture) *cqltype.UUID { return &v.ID }),
		Column("timeid", TimeUUIDCodec, func(v *structFixture) *cqltype.TimeUUID { return &v.TimeID }),
		Column("addr", InetCodec, func(v *structFixture) *netip.Addr { return &v.Addr }),
		Column("hits", CounterCodec, func(v *structFixture) *cqltype.Counter { return &v.Hits }),
		Column("day", DateCodec, func(v *structFixture) *cqltype.Date { return &v.Day }),
		Column("clock", TimeCodec, func(v *structFixture) *cqltype.TimeOfDay { return &v.Clock }),
		Column("at", TimestampCodec, func(v *structFixture) *cqltype.Timestamp { return &v.At }),
		Column("span", DurationCodec, func(v *structFixture) *cqltype.Duration { return &v.Span }),
		Column("maybeint", NullableDecoder(Int32), func(v *structFixture) *cqltype.Nullable[int32] { return &v.MaybeInt }),
		Column("maybetext", NullableDecoder(StringCodec), func(v *structFixture) *cqltype.Nullable[string] { return &v.MaybeText }),
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

func decodeTestRow[R any](t testing.TB, columns cqltype.Columns, rows RowDecoder[R], chain wire.Chain) R {
	t.Helper()
	plan, err := rows.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	reader := NewRowReader(columns, chain, wire.NewLease(chain, nil))
	row, err := plan.Decode(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := FinishRowReader(reader); err != nil {
		t.Fatal(err)
	}
	return row
}

type structPoint struct{ X, Y int32 }

type EmbeddedFixture struct{ X int32 }

type pointCodec struct{}

func (pointCodec) CheckType(t cqltype.DataType) error { return Int64.CheckType(t) }
func (pointCodec) Encode(w *ValueWriter, v structPoint) error {
	return Int64.Encode(w, int64(v.X)<<32|int64(uint32(v.Y)))
}
func (pointCodec) Decode(r ValueReader) (structPoint, error) {
	v, err := Int64.Decode(r)
	return structPoint{X: int32(v >> 32), Y: int32(v)}, err
}
func (pointCodec) Clone(v structPoint) structPoint { return v }

type auditFields struct {
	CreatedAt cqltype.Timestamp
	UpdatedAt cqltype.Timestamp
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
type Inner struct{ CreatedAt cqltype.Timestamp }
