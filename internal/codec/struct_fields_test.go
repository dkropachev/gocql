// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"net/netip"
	"reflect"
	"strings"
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

func TestStructMappingMatchesHandWritten(t *testing.T) {
	columns := structFixtureColumns(t)
	value := structFixtureValue()
	wantParams, wantRows := handWrittenFixture()
	gotParams, err := ReflectParams[structFixture]()
	if err != nil {
		t.Fatal(err)
	}
	gotRows, err := ReflectRow[structFixture]()
	if err != nil {
		t.Fatal(err)
	}
	want := encodeTestParams(t, columns, wantParams, value)
	got := encodeTestParams(t, columns, gotParams, value)
	if !bytes.Equal(chainBytes(t, want), chainBytes(t, got)) {
		t.Fatalf("reflected params encode differently:\nwant %x\ngot  %x", chainBytes(t, want), chainBytes(t, got))
	}
	wantRow := decodeTestRow(t, columns, wantRows, want)
	gotRow := decodeTestRow(t, columns, gotRows, got)
	if !reflect.DeepEqual(wantRow, gotRow) {
		t.Fatalf("reflected row decodes differently:\nwant %+v\ngot  %+v", wantRow, gotRow)
	}
	value.Skipped, value.internal = "", 0
	if !reflect.DeepEqual(gotRow, value) {
		t.Fatalf("round trip:\nwant %+v\ngot  %+v", value, gotRow)
	}
}

// The reflected fields are ordinary Param/Column fields, so built-in codecs
// still compile to direct ops.
func TestStructMappingKeepsDirectOps(t *testing.T) {
	columns := structFixtureColumns(t)
	params, _ := ReflectParams[structFixture]()
	plan, err := params.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	direct := 0
	for _, op := range plan.(paramsPlan[structFixture]).ops {
		if op.kind != directNone {
			direct++
		}
	}
	if direct == 0 {
		t.Fatal("no reflected parameter compiled to a direct op")
	}
	rows, _ := ReflectRow[structFixture]()
	rowPlanValue, err := rows.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	direct = 0
	for _, op := range rowPlanValue.(rowPlan[structFixture]).ops {
		if op.kind != directNone {
			direct++
		}
	}
	if direct == 0 {
		t.Fatal("no reflected column compiled to a direct op")
	}
}

// A reflected mapping encodes without allocating, like a hand-written one,
// and decodes with the same allocations.
func TestStructMappingAllocatesLikeHandWritten(t *testing.T) {
	type small struct {
		ID   int64
		Name string
		Blob []byte
	}
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "name", Type: rowTestType(t, cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "blob", Type: rowTestType(t, cqltype.TypeBlob)},
	)
	hand := Params(
		Param("id", Int64, func(v small) int64 { return v.ID }),
		Param("name", StringCodec, func(v small) string { return v.Name }),
		Param("blob", BytesEncoder, func(v small) []byte { return v.Blob }),
	)
	handRows := Row(
		Column("id", Int64, func(v *small) *int64 { return &v.ID }),
		Column("name", StringCodec, func(v *small) *string { return &v.Name }),
		Column("blob", OwnedBlobCodec, func(v *small) *[]byte { return &v.Blob }),
	)
	value := small{ID: 1, Name: "x", Blob: []byte{1}}
	encodeAllocs := func(params ParamEncoder[small]) float64 {
		plan, err := params.Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		builder := wire.NewBuilder(64)
		var w ParamWriter
		var storage []byte
		return testing.AllocsPerRun(100, func() {
			builder.BeginOwned(storage)
			ResetParamWriter(&w, builder, columns, false)
			if err := plan.Encode(&w, value); err != nil {
				t.Fatal(err)
			}
			storage = builder.OwnedBytes()[:0]
		})
	}
	if hand, reflected := encodeAllocs(hand), encodeAllocs(MustParamsOf[small]()); hand != 0 || reflected != 0 {
		t.Fatalf("params encode allocations: hand-written %.1f, reflected %.1f", hand, reflected)
	}
	chain := encodeTestParams(t, columns, hand, value)
	decodeAllocs := func(rows RowDecoder[small]) float64 {
		plan, err := rows.Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		var dst small
		return testing.AllocsPerRun(100, func() {
			reader := RowReader{columns: columns, cursor: wire.NewCursor(chain)}
			if err := plan.DecodeInto(&reader, &dst); err != nil {
				t.Fatal(err)
			}
		})
	}
	if hand, reflected := decodeAllocs(handRows), decodeAllocs(MustRowOf[small]()); reflected != hand {
		t.Fatalf("row decode allocations: hand-written %.1f, reflected %.1f", hand, reflected)
	}
}

func TestStructMappingSubsetOfColumns(t *testing.T) {
	type tweet struct {
		ID       int64
		Timeline string
		Text     string
	}
	update := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "text", Type: rowTestType(t, cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
	)
	value := tweet{ID: 3, Timeline: "unused", Text: "edited"}
	chain := encodeTestParams(t, update, MustParamsOf[tweet](), value)
	row := decodeTestRow(t, update, MustRowOf[tweet](), chain)
	if row != (tweet{ID: 3, Text: "edited"}) {
		t.Fatalf("subset round trip: %+v", row)
	}
	// Every column still needs a field.
	extra := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "author", Type: rowTestType(t, cqltype.TypeText)},
	)
	if _, err := MustParamsOf[tweet]().Compile(extra); err == nil || !strings.Contains(err.Error(), "author") {
		t.Fatalf("unmapped column: %v", err)
	}
	if _, err := MustRowOf[tweet]().Compile(extra); err == nil || !strings.Contains(err.Error(), "author") {
		t.Fatalf("unmapped result column: %v", err)
	}
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

func TestStructMappingWithCodec(t *testing.T) {
	type shape struct {
		Origin structPoint
		Corner cqltype.Nullable[structPoint]
	}
	if _, err := ReflectParams[shape](); err == nil || !strings.Contains(err.Error(), "WithCodec") {
		t.Fatalf("unsupported type: %v", err)
	}
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "origin", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "corner", Type: rowTestType(t, cqltype.TypeBigInt)},
	)
	value := shape{Origin: structPoint{1, -2}, Corner: cqltype.Nullable[structPoint]{Value: structPoint{3, 4}, Valid: true}}
	chain := encodeTestParams(t, columns, MustParamsOf(WithCodec[shape](Codec[structPoint](pointCodec{}))), value)
	row := decodeTestRow(t, columns, MustRowOf(WithCodec[shape](Codec[structPoint](pointCodec{}))), chain)
	if row != value {
		t.Fatalf("round trip: %+v", row)
	}
}

func TestStructMappingErrors(t *testing.T) {
	type embedded struct{ *EmbeddedFixture }
	type duplicate struct {
		A int32 `cql:"x"`
		B int32 `cql:"x"`
	}
	type badOption struct {
		A int32 `cql:"a,omitempty"`
	}
	type badCodec struct {
		A string `cql:"a,codec=Int32"`
	}
	for name, err := range map[string]error{
		"not a struct":     func() error { _, err := ReflectParams[int](); return err }(),
		"embedded pointer": func() error { _, err := ReflectParams[embedded](); return err }(),
		"both map":         func() error { _, err := ReflectParams[duplicate](); return err }(),
		"omitempty":        func() error { _, err := ReflectRow[badOption](); return err }(),
		"\"Int32\"":        func() error { _, err := ReflectRow[badCodec](); return err }(),
	} {
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestStructMappingPrefersRegistered(t *testing.T) {
	type registered struct{ ID int64 }
	called := false
	RegisterStruct(StructFields[registered]{
		Params: []InputField[registered]{Param("id", Int64, func(v registered) int64 { called = true; return v.ID })},
	})
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)})
	params := MustParamsOf[registered]()
	plan, err := params.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Encode(NewParamWriter(wire.NewBuilder(8), columns, false), registered{ID: 1}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("ParamsOf did not use the registered mapping")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("registering twice did not panic")
		}
	}()
	RegisterStruct(StructFields[registered]{})
}

func TestStructMappingReuseBuffers(t *testing.T) {
	type blobRow struct {
		ID    int64
		Blob  []byte
		Other []byte // not selected: emptied, keeping its storage
	}
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "blob", Type: rowTestType(t, cqltype.TypeBlob)},
	)
	chain := encodeTestParams(t, columns, MustParamsOf[blobRow](), blobRow{ID: 1, Blob: []byte("payload")})
	storage := make([]byte, 0, 16)
	decodeAllocs := func(rows RowDecoder[blobRow]) (float64, blobRow) {
		plan, err := rows.Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		dst := blobRow{Blob: storage, Other: []byte("stale")}
		allocs := testing.AllocsPerRun(100, func() {
			reader := RowReader{columns: columns, cursor: wire.NewCursor(chain)}
			if err := plan.DecodeInto(&reader, &dst); err != nil {
				t.Fatal(err)
			}
		})
		return allocs, dst
	}
	fresh, _ := decodeAllocs(MustRowOf[blobRow]())
	reused, row := decodeAllocs(MustRowOf(ReuseBuffers[blobRow]()))
	// The reader in this harness allocates on its own; the blob is the difference.
	if reused != fresh-1 {
		t.Fatalf("blob decode allocations: default %.1f, ReuseBuffers %.1f", fresh, reused)
	}
	if &row.Blob[:1][0] != &storage[:1][0] {
		t.Fatal("ReuseBuffers did not decode into the seeded storage")
	}
	if row.ID != 1 || string(row.Blob) != "payload" || len(row.Other) != 0 {
		t.Fatalf("ReuseBuffers row: %+v", row)
	}
}

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

// Value-embedded structs map their fields as the outer struct's, at their
// offset in it; an outer field hides an embedded one with the same column.
func TestStructMappingEmbedded(t *testing.T) {
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "createdat", Type: rowTestType(t, cqltype.TypeTimestamp)},
		cqltype.ColumnSpec{Name: "updatedat", Type: rowTestType(t, cqltype.TypeTimestamp)},
		cqltype.ColumnSpec{Name: "owner", Type: rowTestType(t, cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "text", Type: rowTestType(t, cqltype.TypeText)},
	)
	value := embeddedRow{ID: 1, auditFields: auditFields{CreatedAt: 2, UpdatedAt: 3, ID: 99}, EmbeddedOwner: EmbeddedOwner{"me"}, Text: "t"}
	chain := encodeTestParams(t, columns, MustParamsOf[embeddedRow](), value)
	row := decodeTestRow(t, columns, MustRowOf[embeddedRow](), chain)
	value.auditFields.ID = 0 // hidden: neither encoded nor decoded
	if row != value {
		t.Fatalf("round trip:\nwant %+v\ngot  %+v", value, row)
	}
	type ambiguous struct {
		auditFields
		Inner
	}
	if _, err := ReflectRow[ambiguous](); err == nil || !strings.Contains(err.Error(), "both map column \"createdat\"") {
		t.Fatalf("same-depth conflict: %v", err)
	}
	type tagged struct {
		EmbeddedOwner `cql:"owner"`
	}
	if _, err := ReflectRow[tagged](); err == nil || !strings.Contains(err.Error(), "only the tag") {
		t.Fatalf("tagged embedded field: %v", err)
	}
}

// Inner embeds a CreatedAt at the same depth as auditFields'.
type Inner struct{ CreatedAt cqltype.Timestamp }
