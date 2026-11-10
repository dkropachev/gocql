// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStructMapping(t *testing.T) {
	// Scylla sends the previous row's columns even when a conditional statement
	// applied; all null, they decode as no previous row, so a struct with
	// non-nullable fields still decodes the result.
	t.Run("CASResult", func(t *testing.T) {
		type previous struct {
			K int32
			V string
		}
		columns := cqltype.NewColumns(
			cqltype.ColumnSpec{Name: "[applied]", Type: rowTestType(t, cqltype.TypeBoolean)},
			cqltype.ColumnSpec{Name: "k", Type: rowTestType(t, cqltype.TypeInt)},
			cqltype.ColumnSpec{Name: "v", Type: rowTestType(t, cqltype.TypeText)},
		)
		plan, err := CASRows(MustRowOf[previous]()).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		decode := func(cells ...[]byte) (CASResult[previous], error) {
			var data []byte
			for _, c := range cells {
				data = append(data, c...)
			}
			chain, err := wire.NewChain(wire.Borrow(data))
			if err != nil {
				t.Fatal(err)
			}
			reader := NewRowReader(columns, chain, wire.NewLease(chain, nil))
			result, err := plan.Decode(reader)
			if err == nil {
				err = FinishRowReader(reader)
			}
			return result, err
		}
		null := rowScalarFrame(-1, nil)
		if got, err := decode(rowScalarFrame(1, []byte{1}), null, null); err != nil || !got.Applied || got.Previous.Set {
			t.Fatalf("applied with null previous: %+v %v", got, err)
		}
		got, err := decode(rowScalarFrame(1, []byte{0}), rowScalarFrame(4, []byte{0, 0, 0, 7}), rowScalarFrame(1, []byte("x")))
		if err != nil || got.Applied || !got.Previous.Set || got.Previous.Value != (previous{K: 7, V: "x"}) {
			t.Fatalf("not applied: %+v %v", got, err)
		}
		// An applied result with a previous value still decodes it.
		got, err = decode(rowScalarFrame(1, []byte{1}), rowScalarFrame(4, []byte{0, 0, 0, 8}), rowScalarFrame(1, []byte("y")))
		if err != nil || !got.Applied || got.Previous.Value != (previous{K: 8, V: "y"}) {
			t.Fatalf("applied with previous: %+v %v", got, err)
		}
		// A null previous row of a statement that did not apply still fails on
		// non-nullable fields, as before.
		if _, err := decode(rowScalarFrame(1, []byte{0}), null, null); err == nil {
			t.Fatal("not applied with null previous decoded into non-nullable fields")
		}
	})
	t.Run("Collections", func(t *testing.T) {
		// CQL stores an empty collection as null; both decode as an empty slice.
		t.Run("NullDecodesAsEmpty", func(t *testing.T) {
			columns := collectionColumns(t)
			nulls := Params(
				Param("id", Int64, func(collectionRow) int64 { return 1 }),
				Param("names", NullableEncoder(ListEncoder(StringCodec)), func(collectionRow) cqltype.Nullable[[]string] { return cqltype.Nullable[[]string]{} }),
				Param("scores", SetEncoder(Int32), func(collectionRow) []int32 { return nil }),
				Param("blobs", NullableEncoder(ListEncoder(BytesEncoder)), func(collectionRow) cqltype.Nullable[[][]byte] { return cqltype.Nullable[[][]byte]{} }),
				Param("attrs", NullableEncoder(MapEncoder(StringCodec, Int64)), func(collectionRow) cqltype.Nullable[[]Entry[string, int64]] {
					return cqltype.Nullable[[]Entry[string, int64]]{}
				}),
			)
			row := decodeTestRow(t, columns, MustRowOf(attrsCodec), encodeTestParams(t, columns, nulls, collectionRow{}))
			if len(row.Names) != 0 || len(row.Scores) != 0 || len(row.Blobs) != 0 || len(row.Attrs) != 0 {
				t.Fatalf("null collections: %+v", row)
			}
		})
		// With ReuseBuffers, decoding into the same row keeps the storage of slice
		// fields and of their []byte elements.
		t.Run("ReuseBuffers", func(t *testing.T) {
			type blobsRow struct {
				ID    int64
				Blobs [][]byte
			}
			columns := cqltype.NewColumns(
				cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
				cqltype.ColumnSpec{Name: "blobs", Type: CodecTestMust(cqltype.ListType(rowTestType(t, cqltype.TypeBlob)))},
			)
			chain := encodeTestParams(t, columns, MustParamsOf[blobsRow](), blobsRow{ID: 1, Blobs: [][]byte{{1, 2}, {3}}})
			allocs := func(rows RowDecoder[blobsRow]) (float64, blobsRow) {
				plan, err := rows.Compile(columns)
				if err != nil {
					t.Fatal(err)
				}
				dst := blobsRow{Blobs: [][]byte{make([]byte, 0, 8), make([]byte, 0, 8)}[:0]}
				n := testing.AllocsPerRun(100, func() {
					reader := RowReader{columns: columns, cursor: wire.NewCursor(chain)}
					if err := plan.DecodeInto(&reader, &dst); err != nil {
						t.Fatal(err)
					}
				})
				return n, dst
			}
			fresh, _ := allocs(MustRowOf[blobsRow]())
			reused, row := allocs(MustRowOf(ReuseBuffers[blobsRow]()))
			// Without reuse: the outer slice and both elements.
			if fresh-reused != 3 {
				t.Fatalf("allocations: default %.1f, ReuseBuffers %.1f", fresh, reused)
			}
			if !reflect.DeepEqual(row.Blobs, [][]byte{{1, 2}, {3}}) {
				t.Fatalf("reused row: %+v", row)
			}
		})
		t.Run("RoundTrip", func(t *testing.T) {
			columns := collectionColumns(t)
			value := collectionRow{
				ID: 1, Names: []string{"a", "bc"}, Scores: []int32{3, 1},
				Blobs: [][]byte{{1}, {2, 3}}, Attrs: []Entry[string, int64]{{"x", 1}, {"y", 2}},
			}
			chain := encodeTestParams(t, columns, MustParamsOf(attrsCodec), value)
			// The bytes match the existing slice encoders.
			hand := Params(
				Param("id", Int64, func(v collectionRow) int64 { return v.ID }),
				Param("names", ListEncoder(StringCodec), func(v collectionRow) []string { return v.Names }),
				Param("scores", SetEncoder(Int32), func(v collectionRow) []int32 { return v.Scores }),
				Param("blobs", ListEncoder(BytesEncoder), func(v collectionRow) [][]byte { return v.Blobs }),
				Param("attrs", MapEncoder(StringCodec, Int64), func(v collectionRow) []Entry[string, int64] { return v.Attrs }),
			)
			if got, want := chainBytes(t, chain), chainBytes(t, encodeTestParams(t, columns, hand, value)); !reflect.DeepEqual(got, want) {
				t.Fatalf("collection encoding:\ngot  %x\nwant %x", got, want)
			}
			rows := MustRowOf(attrsCodec)
			row := decodeTestRow(t, columns, rows, chain)
			if !reflect.DeepEqual(row, value) {
				t.Fatalf("round trip:\nwant %+v\ngot  %+v", value, row)
			}
			clone := rows.Clone(row)
			clone.Blobs[0][0], clone.Names[0] = 9, "z"
			if row.Blobs[0][0] != 1 || row.Names[0] != "a" {
				t.Fatal("Clone shares collection storage")
			}
		})
	})
	t.Run("Reflect", func(t *testing.T) {
		// A reflected mapping encodes without allocating, like a hand-written one,
		// and decodes with the same allocations.
		t.Run("AllocatesLikeHandWritten", func(t *testing.T) {
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
		})
		// Value-embedded structs map their fields as the outer struct's, at their
		// offset in it; an outer field hides an embedded one with the same column.
		t.Run("Embedded", func(t *testing.T) {
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
		})
		t.Run("Errors", func(t *testing.T) {
			type embedded struct{ *EmbeddedFixture }
			type duplicate struct {
				A int32 `cql:"x"`
				B int32 `cql:"x"`
			}
			type badOption struct {
				A int32 `cql:"a,omitempty"`
			}
			type badCodec struct {
				A string `cql:"a,codec=Int32Codec"`
			}
			for name, err := range map[string]error{
				"not a struct":     func() error { _, err := ReflectParams[int](); return err }(),
				"embedded pointer": func() error { _, err := ReflectParams[embedded](); return err }(),
				"both map":         func() error { _, err := ReflectParams[duplicate](); return err }(),
				"omitempty":        func() error { _, err := ReflectRow[badOption](); return err }(),
				"\"Int32Codec\"":   func() error { _, err := ReflectRow[badCodec](); return err }(),
			} {
				if err == nil || !strings.Contains(err.Error(), name) {
					t.Errorf("%s: got %v", name, err)
				}
			}
		})
		// The reflected fields are ordinary Param/Column fields, so built-in codecs
		// still compile to direct ops.
		t.Run("KeepsDirectOps", func(t *testing.T) {
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
		})
		t.Run("MatchesHandWritten", func(t *testing.T) {
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
		})
		t.Run("ReuseBuffers", func(t *testing.T) {
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
		})
		t.Run("SubsetOfColumns", func(t *testing.T) {
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
		})
		t.Run("WithCodec", func(t *testing.T) {
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
		})
	})
	t.Run("RegisterStruct", func(t *testing.T) {
		type registered struct{ ID int64 }
		// The registry is process-wide; forget the mapping so that -count>1
		// registers it afresh instead of panicking on the second run.
		t.Cleanup(func() { structRegistry.Delete(reflect.TypeFor[registered]()) })
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
	})
	t.Run("StreamRowOf", func(t *testing.T) {
		t.Run("MatchesHandWritten", func(t *testing.T) {
			columns := streamFixtureColumns(t)
			value := streamFixture{
				ID: 7, Key: cqltype.UUID{1, 2, 3}, Payload: []byte("payload"), Name: []byte("héllo"),
				Pieces: [][]byte{[]byte("a"), []byte("bc")}, Tags: []int32{1, 2},
				Maybe: cqltype.Nullable[int64]{Value: 5, Valid: true}, At: 1234,
			}
			data := streamFixtureRow(t, columns, value)
			hand := StreamRow(func(*streamFixture) {},
				StreamColumn("id", StreamInt32, func(v *streamFixture) *int32 { return &v.ID }),
				StreamColumn("key", StreamUUID, func(v *streamFixture) *cqltype.UUID { return &v.Key }),
				StreamColumn("payload", StreamBlob, func(v *streamFixture) *[]byte { return &v.Payload }),
				StreamColumn("name", StreamText, func(v *streamFixture) *[]byte { return &v.Name }),
				StreamColumn("pieces", StreamList(StreamBlob), func(v *streamFixture) *[][]byte { return &v.Pieces }),
				StreamColumn("tags", StreamSet(StreamInt32), func(v *streamFixture) *[]int32 { return &v.Tags }),
				StreamColumn("maybe", StreamNullable(StreamInt64), func(v *streamFixture) *cqltype.Nullable[int64] { return &v.Maybe }),
				StreamColumn("at", StreamTimestamp, func(v *streamFixture) *cqltype.Timestamp { return &v.At }),
			)
			want, got := seededStreamFixture(), seededStreamFixture()
			streamDecodeRow(t, columns, hand, data, &want)
			streamDecodeRow(t, columns, MustStreamRowOf[streamFixture](), data, &got)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("reflected stream row decodes differently:\nwant %+v\ngot  %+v", want, got)
			}
			if !reflect.DeepEqual(got, value) {
				t.Fatalf("round trip:\nwant %+v\ngot  %+v", value, got)
			}
		})
		// Decoding into the same row keeps the storage of []byte and slice fields.
		t.Run("ReusesStorage", func(t *testing.T) {
			columns := streamFixtureColumns(t)
			rows := MustStreamRowOf[streamFixture]()
			first := streamFixtureRow(t, columns, streamFixture{Payload: []byte("long payload"), Pieces: [][]byte{[]byte("abc")}})
			second := streamFixtureRow(t, columns, streamFixture{Payload: []byte("short"), Pieces: [][]byte{[]byte("d")}})
			dst := seededStreamFixture()
			streamDecodeRow(t, columns, rows, first, &dst)
			payload, piece := &dst.Payload[0], &dst.Pieces[0][0]
			streamDecodeRow(t, columns, rows, second, &dst)
			if string(dst.Payload) != "short" || string(dst.Pieces[0]) != "d" {
				t.Fatalf("second row: %+v", dst)
			}
			if &dst.Payload[0] != payload || &dst.Pieces[0][0] != piece {
				t.Fatal("streaming struct row did not reuse slice storage")
			}
		})
		t.Run("SubsetAndErrors", func(t *testing.T) {
			type pair struct {
				ID      int32
				Payload []byte
				Extra   int64
			}
			columns := cqltype.NewColumns(
				cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)},
				cqltype.ColumnSpec{Name: "payload", Type: rowTestType(t, cqltype.TypeBlob)},
			)
			data := chainBytes(t, encodeTestParams(t, columns, MustParamsOf[pair](), pair{ID: 3, Payload: []byte("x"), Extra: 9}))
			dst := pair{Payload: make([]byte, 0, 8), Extra: 1}
			streamDecodeRow(t, columns, MustStreamRowOf[pair](), data, &dst)
			if dst.ID != 3 || string(dst.Payload) != "x" {
				t.Fatalf("subset row: %+v", dst)
			}
			// Every column still needs a field.
			wider := cqltype.NewColumns(
				cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)},
				cqltype.ColumnSpec{Name: "author", Type: rowTestType(t, cqltype.TypeBlob)},
			)
			if _, err := MustStreamRowOf[pair]().Compile(wider); err == nil {
				t.Fatal("unmapped streaming column accepted")
			}
			type text struct{ Name string }
			if _, err := ReflectStreamRow[text](); err == nil || !strings.Contains(err.Error(), "WithStreamCodec") {
				t.Fatalf("string field: %v", err)
			}
			type badTag struct {
				Name []byte `cql:"name,stream=StreamInt32"`
			}
			if _, err := ReflectStreamRow[badTag](); err == nil || !strings.Contains(err.Error(), "StreamInt32") {
				t.Fatalf("wrong stream tag: %v", err)
			}
		})
		t.Run("WithStreamCodec", func(t *testing.T) {
			type hitCount int64 // a type with no built-in streaming decoder
			type counted struct {
				ID   int32
				Hits hitCount
			}
			columns := cqltype.NewColumns(
				cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)},
				cqltype.ColumnSpec{Name: "hits", Type: rowTestType(t, cqltype.TypeBigInt)},
			)
			type source struct {
				ID   int32
				Hits int64
			}
			data := chainBytes(t, encodeTestParams(t, columns, MustParamsOf[source](), source{ID: 1, Hits: 42}))
			hits := streamInteger[hitCount](cqltype.TypeBigInt, 8)
			var dst counted
			streamDecodeRow(t, columns, MustStreamRowOf(WithStreamCodec[counted](hits)), data, &dst)
			if dst != (counted{ID: 1, Hits: 42}) {
				t.Fatalf("custom stream codec: %+v", dst)
			}
		})
	})
}
