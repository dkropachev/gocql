// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type streamFixture struct {
	ID      int32
	Key     cqltype.UUID `cql:"key"`
	Payload []byte
	Name    []byte `cql:"name,stream=StreamText"`
	Pieces  [][]byte
	Tags    []int32 `cql:"tags,stream=StreamSet"`
	Maybe   cqltype.Nullable[int64]
	At      cqltype.Timestamp
	Unused  float64 // in no statement below
	Skipped string  `cql:"-"`
}

func streamFixtureColumns(t testing.TB) cqltype.Columns {
	t.Helper()
	blobs := CodecTestMust(cqltype.ListType(rowTestType(t, cqltype.TypeBlob)))
	ints := CodecTestMust(cqltype.SetType(rowTestType(t, cqltype.TypeInt)))
	return cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "payload", Type: rowTestType(t, cqltype.TypeBlob)},
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeInt)},
		cqltype.ColumnSpec{Name: "key", Type: rowTestType(t, cqltype.TypeUUID)},
		cqltype.ColumnSpec{Name: "name", Type: rowTestType(t, cqltype.TypeText)},
		cqltype.ColumnSpec{Name: "pieces", Type: blobs},
		cqltype.ColumnSpec{Name: "tags", Type: ints},
		cqltype.ColumnSpec{Name: "maybe", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "at", Type: rowTestType(t, cqltype.TypeTimestamp)},
	)
}

func streamFixtureRow(t testing.TB, columns cqltype.Columns, v streamFixture) []byte {
	t.Helper()
	params := Params(
		Param("payload", BytesEncoder, func(v streamFixture) []byte { return v.Payload }),
		Param("id", Int32, func(v streamFixture) int32 { return v.ID }),
		Param("key", UUIDCodec, func(v streamFixture) cqltype.UUID { return v.Key }),
		Param("name", StringCodec, func(v streamFixture) string { return string(v.Name) }),
		Param("pieces", ListEncoder(BytesEncoder), func(v streamFixture) [][]byte { return v.Pieces }),
		Param("tags", SetEncoder(Int32), func(v streamFixture) []int32 { return v.Tags }),
		Param("maybe", NullableEncoder(Int64), func(v streamFixture) cqltype.Nullable[int64] { return v.Maybe }),
		Param("at", TimestampCodec, func(v streamFixture) cqltype.Timestamp { return v.At }),
	)
	return chainBytes(t, encodeTestParams(t, columns, params, v))
}

// seededStreamFixture has the storage streaming decoders write into: they
// reuse destination capacity and never allocate.
func seededStreamFixture() streamFixture {
	return streamFixture{
		Payload: make([]byte, 0, 32), Name: make([]byte, 0, 32),
		Pieces: [][]byte{make([]byte, 0, 8), make([]byte, 0, 8)}[:0], Tags: make([]int32, 0, 4),
	}
}

func streamDecodeRow[R any](t testing.TB, columns cqltype.Columns, rows StreamRowDecoder[R], data []byte, dst *R) {
	t.Helper()
	plan, err := rows.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	r, scope := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
	plan.Reset(dst)
	if err := plan.DecodeInto(r, dst); err != nil {
		t.Fatal(err)
	}
	if err := r.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	if err := scope.RequireEnd(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamStructMappingMatchesHandWritten(t *testing.T) {
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
}

// Decoding into the same row keeps the storage of []byte and slice fields.
func TestStreamStructMappingReusesStorage(t *testing.T) {
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
}

func TestStreamStructMappingSubsetAndErrors(t *testing.T) {
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
}

func TestStreamStructMappingWithStreamCodec(t *testing.T) {
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
}
