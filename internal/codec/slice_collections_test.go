// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"reflect"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type collectionRow struct {
	ID     int64
	Names  []string
	Scores []int32 `cql:"scores,codec=SetCodec"`
	Blobs  [][]byte
	Attrs  []Entry[string, int64]
}

func collectionColumns(t testing.TB) cqltype.Columns {
	t.Helper()
	text, blob := rowTestType(t, cqltype.TypeText), rowTestType(t, cqltype.TypeBlob)
	return cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: rowTestType(t, cqltype.TypeBigInt)},
		cqltype.ColumnSpec{Name: "names", Type: CodecTestMust(cqltype.ListType(text))},
		cqltype.ColumnSpec{Name: "scores", Type: CodecTestMust(cqltype.SetType(rowTestType(t, cqltype.TypeInt)))},
		cqltype.ColumnSpec{Name: "blobs", Type: CodecTestMust(cqltype.ListType(blob))},
		cqltype.ColumnSpec{Name: "attrs", Type: CodecTestMust(cqltype.MapType(text, rowTestType(t, cqltype.TypeBigInt)))},
	)
}

var attrsCodec = WithCodec[collectionRow](MapCodec(StringCodec, Int64))

func TestStructMappingCollections(t *testing.T) {
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
}

// CQL stores an empty collection as null; both decode as an empty slice.
func TestSliceCodecsDecodeNullAsEmpty(t *testing.T) {
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
}

// With ReuseBuffers, decoding into the same row keeps the storage of slice
// fields and of their []byte elements.
func TestStructMappingCollectionsReuseBuffers(t *testing.T) {
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
}
