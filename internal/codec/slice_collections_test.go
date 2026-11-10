// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"reflect"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
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

func TestVectorStructMapping(t *testing.T) {
	type row struct {
		Values []int32 `cql:"values,codec=VectorCodec,stream=StreamVector"`
	}
	typ := CodecTestMust(cqltype.VectorDataType(cqltype.IntType, 3))
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "values", Type: typ})
	want := row{Values: []int32{7, 2, -3}}
	body := encodeTestParams(t, columns, MustParamsOf[row](), want)
	got := decodeTestRow(t, columns, MustRowOf[row](), body)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buffered vector = %v; want %v", got, want)
	}
	streamed := row{Values: make([]int32, 0, 3)}
	streamDecodeRow(t, columns, MustStreamRowOf[row](), chainBytes(t, body), &streamed)
	if !reflect.DeepEqual(streamed, want) {
		t.Fatalf("streamed vector = %v; want %v", streamed, want)
	}
}
