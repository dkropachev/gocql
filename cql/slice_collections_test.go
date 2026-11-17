// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"reflect"
	"testing"
)

type collectionRow struct {
	ID     int64
	Names  []string
	Scores []int32 `cql:"scores,codec=SetCodec"`
	Blobs  [][]byte
	Attrs  []Entry[string, int64]
}

func collectionColumns(t testing.TB) Columns {
	t.Helper()
	text, blob := rowTestType(t, TypeText), rowTestType(t, TypeBlob)
	return NewColumns(
		ColumnSpec{Name: "id", Type: rowTestType(t, TypeBigInt)},
		ColumnSpec{Name: "names", Type: CodecTestMust(ListType(text))},
		ColumnSpec{Name: "scores", Type: CodecTestMust(SetType(rowTestType(t, TypeInt)))},
		ColumnSpec{Name: "blobs", Type: CodecTestMust(ListType(blob))},
		ColumnSpec{Name: "attrs", Type: CodecTestMust(MapType(text, rowTestType(t, TypeBigInt)))},
	)
}

var attrsCodec = WithCodec[collectionRow](MapCodec(StringCodec, Int64Codec))

func TestVectorStructMapping(t *testing.T) {
	type row struct {
		Values []int32 `cql:"values,codec=VectorCodec,stream=Vector"`
	}
	typ := CodecTestMust(VectorType(IntType, 3))
	columns := NewColumns(ColumnSpec{Name: "values", Type: typ})
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
