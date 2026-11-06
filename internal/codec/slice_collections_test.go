// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
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
