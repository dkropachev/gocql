// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
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
