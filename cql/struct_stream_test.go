// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"bytes"
	"testing"
)

type streamFixture struct {
	ID      int32
	Key     UUID `cql:"key"`
	Payload []byte
	Name    []byte `cql:"name,stream=Text"`
	Pieces  [][]byte
	Tags    []int32 `cql:"tags,stream=Set"`
	Maybe   Nullable[int64]
	At      Timestamp
	Unused  float64 // in no statement below
	Skipped string  `cql:"-"`
}

func streamFixtureColumns(t testing.TB) Columns {
	t.Helper()
	blobs := CodecTestMust(ListType(rowTestType(t, TypeBlob)))
	ints := CodecTestMust(SetType(rowTestType(t, TypeInt)))
	return NewColumns(
		ColumnSpec{Name: "payload", Type: rowTestType(t, TypeBlob)},
		ColumnSpec{Name: "id", Type: rowTestType(t, TypeInt)},
		ColumnSpec{Name: "key", Type: rowTestType(t, TypeUUID)},
		ColumnSpec{Name: "name", Type: rowTestType(t, TypeText)},
		ColumnSpec{Name: "pieces", Type: blobs},
		ColumnSpec{Name: "tags", Type: ints},
		ColumnSpec{Name: "maybe", Type: rowTestType(t, TypeBigInt)},
		ColumnSpec{Name: "at", Type: rowTestType(t, TypeTimestamp)},
	)
}

func streamFixtureRow(t testing.TB, columns Columns, v streamFixture) []byte {
	t.Helper()
	params := Params(
		Param("payload", ByteSliceCodec, func(v streamFixture) []byte { return v.Payload }),
		Param("id", Int32Codec, func(v streamFixture) int32 { return v.ID }),
		Param("key", UUIDCodec, func(v streamFixture) UUID { return v.Key }),
		Param("name", StringCodec, func(v streamFixture) string { return string(v.Name) }),
		Param("pieces", ListEncoder(ByteSliceCodec), func(v streamFixture) [][]byte { return v.Pieces }),
		Param("tags", SetEncoder(Int32Codec), func(v streamFixture) []int32 { return v.Tags }),
		Param("maybe", NullableEncoder(Int64Codec), func(v streamFixture) Nullable[int64] { return v.Maybe }),
		Param("at", TimestampCodec, func(v streamFixture) Timestamp { return v.At }),
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

func streamDecodeRow[R any](t testing.TB, columns Columns, rows StreamRowDecoder[R], data []byte, dst *R) {
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
