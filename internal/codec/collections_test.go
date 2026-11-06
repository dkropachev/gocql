// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"encoding/hex"
	"sync/atomic"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func containerValue(t testing.TB, typ cqltype.DataType, body []byte) EncodedValue {
	t.Helper()
	v, err := NewEncodedValue(typ, cqltype.Present, cqltype.BorrowBytes(body))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func containerGolden[T any](t *testing.T, typ cqltype.DataType, encoder Encoder[T], value T, golden string) EncodedValue {
	t.Helper()
	encoded, err := EncodeValueBorrowed(typ, encoder, value)
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded.Bytes().OwnedBytes(), want) {
		t.Fatalf("wire body = %x, want %x", encoded.Bytes().OwnedBytes(), want)
	}
	return encoded
}

type containerCheckedInt struct{ checks *atomic.Int64 }

func (c containerCheckedInt) CheckType(typ cqltype.DataType) error {
	c.checks.Add(1)
	return Int32.CheckType(typ)
}
func (c containerCheckedInt) Decode(r ValueReader) (int32, error) { return Int32.Decode(r) }
func (c containerCheckedInt) Clone(v int32) int32                 { return v }
