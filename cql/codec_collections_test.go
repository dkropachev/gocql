// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"bytes"
	"encoding/hex"
	"sync/atomic"
	"testing"
)

func containerValue(t testing.TB, typ DataType, body []byte) EncodedValue {
	t.Helper()
	v, err := NewEncodedValue(typ, Present, BorrowBytes(body))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func containerGolden[T any](t *testing.T, typ DataType, encoder Encoder[T], value T, golden string) EncodedValue {
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

func (c containerCheckedInt) CheckType(typ DataType) error {
	c.checks.Add(1)
	return Int32Codec.CheckType(typ)
}
func (c containerCheckedInt) Decode(r ValueReader) (int32, error) { return Int32Codec.Decode(r) }
func (c containerCheckedInt) Clone(v int32) int32                 { return v }
