// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestStringCodecDecodesOwnedString(t *testing.T) {
	typ, err := cqltype.ScalarType(cqltype.TypeVarchar)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeValueOwned(typ, StringCodec, "héllo")
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeValue(StringCodec, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != "héllo" {
		t.Fatalf("decoded %q", got)
	}
	null, err := NewEncodedValue(typ, cqltype.Null, cqltype.BorrowBytes(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeValue(StringCodec, null); err == nil {
		t.Fatal("null decoded into a string")
	}
	if v, err := DecodeValue(NullableCodec(StringCodec), null); err != nil || v.Valid {
		t.Fatalf("nullable null = %+v, %v", v, err)
	}
}

func TestStringCodecDecodesCollectionElements(t *testing.T) {
	text, err := cqltype.ScalarType(cqltype.TypeText)
	if err != nil {
		t.Fatal(err)
	}
	typ, err := cqltype.ListType(text)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeValueOwned(typ, ListEncoder(StringCodec), []string{"a", "bc"})
	if err != nil {
		t.Fatal(err)
	}
	values, err := DecodeValue(ListDecoder(StringCodec), encoded)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	it := values.Iterator()
	for it.Next() {
		got = append(got, it.Value())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "bc" {
		t.Fatalf("decoded %q", got)
	}
}

func TestStringCodecRejectsWrongTypes(t *testing.T) {
	blob, err := cqltype.ScalarType(cqltype.TypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	if err := StringCodec.CheckType(blob); err == nil {
		t.Fatal("StringCodec accepted blob")
	}
	text, err := cqltype.ScalarType(cqltype.TypeText)
	if err != nil {
		t.Fatal(err)
	}
	if err := ASCIIStringCodec.CheckType(text); err == nil {
		t.Fatal("ASCIIStringCodec accepted text")
	}
}
