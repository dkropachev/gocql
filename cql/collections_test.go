// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestCollectionDecoders(t *testing.T) {
	list, err := cql.ListType(cql.IntType)
	if err != nil {
		t.Fatal(err)
	}
	set, err := cql.SetType(cql.IntType)
	if err != nil {
		t.Fatal(err)
	}
	vector, err := cql.VectorType(cql.IntType, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		typ     cql.DataType
		codec   cql.Codec[[]int32]
		decoder cql.Decoder[[]int32]
		stream  cql.StreamDecoder[[]int32]
	}{
		{"list", list, cql.ListCodec(cql.Int32Codec), cql.ListDecoder(cql.Int32Codec), cql.StreamList(cql.StreamInt32)},
		{"set", set, cql.SetCodec(cql.Int32Codec), cql.SetDecoder(cql.Int32Codec), cql.StreamSet(cql.StreamInt32)},
		{"vector", vector, cql.VectorCodec(cql.Int32Codec), cql.VectorDecoder(cql.Int32Codec), cql.StreamVector(cql.StreamInt32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := []int32{7, -3}
			encoded, err := cql.EncodeValue(tc.typ, tc.codec, want)
			if err != nil {
				t.Fatal(err)
			}
			for _, decoder := range []cql.Decoder[[]int32]{tc.codec, tc.decoder} {
				got, err := cql.DecodeValue(decoder, encoded)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("decode = %v, %v; want %v", got, err, want)
				}
				clone := decoder.Clone(got)
				clone[0] = 99
				if got[0] != 7 {
					t.Fatal("Clone shares slice storage")
				}
			}
			if _, err := tc.stream.Compile(tc.typ); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMapDecoders(t *testing.T) {
	typ, err := cql.MapType(cql.BlobType, cql.IntType)
	if err != nil {
		t.Fatal(err)
	}
	codec := cql.MapCodec(cql.ByteSliceCodec, cql.Int32Codec)
	want := []cql.Entry[[]byte, int32]{{Key: []byte("z"), Value: 7}, {Key: []byte("a"), Value: 3}}
	encoded, err := cql.EncodeValue(typ, codec, want)
	if err != nil {
		t.Fatal(err)
	}
	for _, decoder := range []cql.Decoder[[]cql.Entry[[]byte, int32]]{codec, cql.MapDecoder(cql.ByteSliceCodec, cql.Int32Codec)} {
		got, err := cql.DecodeValue(decoder, encoded)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("decode = %v, %v; want ordered non-comparable keys %v", got, err, want)
		}
		clone := decoder.Clone(got)
		clone[0].Key[0] = 'x'
		if string(got[0].Key) != "z" {
			t.Fatal("Clone shares map key storage")
		}
	}
	var stream cql.StreamDecoder[[]cql.Entry[[]byte, int32]] = cql.StreamMap(cql.StreamBlob, cql.StreamInt32)
	if _, err := stream.Compile(typ); err != nil {
		t.Fatal(err)
	}

	// Valid framing, invalid integer value: the error must occur during Decode.
	body, err := hex.DecodeString("00000002000000017a0000000400000007000000016100000003010203")
	if err != nil {
		t.Fatal(err)
	}
	malformed, err := cql.NewEncodedValue(typ, cql.Present, cql.BorrowBytes(body))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cql.DecodeValue(cql.MapDecoder(cql.ByteSliceCodec, cql.Int32Codec), malformed); err == nil || got != nil {
		t.Fatalf("malformed map exposed partial values: %v, %v", got, err)
	}
}
