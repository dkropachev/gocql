// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"context"
	"testing"
	"unsafe"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStreamNullCollectionsPreserveStorage(t *testing.T) {
	null := []byte{255, 255, 255, 255}
	empty := StreamCell([]byte{0, 0, 0, 0})
	for _, tc := range []struct {
		name    string
		typ     cqltype.DataType
		encoder Encoder[[][]byte]
		decoder StreamDecoder[[][]byte]
	}{
		{"list", CodecTestMust(cqltype.ListType(cqltype.BlobType)), ListEncoder(ByteSliceCodec), StreamList(StreamBlob)},
		{"set", CodecTestMust(cqltype.SetType(cqltype.BlobType)), SetEncoder(ByteSliceCodec), StreamSet(StreamBlob)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := streamEncodedFixture(t, tc.typ, tc.encoder, [][]byte{[]byte("one"), []byte("two")})
			first, second := make([]byte, 0, 8), make([]byte, 0, 8)
			dst := [][]byte{first, second}
			outer := unsafe.SliceData(dst)
			for _, input := range [][]byte{data, null, empty, data} {
				if err := StreamDecodeFixture(t, tc.typ, tc.decoder, input, &dst, 1); err != nil {
					t.Fatal(err)
				}
				if cap(dst) != 2 || unsafe.SliceData(dst) != outer {
					t.Fatal("NULL or empty discarded the outer storage")
				}
				if len(input) == len(null) || len(input) == len(empty) {
					if len(dst) != 0 {
						t.Fatal("NULL/empty returned elements")
					}
				} else if string(dst[0]) != "one" || string(dst[1]) != "two" ||
					unsafe.SliceData(dst[0]) != unsafe.SliceData(first) || unsafe.SliceData(dst[1]) != unsafe.SliceData(second) {
					t.Fatal("nested seeded storage was lost")
				}
			}
			wrapped := cqltype.Nullable[[][]byte]{Value: dst, Valid: true}
			if err := StreamDecodeFixture(t, tc.typ, StreamNullable(tc.decoder), null, &wrapped, 1); err != nil || wrapped.Valid || len(wrapped.Value) != 0 || cap(wrapped.Value) != 2 {
				t.Fatalf("nullable NULL: %+v, %v", wrapped, err)
			}
			if err := StreamDecodeFixture(t, tc.typ, StreamNullable(tc.decoder), empty, &wrapped, 1); err != nil || !wrapped.Valid || len(wrapped.Value) != 0 {
				t.Fatalf("nullable present empty: %+v, %v", wrapped, err)
			}
			for _, invalid := range [][]byte{
				{255, 255, 255, 254}, // UNSET is never a result.
				StreamCell([]byte{0, 0, 0, 1, 255, 255, 255, 255}), // NULL collection element.
				StreamCell(nil), // Missing element count.
			} {
				if err := StreamDecodeFixture(t, tc.typ, tc.decoder, invalid, &dst, 1); err == nil {
					t.Fatalf("invalid collection accepted: %x", invalid)
				}
			}
		})
	}
	t.Run("map", func(t *testing.T) {
		list := CodecTestMust(cqltype.ListType(cqltype.BlobType))
		typ := CodecTestMust(cqltype.MapType(cqltype.BlobType, list))
		decoder := StreamMap(StreamBlob, StreamList(StreamBlob))
		data := streamEncodedFixture(t, typ, MapEncoder(ByteSliceCodec, ListEncoder(ByteSliceCodec)), []Entry[[]byte, [][]byte]{{Key: []byte("key"), Value: [][]byte{[]byte("value")}}})
		key, value := make([]byte, 0, 8), make([]byte, 0, 8)
		children := [][]byte{value}
		dst := []Entry[[]byte, [][]byte]{{Key: key, Value: children}}
		outer := unsafe.SliceData(dst)
		for _, input := range [][]byte{data, null, empty, data} {
			if err := StreamDecodeFixture(t, typ, decoder, input, &dst, 1); err != nil {
				t.Fatal(err)
			}
			if cap(dst) != 1 || unsafe.SliceData(dst) != outer {
				t.Fatal("NULL/empty map discarded entries")
			}
			if len(input) == len(null) || len(input) == len(empty) {
				if len(dst) != 0 {
					t.Fatal("NULL/empty map returned entries")
				}
			} else if string(dst[0].Key) != "key" || string(dst[0].Value[0]) != "value" ||
				unsafe.SliceData(dst[0].Key) != unsafe.SliceData(key) || unsafe.SliceData(dst[0].Value) != unsafe.SliceData(children) || unsafe.SliceData(dst[0].Value[0]) != unsafe.SliceData(value) {
				t.Fatal("NULL/empty map lost nested storage")
			}
		}
		for _, invalid := range [][]byte{
			{255, 255, 255, 254},
			StreamCell([]byte{0, 0, 0, 1, 255, 255, 255, 255, 0, 0, 0, 0}),
			StreamCell(nil),
		} {
			if err := StreamDecodeFixture(t, typ, decoder, invalid, &dst, 1); err == nil {
				t.Fatalf("invalid map accepted: %x", invalid)
			}
		}
	})
	t.Run("vector remains nonnullable", func(t *testing.T) {
		typ := CodecTestMust(cqltype.VectorDataType(cqltype.IntType, 2))
		dst := make([]int32, 0, 2)
		if err := StreamDecodeFixture(t, typ, StreamVector(StreamInt32), null, &dst, 1); err == nil {
			t.Fatal("NULL vector accepted")
		}
		wrapped := cqltype.Nullable[[]int32]{Value: dst}
		if err := StreamDecodeFixture(t, typ, StreamNullable(StreamVector(StreamInt32)), null, &wrapped, 1); err != nil || wrapped.Valid || cap(wrapped.Value) != 2 {
			t.Fatalf("nullable vector lost capacity: %+v, %v", wrapped, err)
		}
	})
}

func TestStreamNullCollectionNoAllocation(t *testing.T) {
	list := CodecTestMust(cqltype.ListType(cqltype.BlobType))
	assertNullCollectionNoAllocation(t, list, StreamList(StreamBlob), [][]byte{make([]byte, 8)})
	set := CodecTestMust(cqltype.SetType(cqltype.BlobType))
	assertNullCollectionNoAllocation(t, set, StreamSet(StreamBlob), [][]byte{make([]byte, 8)})
	mapped := CodecTestMust(cqltype.MapType(cqltype.BlobType, list))
	assertNullCollectionNoAllocation(t, mapped, StreamMap(StreamBlob, StreamList(StreamBlob)), []Entry[[]byte, [][]byte]{{Key: make([]byte, 8), Value: [][]byte{make([]byte, 8)}}})
}

func assertNullCollectionNoAllocation[T any](t *testing.T, typ cqltype.DataType, decoder StreamDecoder[[]T], dst []T) {
	t.Helper()
	plan, err := decoder.Compile(typ)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := wire.NewStream(context.Background(), bytes.NewReader(nil), 0, wire.StreamLimits{MaxDepth: 8})
	if err != nil {
		t.Fatal(err)
	}
	reader := StreamValueReader{scope: scope, typ: typ, state: cqltype.Null, shared: &streamDecodeState{}, active: true}
	if allocs := testing.AllocsPerRun(100, func() {
		dst = dst[:1]
		if err := plan.DecodeInto(&reader, &dst); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("NULL collection allocated: %g", allocs)
	}
	// Even NULL must validate the metadata, rather than bypassing a compiled plan.
	reader.typ = cqltype.IntType
	if err := plan.DecodeInto(&reader, &dst); err == nil {
		t.Fatal("NULL bypassed metadata validation")
	}
}
