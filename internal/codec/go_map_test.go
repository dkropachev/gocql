// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"reflect"
	"strings"
	"testing"
	"unsafe"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestGoMapCodec(t *testing.T) {
	typ := CodecTestMust(cqltype.MapType(cqltype.TextType, cqltype.BigIntType))
	codec := GoMapCodec(StringCodec, Int64)
	reader := func(v EncodedValue) ValueReader {
		return newValueReader(v.Type(), v.State(), cqltype.BytesChain(v.Bytes()), cqltype.BytesLease(v.Bytes()))
	}
	t.Run("RoundTrip", func(t *testing.T) {
		want := map[string]int64{"a": 1, "b": -2, "c": 3}
		encoded := CodecTestMust(EncodeValue(typ, codec, want))
		got := CodecTestMust(DecodeValue(codec, encoded))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		// The bytes are a CQL map, whichever entry order Go chose.
		entries := CodecTestMust(DecodeValue(MapCodec(StringCodec, Int64), encoded))
		if len(entries) != len(want) {
			t.Fatalf("entries %v", entries)
		}
		for _, e := range entries {
			if want[e.Key] != e.Value {
				t.Fatalf("entry %v", e)
			}
		}
	})
	t.Run("DuplicateKey", func(t *testing.T) {
		encoded := CodecTestMust(EncodeValue(typ, MapCodec(StringCodec, Int64), []Entry[string, int64]{{"a", 1}, {"a", 2}}))
		if _, err := DecodeValue(codec, encoded); err == nil || !strings.Contains(err.Error(), "repeats an earlier key") {
			t.Fatalf("duplicate key: %v", err)
		}
	})
	t.Run("DecodeIntoReusesMap", func(t *testing.T) {
		dst := map[string]int64{"stale": 9}
		before := reflect.ValueOf(dst).UnsafePointer()
		encoded := CodecTestMust(EncodeValue(typ, codec, map[string]int64{"fresh": 1}))
		if err := codec.(interface {
			DecodeInto(ValueReader, *map[string]int64) error
		}).DecodeInto(reader(encoded), &dst); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(dst, map[string]int64{"fresh": 1}) || reflect.ValueOf(dst).UnsafePointer() != before {
			t.Fatalf("dst %v was not cleared and reused", dst)
		}
		null := CodecTestMust(NewEncodedValue(typ, cqltype.Null, cqltype.Bytes{}))
		if err := codec.(interface {
			DecodeInto(ValueReader, *map[string]int64) error
		}).DecodeInto(reader(null), &dst); err != nil || len(dst) != 0 || reflect.ValueOf(dst).UnsafePointer() != before {
			t.Fatalf("null: %v, %v", dst, err)
		}
		if m := CodecTestMust(DecodeValue(codec, null)); len(m) != 0 {
			t.Fatalf("null decodes as %v", m)
		}
	})
	t.Run("TypeMismatch", func(t *testing.T) {
		if err := codec.CheckType(CodecTestMust(cqltype.MapType(cqltype.IntType, cqltype.BigIntType))); err == nil {
			t.Fatal("int key accepted for a string key codec")
		}
		if err := codec.CheckType(CodecTestMust(cqltype.ListType(cqltype.TextType))); err == nil {
			t.Fatal("list accepted")
		}
	})
}

func TestGoMapStructMapping(t *testing.T) {
	// The erased fields read a map field as one pointer word.
	if unsafe.Sizeof(map[string]int64(nil)) != unsafe.Sizeof(unsafe.Pointer(nil)) {
		t.Fatal("a map value is not one pointer word")
	}
	type row struct {
		ID     int64
		Attrs  map[string]int64
		Blobs  map[cqltype.UUID][]byte
		Skip   map[string]cqltype.Text `cql:"-"`
		Hidden string                  `cql:"-"`
	}
	text, bigint := cqltype.TextType, cqltype.BigIntType
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "id", Type: bigint},
		cqltype.ColumnSpec{Name: "attrs", Type: CodecTestMust(cqltype.MapType(text, bigint))},
		cqltype.ColumnSpec{Name: "blobs", Type: CodecTestMust(cqltype.MapType(cqltype.UUIDType, cqltype.BlobType))},
	)
	want := row{ID: 7, Attrs: map[string]int64{"x": 1, "y": 2}, Blobs: map[cqltype.UUID][]byte{{1}: []byte("one")}}
	body := encodeTestParams(t, columns, MustParamsOf[row](), want)
	got := decodeTestRow(t, columns, MustRowOf[row](), body)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	rows := MustRowOf[row]()
	if clone := rows.Clone(got); !reflect.DeepEqual(clone, got) || reflect.ValueOf(clone.Attrs).UnsafePointer() == reflect.ValueOf(got.Attrs).UnsafePointer() {
		t.Fatalf("clone %+v shares or differs", clone)
	}
	plan := CodecTestMust(rows.Compile(columns))
	detached := got
	DetachRow(plan, &detached)
	if !reflect.DeepEqual(detached, want) {
		t.Fatalf("detached %+v", detached)
	}
	// A map with no built-in pair, such as one of borrowed views, still needs
	// a codec.
	type viewRow struct{ Views map[string]cqltype.Text }
	if _, err := ReflectRow[viewRow](); err == nil {
		t.Fatal("reflection mapped a map of views")
	}
	found := false
	for _, b := range BuiltinFieldTypes() {
		if b.Type == reflect.TypeFor[map[string]int64]() {
			found = b.Default && b.EncoderSrc == "cql.GoMapCodec(cql.StringCodec, cql.Int64Codec)" && b.TypeSrc == "map[string]int64"
		}
	}
	if !found {
		t.Fatal("map[string]int64 is not a default built-in field type")
	}
}
