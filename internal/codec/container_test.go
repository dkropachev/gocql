/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestContainer(t *testing.T) {
	t.Run("Clone", func(t *testing.T) {
		t.Run("FragmentAliasingAndLease", func(t *testing.T) {
			blob := rowTestType(t, cqltype.TypeBlob)
			list, _ := cqltype.ListType(blob)
			payload := []byte("fragmented")
			encoded, err := EncodeValueBorrowed(list, ListEncoder(BytesEncoder), [][]byte{payload})
			if err != nil {
				t.Fatal(err)
			}
			alias := false
			cqltype.EachBytes(encoded.Bytes(), func(s wire.Span) error {
				if len(s.Bytes()) == len(payload) && &s.Bytes()[0] == &payload[0] {
					alias = true
				}
				return nil
			})
			if !alias {
				t.Fatal("container encoder copied borrowed payload")
			}
			body := encoded.Bytes().OwnedBytes()
			spans := make([]wire.Span, len(body))
			for i := range body {
				spans[i] = wire.Borrow(body[i : i+1])
			}
			chain, _ := wire.NewChain(spans...)
			lease := wire.NewLease(chain, nil)
			leased, _ := NewEncodedValue(list, cqltype.Present, cqltype.BytesView(chain, wire.RefOf(lease)))
			sequence, err := DecodeValue(ListDecoder(BlobCodec), leased)
			if err != nil {
				t.Fatal(err)
			}
			clone := sequence.Clone()
			it := sequence.Iterator()
			if !it.Next() {
				t.Fatal(it.Err())
			}
			fragments := 0
			cqltype.EachBytes(it.Value(), func(s wire.Span) error {
				if &s.Bytes()[0] != &body[8+fragments] {
					t.Error("fragment payload was copied")
				}
				fragments++
				return nil
			})
			if fragments != len(payload) {
				t.Fatalf("got %d fragments, want %d", fragments, len(payload))
			}
			lease.Release()
			if it := sequence.Iterator(); it.Next() || !errors.Is(it.Err(), cqltype.ErrReleased) {
				t.Fatalf("released sequence: %v", it.Err())
			}
			values, err := Collect(clone.Iterator())
			if err != nil || string(values[0].OwnedBytes()) != "fragmented" {
				t.Fatalf("detached clone failed: %v", err)
			}
			owned, err := EncodeValue(list, ListEncoder(BytesEncoder), [][]byte{payload})
			if err != nil {
				t.Fatal(err)
			}
			payload[0] = 'X'
			if got := owned.Bytes().OwnedBytes(); string(got[8:]) != "fragmented" {
				t.Fatal("owned collection retained source")
			}
		})
	})
	t.Run("Collect", func(t *testing.T) {
		text, blob := FoundationScalar(t, cqltype.TypeText), FoundationScalar(t, cqltype.TypeBlob)
		tuple := CodecTestMust(cqltype.TupleType(text, blob))
		list := CodecTestMust(cqltype.ListType(tuple))
		encoder := ListEncoder(TupleEncoder(PairParams(1, OneParam(StringEncoder), OneParam(BytesEncoder))))
		encoded := CodecTestMust(EncodeValueBorrowed(list, encoder, []Pair[string, []byte]{{"one", []byte("blob")}, {"two", []byte("more")}}))
		lease := wire.NewLease(cqltype.BytesChain(encoded.body), nil)
		borrowed := EncodedValue{typ: list, state: cqltype.Present, body: cqltype.BytesView(cqltype.BytesChain(encoded.body), wire.RefOf(lease))}
		decoder := ListDecoder(TupleDecoder(PairRows(1, OneColumn(TextCodec), OneColumn(BlobCodec))))
		seq := CodecTestMust(DecodeValue(decoder, borrowed))
		values := CodecTestMust(Collect(seq.Iterator()))
		dynamic := CodecTestMust(DynamicCodec.Decode(NewValueReader(list, cqltype.Present, cqltype.BytesChain(encoded.body), lease)))
		dynamicValues := CodecTestMust(Collect(CodecTestMust(dynamic.Elements())))
		lease.Release()
		if values[0].First.String() != "one" || !bytes.Equal(values[1].Second.OwnedBytes(), []byte("more")) {
			t.Fatal("typed Collect retained lease")
		}
		fields := CodecTestMust(Collect(CodecTestMust(dynamicValues[0].Fields())))
		txt := CodecTestMust(fields[0].Value.Text())
		if txt.String() != "one" {
			t.Fatal("dynamic Collect retained lease")
		}
		// Input-side dynamic values borrow caller arrays too; Collect detaches them.
		payload := []byte("input")
		value := CodecTestMust(ListValue(CodecTestMust(cqltype.ListType(blob)), []Value{BlobValue(payload)}))
		input := CodecTestMust(Collect(CodecTestMust(value.Elements())))
		payload[0] = 'X'
		b := CodecTestMust(input[0].Bytes())
		if string(b.OwnedBytes()) != "input" {
			t.Fatal("input-side Collect did not detach")
		}
	})
	t.Run("Compile", func(t *testing.T) {
		t.Run("ChecksOnceConcurrently", func(t *testing.T) {
			var checks atomic.Int64
			list, _ := cqltype.ListType(rowTestType(t, cqltype.TypeInt))
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "items", Type: list})
			codec := OneColumn(ListDecoder(containerCheckedInt{checks: &checks}))
			plan, err := codec.Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			chain := encodeTestParams(t, columns, OneParam(ListEncoder(Int32)), []int32{1, 2, 3})
			var group sync.WaitGroup
			for range 16 {
				group.Add(1)
				go func() {
					defer group.Done()
					for range 30 {
						sequence, err := plan.Decode(NewRowReader(columns, chain, nil))
						if err != nil {
							t.Error(err)
							return
						}
						values, err := Collect(sequence.Iterator())
						if err != nil || len(values) != 3 || values[2] != 3 {
							t.Errorf("decode values=%v err=%v", values, err)
							return
						}
					}
				}()
			}
			group.Wait()
			if checks.Load() != 1 {
				t.Fatalf("nested metadata checked %d times", checks.Load())
			}
		})
	})
	t.Run("Decode", func(t *testing.T) {
		t.Run("LazyFailureIsPermanent", func(t *testing.T) {
			list, _ := cqltype.ListType(rowTestType(t, cqltype.TypeInt))
			// Both frames are valid; the first integer body is too short.
			body, _ := hex.DecodeString("00000002000000030102030000000400000007")
			sequence, err := DecodeValue(ListDecoder(Int32), containerValue(t, list, body))
			if err != nil {
				t.Fatalf("lazy decode eagerly checked scalar: %v", err)
			}
			it := sequence.Iterator()
			if it.Next() || it.Err() == nil || it.Value() != 0 || it.Next() {
				t.Fatal("failed iterator continued or exposed a partial value")
			}
			if values, err := Collect(sequence.Iterator()); err == nil || values != nil {
				t.Fatal("Collect exposed partial malformed collection")
			}
			clone := sequence.Clone()
			if clone.Len() != 2 {
				t.Fatal("Clone eagerly decoded invalid scalar")
			}
		})
		t.Run("StrictEnclosingFraming", func(t *testing.T) {
			integer := rowTestType(t, cqltype.TypeInt)
			list, _ := cqltype.ListType(integer)
			set, _ := cqltype.SetType(integer)
			mapType, _ := cqltype.MapType(integer, integer)
			for _, test := range []struct {
				typ      cqltype.DataType
				validate func(EncodedValue) error
			}{
				{list, func(v EncodedValue) error { _, e := DecodeValue(ListDecoder(Int32), v); return e }},
				{set, func(v EncodedValue) error { _, e := DecodeValue(SetDecoder(Int32), v); return e }},
				{mapType, func(v EncodedValue) error { _, e := DecodeValue(MapDecoder(Int32, Int32), v); return e }},
			} {
				for _, bad := range []string{"", "000000", "ffffffff", "7fffffff", "000000010000000401", "00000001ffffffff", "00000001fffffffe", "00000001fffffffd", "0000000000"} {
					body, _ := hex.DecodeString(bad)
					if err := test.validate(containerValue(t, test.typ, body)); err == nil {
						t.Fatalf("%s accepted %s", test.typ, bad)
					}
				}
				null, _ := NewEncodedValue(test.typ, cqltype.Null, cqltype.Bytes{})
				if err := test.validate(null); err == nil {
					t.Fatalf("%s accepted null without nullable wrapper", test.typ)
				}
				if err := test.validate(containerValue(t, test.typ, []byte{0, 0, 0, 0})); err != nil {
					t.Fatalf("empty collection rejected: %v", err)
				}
			}
			encoded, err := EncodeValueBorrowed(list, ListEncoder(Int32), nil)
			if err != nil || !bytes.Equal(encoded.Bytes().OwnedBytes(), []byte{0, 0, 0, 0}) {
				t.Fatalf("nil slice must encode empty: %v", err)
			}
			if _, err := EncodeValueBorrowed(list, ListEncoder(NullableEncoder(Int32)), []cqltype.Nullable[int32]{{}}); err == nil {
				t.Fatal("null collection member accepted")
			}
			if _, err := EncodeValueBorrowed(list, ListEncoder(BindEncoder(Int32)), []cqltype.BindValue[int32]{{State: cqltype.Unset}}); err == nil {
				t.Fatal("unset collection member accepted")
			}
		})
	})
	t.Run("Golden", func(t *testing.T) {
		integer := rowTestType(t, cqltype.TypeInt)
		list, _ := cqltype.ListType(integer)
		set, _ := cqltype.SetType(integer)
		vector, _ := cqltype.VectorDataType(integer, 2)
		for _, test := range []struct {
			typ    cqltype.DataType
			enc    Encoder[[]int32]
			dec    Decoder[Sequence[int32]]
			golden string
		}{
			{list, ListEncoder(Int32), ListDecoder(Int32), "00000002000000040000000100000004ffffffff"},
			{set, SetEncoder(Int32), SetDecoder(Int32), "00000002000000040000000100000004ffffffff"},
			{vector, VectorEncoder(Int32), VectorDecoder(Int32), "00000001ffffffff"},
		} {
			encoded := containerGolden(t, test.typ, test.enc, []int32{1, -1}, test.golden)
			sequence, err := DecodeValue(test.dec, encoded)
			if err != nil {
				t.Fatal(err)
			}
			values, err := Collect(sequence.Iterator())
			if err != nil || !reflect.DeepEqual(values, []int32{1, -1}) {
				t.Fatalf("values=%v err=%v", values, err)
			}
		}
		text := rowTestType(t, cqltype.TypeText)
		blob := rowTestType(t, cqltype.TypeBlob)
		mapType, _ := cqltype.MapType(text, blob)
		encoded := containerGolden(t, mapType, MapEncoder(StringEncoder, BytesEncoder), []Entry[string, []byte]{{"x", []byte("y")}}, "0000000100000001780000000179")
		mapping, err := DecodeValue(MapDecoder(TextCodec, BlobCodec), encoded)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := Collect(mapping.Iterator())
		if err != nil || len(entries) != 1 || entries[0].Key.String() != "x" || string(entries[0].Value.OwnedBytes()) != "y" {
			t.Fatalf("entries=%v err=%v", entries, err)
		}
		tiny, _ := cqltype.VectorDataType(rowTestType(t, cqltype.TypeTinyInt), 2)
		containerGolden(t, tiny, VectorEncoder(Int8), []int8{1, -1}, "010101ff")
		texts, _ := cqltype.VectorDataType(text, 2)
		encoded = containerGolden(t, texts, VectorEncoder(StringEncoder), []string{"", "é"}, "0002c3a9")
		strings, err := DecodeValue(VectorDecoder(TextCodec), encoded)
		if err != nil {
			t.Fatal(err)
		}
		stringsValues, err := Collect(strings.Iterator())
		if err != nil || stringsValues[0].String() != "" || stringsValues[1].String() != "é" {
			t.Fatalf("variable vector: %v", err)
		}
		nested, _ := cqltype.VectorDataType(vector, 2)
		encoded = containerGolden(t, nested, VectorEncoder(VectorEncoder(Int32)), [][]int32{{1, 2}, {3, 4}}, "00000001000000020000000300000004")
		outer, err := DecodeValue(VectorDecoder(VectorDecoder(Int32)), encoded)
		if err != nil {
			t.Fatal(err)
		}
		outerIt := outer.Iterator()
		for n := int32(1); outerIt.Next(); n += 2 {
			inner, err := Collect(outerIt.Value().Iterator())
			if err != nil || !reflect.DeepEqual(inner, []int32{n, n + 1}) {
				t.Fatalf("nested vector=%v err=%v", inner, err)
			}
		}
		if outerIt.Err() != nil {
			t.Fatal(outerIt.Err())
		}
	})
	t.Run("Iterator", func(t *testing.T) {
		t.Run("AllocationsDoNotScale", func(t *testing.T) {
			typ, _ := cqltype.ListType(rowTestType(t, cqltype.TypeInt))
			allocationCount := func(count int) float64 {
				encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, count))
				if err != nil {
					t.Fatal(err)
				}
				sequence, err := DecodeValue(ListDecoder(Int32), encoded)
				if err != nil {
					t.Fatal(err)
				}
				return testing.AllocsPerRun(50, func() {
					it := sequence.Iterator()
					for it.Next() {
						containerIntSink = it.Value()
					}
					if it.Err() != nil {
						t.Fatal(it.Err())
					}
				})
			}
			small, large := allocationCount(1), allocationCount(1024)
			if large > small {
				t.Fatalf("iteration allocations scale with elements: one=%g, 1024=%g", small, large)
			}
		})
		t.Run("EmptyDetectsRelease", func(t *testing.T) {
			integer := rowTestType(t, cqltype.TypeInt)
			list, _ := cqltype.ListType(integer)
			mapType, _ := cqltype.MapType(integer, integer)
			chain, _ := wire.NewChain(wire.Borrow([]byte{0, 0, 0, 0}))
			lease := wire.NewLease(chain, nil)
			sequence, err := ListDecoder(Int32).Decode(NewValueReader(list, cqltype.Present, chain, lease))
			if err != nil {
				t.Fatal(err)
			}
			mapping, err := MapDecoder(Int32, Int32).Decode(NewValueReader(mapType, cqltype.Present, chain, lease))
			if err != nil {
				t.Fatal(err)
			}
			items, entries := sequence.Iterator(), mapping.Iterator()
			lease.Release()
			if items.Next() || !errors.Is(items.Err(), cqltype.ErrReleased) {
				t.Fatalf("empty sequence iterator: %v", items.Err())
			}
			if entries.Next() || !errors.Is(entries.Err(), cqltype.ErrReleased) {
				t.Fatalf("empty map iterator: %v", entries.Err())
			}
		})
		t.Run("SegmentedAllocationsDoNotScale", func(t *testing.T) {
			typ, _ := cqltype.ListType(rowTestType(t, cqltype.TypeInt))
			allocationCount := func(count int) float64 {
				encoded, err := EncodeValueBorrowed(typ, ListEncoder(Int32), make([]int32, count))
				if err != nil {
					t.Fatal(err)
				}
				data := encoded.Bytes().OwnedBytes()
				spans := make([]wire.Span, len(data))
				for index := range data {
					spans[index] = wire.Borrow(data[index : index+1])
				}
				chain, _ := wire.NewChain(spans...)
				sequence, err := ListDecoder(Int32).Decode(NewValueReader(typ, cqltype.Present, chain, nil))
				if err != nil {
					t.Fatal(err)
				}
				return testing.AllocsPerRun(30, func() {
					it := sequence.Iterator()
					for it.Next() {
						containerIntSink = it.Value()
					}
					if it.Err() != nil {
						t.Fatal(it.Err())
					}
				})
			}
			small, large := allocationCount(1), allocationCount(1024)
			if large > small {
				t.Fatalf("cross-span allocations scale with elements: one=%g, 1024=%g", small, large)
			}
		})
	})
	t.Run("Vector", func(t *testing.T) {
		t.Run("StrictDimensionsAndLengths", func(t *testing.T) {
			integer := rowTestType(t, cqltype.TypeInt)
			vector, _ := cqltype.VectorDataType(integer, 2)
			for _, bad := range [][]byte{nil, {0, 0, 0, 1}, make([]byte, 7), make([]byte, 9)} {
				if _, err := DecodeValue(VectorDecoder(NullableDecoder(Int32)), containerValue(t, vector, bad)); err == nil {
					t.Fatalf("short/surplus vector accepted: %x", bad)
				}
			}
			if _, err := EncodeValueBorrowed(vector, VectorEncoder(Int32), []int32{1}); err == nil {
				t.Fatal("short vector encoded")
			}
			if _, err := EncodeValueBorrowed(vector, VectorEncoder(NullableEncoder(Int32)), []cqltype.Nullable[int32]{{}, {}}); err == nil {
				t.Fatal("null vector member encoded")
			}
			text := rowTestType(t, cqltype.TypeText)
			texts, _ := cqltype.VectorDataType(text, 1)
			for _, bad := range []string{"", "02ff", "8000", "ff0000000000000000", "f080000000", "0000"} {
				body, _ := hex.DecodeString(bad)
				if _, err := DecodeValue(VectorDecoder(TextCodec), containerValue(t, texts, body)); err == nil {
					t.Fatalf("bad vint/length accepted: %s", bad)
				}
			}
			empty, err := DecodeValue(VectorDecoder(TextCodec), containerValue(t, texts, []byte{0}))
			if err != nil {
				t.Fatal(err)
			}
			values, err := Collect(empty.Iterator())
			if err != nil || len(values) != 1 || values[0].String() != "" {
				t.Fatalf("final zero length must stay present: %v", err)
			}
			huge, _ := cqltype.VectorDataType(integer, 65535)
			huge, _ = cqltype.VectorDataType(huge, 65535)
			if err := VectorDecoder(VectorDecoder(Int32)).CheckType(huge); err == nil {
				t.Fatal("overflowing vector width accepted")
			}
		})
		t.Run("VariablePayloadBorrowingAndOwnedDetach", func(t *testing.T) {
			vector, _ := cqltype.VectorDataType(rowTestType(t, cqltype.TypeBlob), 2)
			payload := bytes.Repeat([]byte{'x'}, 128)
			input := [][]byte{payload, {}}
			borrowed, err := EncodeValueBorrowed(vector, VectorEncoder(BytesEncoder), input)
			if err != nil {
				t.Fatal(err)
			}
			data := borrowed.Bytes().OwnedBytes()
			if len(data) != 131 || data[0] != 0x80 || data[1] != 0x80 || data[130] != 0 {
				t.Fatalf("unsigned vint body = %x", data)
			}
			alias := false
			cqltype.EachBytes(borrowed.Bytes(), func(span wire.Span) error {
				if span.Len() == len(payload) && &span.Bytes()[0] == &payload[0] {
					alias = true
				}
				return nil
			})
			if !alias {
				t.Fatal("variable vector encoder gathered its payload")
			}
			owned, err := EncodeValue(vector, VectorEncoder(BytesEncoder), input)
			if err != nil {
				t.Fatal(err)
			}
			payload[0] = 'Y'
			if got := owned.Bytes().OwnedBytes(); got[2] != 'x' {
				t.Fatal("owned vector retained borrowed payload")
			}
			// Every scalar and vint can straddle receive segments without gathering.
			spans := make([]wire.Span, len(data))
			for index := range data {
				spans[index] = wire.Borrow(data[index : index+1])
			}
			chain, _ := wire.NewChain(spans...)
			sequence, err := VectorDecoder(BlobCodec).Decode(NewValueReader(vector, cqltype.Present, chain, nil))
			if err != nil {
				t.Fatal(err)
			}
			values, err := Collect(sequence.Iterator())
			if err != nil || len(values) != 2 || values[0].Len() != 128 || values[1].Len() != 0 {
				t.Fatalf("segmented vector values=%v err=%v", values, err)
			}
		})
	})
}
