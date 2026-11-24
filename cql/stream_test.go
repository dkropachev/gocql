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

package cql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStream(t *testing.T) {
	t.Run("Collection", func(t *testing.T) {
		t.Run("EncodedNestedFinalReadIdentity", func(t *testing.T) {
			blob := FoundationScalar(t, TypeBlob)
			list := CodecTestMust(ListType(blob))
			payload := bytes.Repeat([]byte{0x72}, 300<<10)
			data := streamEncodedFixture(t, list, ListEncoder(ByteSliceCodec), [][]byte{payload})
			dst := StreamCell{Bytes: make([]byte, 0, len(data)-4)}
			start := uintptr(unsafe.Pointer(unsafe.SliceData(dst.Bytes)))
			end := start + uintptr(cap(dst.Bytes))
			bulk := 0
			source := &streamChunkSource{data: data, step: 4093, record: func(p []byte, offset int) {
				if offset < 12 {
					return
				}
				if len(p) < 64 {
					return
				}
				address := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
				if address < start || address+uintptr(len(p)) > end {
					t.Fatal("encoded payload staged in driver scratch")
				}
				bulk++
			}}
			columns := NewColumns(ColumnSpec{Name: "encoded", Type: list})
			r, _ := streamReaderFixture(t, columns, source, len(data))
			plan := CodecTestMust(StreamOneColumn(StreamEncoded).Compile(columns))
			if err := plan.DecodeInto(r, &dst); err != nil || !bytes.Equal(dst.Bytes, data[4:]) || bulk == 0 {
				t.Fatal("encoded final storage", err)
			}
			if r.shared.discard != nil {
				t.Fatal("encoded output allocated discard storage")
			}
		})
		t.Run("NestedCapacityAndBacking", func(t *testing.T) {
			blob := FoundationScalar(t, TypeBlob)
			list := CodecTestMust(ListType(blob))
			data := streamEncodedFixture(t, list, ListEncoder(ByteSliceCodec), [][]byte{[]byte("one"), []byte("two")})
			first, second := make([]byte, 8), make([]byte, 8)
			out := [][]byte{first[:0], second[:0]}
			if err := StreamDecodeFixture(t, list, StreamList(StreamBlob), data, &out, 1); err != nil || len(out) != 2 || string(out[0]) != "one" || &out[0][0] != &first[0] || &out[1][0] != &second[0] {
				t.Fatal("nested storage", out, err)
			}
			// Decode reslices seeded slots beyond len, preserving their child backing.
			out = out[:0]
			if err := StreamDecodeFixture(t, list, StreamList(StreamBlob), data, &out, 3); err != nil || &out[0][0] != &first[0] {
				t.Fatal("reused nested storage", err)
			}
			source := &streamChunkSource{data: data}
			cols := NewColumns(ColumnSpec{Name: "v", Type: list})
			r, _ := streamReaderFixture(t, cols, source, len(data))
			plan := CodecTestMust(StreamOneColumn(StreamList(StreamBlob)).Compile(cols))
			small := make([][]byte, 0, 1)
			if err := plan.DecodeInto(r, &small); err == nil || source.offset != 8 {
				t.Fatalf("outer count capacity %v offset %d", err, source.offset)
			}
			missingChild := [][]byte{make([]byte, 0, 2), make([]byte, 0, 8)}
			if err := StreamDecodeFixture(t, list, StreamList(StreamBlob), data, &missingChild, 1); err == nil {
				t.Fatal("child capacity grew")
			}
			m := CodecTestMust(MapType(blob, list))
			input := []Entry[[]byte, [][]byte]{{Key: []byte("k"), Value: [][]byte{[]byte("one"), []byte("two")}}}
			mdata := streamEncodedFixture(t, m, MapEncoder(ByteSliceCodec, ListEncoder(ByteSliceCodec)), input)
			mapped := []Entry[[]byte, [][]byte]{{Key: make([]byte, 0, 4), Value: [][]byte{first[:0], second[:0]}}}
			if err := StreamDecodeFixture(t, m, StreamMap(StreamBlob, StreamList(StreamBlob)), mdata, &mapped, 1); err != nil || string(mapped[0].Key) != "k" || string(mapped[0].Value[1]) != "two" {
				t.Fatal("map", mapped, err)
			}
			set := CodecTestMust(SetType(blob))
			setData := streamEncodedFixture(t, set, SetEncoder(ByteSliceCodec), [][]byte{[]byte("x")})
			out = out[:0]
			if err := StreamDecodeFixture(t, set, StreamSet(StreamBlob), setData, &out, 1); err != nil || string(out[0]) != "x" {
				t.Fatal("set", err)
			}
			if _, err := StreamList(StreamInt32).Compile(list); err == nil {
				t.Fatal("nested mismatch accepted")
			}
		})
		t.Run("Vectors", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			text := FoundationScalar(t, TypeText)
			fixed := CodecTestMust(VectorType(integer, 3))
			data := streamEncodedFixture(t, fixed, VectorEncoder(Int32Codec), []int32{1, -2, 3})
			out := make([]int32, 0, 3)
			if err := StreamDecodeFixture(t, fixed, StreamVector(StreamInt32), data, &out, 1); err != nil || !reflect.DeepEqual(out, []int32{1, -2, 3}) {
				t.Fatal("fixed vector", out, err)
			}
			if err := StreamDecodeFixture(t, fixed, StreamVector(StreamInt32), streamCellBytes(data[4:len(data)-1]), &out, 1); err == nil {
				t.Fatal("short vector accepted")
			}
			variable := CodecTestMust(VectorType(text, 2))
			vdata := streamEncodedFixture(t, variable, VectorEncoder(StringCodec), []string{"€", ""})
			vout := [][]byte{make([]byte, 0, 4), make([]byte, 0, 1)}
			if err := StreamDecodeFixture(t, variable, StreamVector(StreamText), vdata, &vout, 1); err != nil || string(vout[0]) != "€" || len(vout[1]) != 0 {
				t.Fatal("variable vector", vout, err)
			}
			nested := CodecTestMust(VectorType(fixed, 2))
			nData := streamEncodedFixture(t, nested, VectorEncoder(VectorEncoder(Int32Codec)), [][]int32{{1, 2, 3}, {4, 5, 6}})
			nestedOut := [][]int32{make([]int32, 0, 3), make([]int32, 0, 3)}
			if err := StreamDecodeFixture(t, nested, StreamVector(StreamVector(StreamInt32)), nData, &nestedOut, 1); err != nil || nestedOut[1][2] != 6 {
				t.Fatal("nested vector", nestedOut, err)
			}
		})
	})
	t.Run("Compile", func(t *testing.T) {
		t.Run("ConcurrencyAndNilDestinations", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			cols := NewColumns(ColumnSpec{Name: "v", Type: integer})
			plan := CodecTestMust(StreamOneColumn(StreamInt32).Compile(cols))
			data := streamEncodedFixture(t, integer, Int32Codec, int32(42))
			var group sync.WaitGroup
			for i := 0; i < 8; i++ {
				group.Add(1)
				go func() {
					defer group.Done()
					for n := 0; n < 20; n++ {
						r, _ := streamReaderFixture(t, cols, bytes.NewReader(data), len(data))
						var dst int32
						plan.Reset(&dst)
						if err := plan.DecodeInto(r, &dst); err != nil || dst != 42 {
							t.Error("shared immutable plan", err)
						}
					}
				}()
			}
			group.Wait()
			r, _ := streamReaderFixture(t, cols, bytes.NewReader(data), len(data))
			err := plan.DecodeInto(r, nil)
			if _, ok := errors.AsType[*ValidationError](err); !ok {
				t.Fatal("nil destination", err)
			}
		})
		t.Run("CustomReusesCompiledChildren", func(t *testing.T) {
			integer, blob := FoundationScalar(t, TypeInt), FoundationScalar(t, TypeBlob)
			tuple := CodecTestMust(TupleType(integer, blob))
			cols := NewColumns(ColumnSpec{Name: "v", Type: tuple})
			checks := 0
			plan := CodecTestMust(StreamOneColumn(customStreamDecoder{&checks}).Compile(cols))
			data := streamEncodedFixture(t, tuple, TupleEncoder(PairParams(OneParam(Int32Codec), OneParam(ByteSliceCodec))), Pair[int32, []byte]{7, []byte("payload")})
			combined := bytes.Repeat(data, 100)
			reader, scope := streamReaderFixture(t, cols, bytes.NewReader(combined), len(combined))
			dst := customStreamPair{B: make([]byte, 0, 32)}
			for i := 0; i < 100; i++ {
				reader.index = 0
				plan.Reset(&dst)
				if err := plan.DecodeInto(reader, &dst); err != nil || dst.A != 7 || string(dst.B) != "payload" {
					t.Fatal("custom stream", err)
				}
			}
			if checks != 1 || scope.Remaining() != 0 {
				t.Fatal("custom plan recompiled", checks)
			}
		})
		t.Run("NilPlansAreValidationErrors", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			cols := NewColumns(ColumnSpec{Name: "v", Type: integer})
			tuple := CodecTestMust(TupleType(integer))
			list := CodecTestMust(ListType(integer))
			m := CodecTestMust(MapType(integer, integer))
			cases := []func() error{
				func() error { _, e := StreamOneColumn(nilStreamValue{}).Compile(cols); return e },
				func() error {
					_, e := StreamRowInto(func(*int32) {}, StreamColumn("v", nilStreamValue{}, func(p *int32) *int32 { return p })).Compile(cols)
					return e
				},
				func() error { _, e := StreamList(nilStreamValue{}).Compile(list); return e },
				func() error { _, e := StreamMap(nilStreamValue{}, StreamInt32).Compile(m); return e },
				func() error { _, e := StreamNullable(nilStreamValue{}).Compile(integer); return e },
				func() error { _, e := StreamEmpty(nilStreamValue{}).Compile(integer); return e },
				func() error { _, e := StreamTuple(nilStreamRows{}).Compile(tuple); return e },
				func() error { return TupleDecoder(nilBufferedRows{}).CheckType(tuple) },
				func() error { return TupleEncoder(nilBufferedParams{}).CheckType(tuple) },
			}
			for i, test := range cases {
				err := test()
				if _, ok := errors.AsType[*ValidationError](err); !ok {
					t.Fatalf("case %d: %v", i, err)
				}
			}
		})
		t.Run("SharedTypeDAG", func(t *testing.T) {
			left, right := FoundationScalar(t, TypeInt), FoundationScalar(t, TypeInt)
			for i := 0; i < 64; i++ {
				left = CodecTestMust(TupleType(left, left))
				right = CodecTestMust(TupleType(right, right))
			}
			if !left.Equal(right) {
				t.Fatal("equivalent shared descriptors differ")
			}
			if err := DynamicCodec.CheckType(left); err != nil {
				t.Fatal(err)
			}
			plan := CodecTestMust(compileStreamValidation(left, 0))
			nodes := 0
			for len(plan.fields) > 0 {
				if len(plan.fields) != 2 || plan.fields[0] != plan.fields[1] {
					t.Fatal("shared child metadata expanded into duplicate plans")
				}
				nodes++
				plan = plan.fields[0]
			}
			if nodes != 64 {
				t.Fatal("unexpected graph height", nodes)
			}
			for i := 64; i < 130; i++ {
				left = CodecTestMust(TupleType(left, left))
			}
			if _, err := compileStreamValidation(left, 0); err == nil {
				t.Fatal("excessive descriptor depth accepted")
			}
			if err := DynamicCodec.CheckType(left); err == nil {
				t.Fatal("excessive dynamic descriptor depth accepted")
			}
		})
	})
	t.Run("PageValidation", func(t *testing.T) {
		t.Run("MetadataReplacement", func(t *testing.T) {
			boolean := FoundationScalar(t, TypeBoolean)
			columns := NewColumns(ColumnSpec{Name: "v", Type: boolean})
			prior, _ := streamReaderFixture(t, columns, bytes.NewReader(nil), 0)
			for _, candidate := range []struct {
				name    string
				columns Columns
			}{
				{"equal-new-backing", NewColumns(ColumnSpec{Name: "v", Type: boolean})},
				{"equal-new-descriptor", NewColumns(ColumnSpec{Name: "v", Type: FoundationScalar(t, TypeBoolean)})},
				{"changed-type", NewColumns(ColumnSpec{Name: "v", Type: FoundationScalar(t, TypeInt)})},
				{"changed-name", NewColumns(ColumnSpec{Name: "new", Type: boolean})},
			} {
				t.Run(candidate.name, func(t *testing.T) {
					scope := CodecTestMust(wire.NewStream(context.Background(), bytes.NewReader(nil), 0, wire.StreamLimits{}))
					next := CodecTestMust(newStreamRowReaderReusingValidation(candidate.columns, scope, prior))
					if next.validations[0] == prior.validations[0] {
						t.Fatal("replacement metadata kept old validation descriptors")
					}
					if descriptorOf(next.validations[0].typ) != descriptorOf(columnSlice(candidate.columns)[0].Type) {
						t.Fatal("validation did not use exact new metadata")
					}
				})
			}
		})
		t.Run("NestedMalformedAndIndependentReaders", func(t *testing.T) {
			boolean := FoundationScalar(t, TypeBoolean)
			list := CodecTestMust(ListType(boolean))
			columns := NewColumns(ColumnSpec{Name: "v", Type: list})
			valid := streamEncodedFixture(t, list, ListEncoder(BoolCodec), []bool{true, false})
			invalid := append([]byte(nil), valid...)
			invalid[len(invalid)-1] = 2
			prior, _ := streamReaderFixture(t, columns, bytes.NewReader(valid), len(valid))
			// Each reader has its own mutable skip scratch, cursor and sticky error.
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, data := range [][]byte{valid, invalid, valid} {
						scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{DiscardBytes: 3})
						if err != nil {
							t.Error(err)
							return
						}
						next, err := newStreamRowReaderReusingValidation(columns, scope, prior)
						if err != nil {
							t.Error(err)
							return
						}
						err = next.SkipColumn()
						if (err != nil) != (data[len(data)-1] == 2) {
							t.Error("nested boolean validation changed", err)
							return
						}
						if err == nil {
							if err := next.RequireEnd(); err != nil {
								t.Error(err)
							}
							if err := scope.RequireEnd(); err != nil {
								t.Error(err)
							}
						}
					}
				}()
			}
			wg.Wait()
			if prior.shared.err != nil || prior.index != 0 {
				t.Fatal("independent readers mutated prior page")
			}
		})
		t.Run("Ownership", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			columns := NewColumns(ColumnSpec{Name: "v", Type: integer})
			data := streamCellBytes([]byte{0, 0, 0, 7})
			prior, _ := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
			plan := CodecTestMust(StreamOneColumn(StreamInt32).Compile(columns))
			var old int32
			if err := plan.DecodeInto(prior, &old); err != nil || old != 7 {
				t.Fatal(old, err)
			}
			prior.shared.fail(errors.New("previous page failure"))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			scope := CodecTestMust(wire.NewStream(ctx, bytes.NewReader(data), int64(len(data)), wire.StreamLimits{DiscardBytes: 17}))
			next := CodecTestMust(newStreamRowReaderReusingValidation(columns, scope, prior))
			if next == prior || next.shared == prior.shared || next.scope != scope || next.Context() != ctx {
				t.Fatal("new page inherited mutable reader state")
			}
			if next.validations[0] != prior.validations[0] || &next.validations[0] != &prior.validations[0] {
				t.Fatal("identical immutable metadata did not share validation storage")
			}
			if next.shared.discardBytes != 17 {
				t.Fatal("previous page limits inherited", next.shared.discardBytes)
			}
			// Releasing the old reader must not invalidate the new page's plans.
			*prior = StreamRowReader{}
			var value int32
			if err := plan.DecodeInto(next, &value); err != nil || value != 7 {
				t.Fatal(value, err)
			}
			if err := next.RequireEnd(); err != nil {
				t.Fatal(err)
			}
			// Cancellation is enforced where a page reads its source.
			cancel()
			scope = CodecTestMust(wire.NewStream(ctx, bytes.NewReader(data), int64(len(data)), wire.StreamLimits{}))
			again := CodecTestMust(newStreamRowReaderReusingValidation(columns, scope, next))
			if err := plan.DecodeInto(again, &value); !errors.Is(err, context.Canceled) {
				t.Fatal("new page context not enforced", err)
			}
			if _, err := newStreamRowReaderReusingValidation(columns, nil, next); err == nil {
				t.Fatal("nil page scope accepted")
			}
		})
		t.Run("UsesNewEnvelopeLimits", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			list := CodecTestMust(ListType(integer))
			columns := NewColumns(ColumnSpec{Name: "v", Type: list})
			data := streamEncodedFixture(t, list, ListEncoder(Int32Codec), []int32{1, 2})
			prior, _ := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
			if err := prior.SkipColumn(); err != nil {
				t.Fatal(err)
			}
			for _, limits := range []wire.StreamLimits{
				{MaxDepth: 1},
				{MaxCount: 1},
			} {
				scope := CodecTestMust(wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), limits))
				next := CodecTestMust(newStreamRowReaderReusingValidation(columns, scope, prior))
				if err := next.SkipColumn(); !errors.Is(err, wire.ErrLimit) {
					t.Fatal("shared validation bypassed stricter envelope limit", limits, err)
				}
			}
			if _, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{MaxBytes: int64(len(data) - 1)}); !errors.Is(err, wire.ErrLimit) {
				t.Fatal("new envelope byte limit bypassed", err)
			}
		})
	})
	t.Run("Reader", func(t *testing.T) {
		_ = context.Canceled
		integer := FoundationScalar(t, TypeInt)
		cols := NewColumns(ColumnSpec{Name: "v", Type: integer})
		data := streamCellBytes([]byte{0, 0, 0, 1})
		reader, _ := streamReaderFixture(t, cols, bytes.NewReader(data), len(data))
		plan := CodecTestMust(StreamOneColumn(StreamInt32).Compile(cols))
		var dst int32
		if err := plan.DecodeInto(reader, &dst); err != nil {
			t.Fatal(err)
		}
		if err := plan.DecodeInto(reader, &dst); err == nil {
			t.Fatal("second row read without resetting cursor")
		}
	})
	t.Run("Reset", func(t *testing.T) {
		t.Run("AbsentCustomResetterSucceeds", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			udt := CodecTestMust(UDTType("ks", "one", ColumnSpec{Name: "id", Type: integer}))
			options := UDTOptions{Keyspace: "ks", Name: "one", AllowMissingFields: true, AllowExtraDatabaseFields: true}
			row := StreamRowInto(func(*int32) {}, StreamColumn("missing", customStreamResettable{}, func(value *int32) *int32 { return value }))
			value := int32(42)
			if err := StreamDecodeFixture(t, udt, StreamUDT(row, options), streamCellBytes(streamCellBytes([]byte{0, 0, 0, 7})), &value, 1); err != nil || value != 0 {
				t.Fatal("custom declaration reset", value, err)
			}
		})
		t.Run("MissingPayloadPreservesSeeds", func(t *testing.T) {
			type destination struct {
				ID      int32
				Payload []byte
				Nested  [][]byte
				Custom  int32
				Wrapped Nullable[int32]
			}
			integer := FoundationScalar(t, TypeInt)
			udt := CodecTestMust(UDTType("ks", "one", ColumnSpec{Name: "id", Type: integer}))
			options := UDTOptions{Keyspace: "ks", Name: "one", AllowMissingFields: true}
			row := StreamRowInto(func(*destination) {}, StreamColumn("id", StreamInt32, func(d *destination) *int32 { return &d.ID }), StreamColumn("payload", StreamBlob, func(d *destination) *[]byte { return &d.Payload }), StreamColumn("nested", StreamList(StreamBlob), func(d *destination) *[][]byte { return &d.Nested }))
			backing, child := make([]byte, 16), make([]byte, 32)
			out := destination{Payload: backing[:10], Nested: [][]byte{child[:12]}}
			nestedStart := &out.Nested[0]
			data := streamCellBytes(streamCellBytes([]byte{0, 0, 0, 7}))
			for i := 0; i < 2; i++ {
				if err := StreamDecodeFixture(t, udt, StreamUDT(row, options), data, &out, 1); err != nil || out.ID != 7 || len(out.Payload) != 0 || cap(out.Payload) != 16 || len(out.Nested) != 0 || cap(out.Nested) != 1 {
					t.Fatal("missing payload reset", out, err)
				}
				out.Payload = out.Payload[:1]
				out.Nested = out.Nested[:1]
				if &out.Payload[0] != &backing[0] || &out.Nested[0] != nestedStart || cap(out.Nested[0]) != 32 {
					t.Fatal("reset lost seeded storage")
				}
			}
			// A custom decoder of an absent field resets it, wrapped or not.
			absent := StreamRowInto(nil, StreamColumn("id", StreamInt32, func(d *destination) *int32 { return &d.ID }),
				StreamColumn("custom", StreamNullable(customStreamResettable{}), func(d *destination) *Nullable[int32] { return &d.Wrapped }),
				StreamColumn("plain", customStreamResettable{}, func(d *destination) *int32 { return &d.Custom }))
			out = destination{Custom: 5, Wrapped: Nullable[int32]{Value: 6, Valid: true}}
			if err := StreamDecodeFixture(t, udt, StreamUDT(absent, options), data, &out, 1); err != nil || out.ID != 7 || out.Custom != 0 || out.Wrapped.Valid || out.Wrapped.Value != 0 {
				t.Fatal("absent custom fields", out, err)
			}
		})
	})
	t.Run("Row", func(t *testing.T) {
		t.Run("DuplicateNamesAndMissingUDTFields", func(t *testing.T) {
			integer := FoundationScalar(t, TypeInt)
			columns := NewColumns(ColumnSpec{Name: "dup", Type: integer}, ColumnSpec{Name: "dup", Type: integer})
			data := append(streamCellBytes([]byte{0, 0, 0, 1}), streamCellBytes([]byte{0, 0, 0, 2})...)
			decoder := StreamRowInto(func(*Pair[int32, int32]) {}, StreamColumnAt(0, StreamInt32, func(p *Pair[int32, int32]) *int32 { return &p.First }), StreamColumnAt(1, StreamInt32, func(p *Pair[int32, int32]) *int32 { return &p.Second }))
			plan := CodecTestMust(decoder.Compile(columns))
			reader, _ := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
			var dst Pair[int32, int32]
			plan.Reset(&dst)
			if err := plan.DecodeInto(reader, &dst); err != nil || dst.First != 1 || dst.Second != 2 {
				t.Fatal("duplicate positional names", dst, err)
			}
			if _, err := StreamRowInto(func(*int32) {}, StreamColumn("dup", StreamInt32, func(v *int32) *int32 { return v })).Compile(columns); err == nil {
				t.Fatal("ambiguous result alias accepted")
			}
			type value struct{ ID, Missing int32 }
			udt := CodecTestMust(UDTType("ks", "entry", ColumnSpec{Name: "id", Type: integer}))
			row := StreamRowInto(func(*value) {}, StreamColumn("id", StreamInt32, func(v *value) *int32 { return &v.ID }), StreamColumn("missing", StreamInt32, func(v *value) *int32 { return &v.Missing }))
			out := value{Missing: 9}
			options := UDTOptions{Keyspace: "ks", Name: "entry", AllowMissingFields: true}
			if err := StreamDecodeFixture(t, udt, StreamUDT(row, options), streamCellBytes(streamCellBytes([]byte{0, 0, 0, 7})), &out, 1); err != nil || out.ID != 7 || out.Missing != 0 {
				t.Fatal("missing application field reset", out, err)
			}
			extra := CodecTestMust(UDTType("ks", "entry", ColumnSpec{Name: "id", Type: integer}, ColumnSpec{Name: "extra", Type: integer}))
			options.AllowExtraDatabaseFields = true
			var single int32
			if err := StreamDecodeFixture(t, extra, StreamUDT(StreamOneColumn(StreamInt32), options), streamCellBytes(data), &single, 1); err != nil || single != 1 {
				t.Fatal("single-column UDT policy", single, err)
			}
		})
		t.Run("RecordsAndEncodedOutput", func(t *testing.T) {
			type record struct {
				ID       int32
				Name     Nullable[[]byte]
				Unmapped int
			}
			integer, text := FoundationScalar(t, TypeInt), FoundationScalar(t, TypeText)
			tuple := CodecTestMust(TupleType(integer, text))
			encoder := TupleEncoder(Params(ParamAt(0, Int32Codec, func(r record) int32 { return r.ID }), ParamAt(1, NullableEncoder(ByteSliceCodec), func(r record) Nullable[[]byte] { return r.Name })))
			_ = encoder // Text uses a typed string input fixture below.
			params := TupleEncoder(PairParams(OneParam(Int32Codec), OneParam(NullableEncoder(StringCodec))))
			data := streamEncodedFixture(t, tuple, params, Pair[int32, Nullable[string]]{First: 7, Second: Nullable[string]{Value: "name", Valid: true}})
			row := StreamRowInto(func(r *record) { r.Unmapped = 0 }, StreamColumnAt(0, StreamInt32, func(r *record) *int32 { return &r.ID }), StreamColumnAt(1, StreamNullable(StreamText), func(r *record) *Nullable[[]byte] { return &r.Name }))
			backing := make([]byte, 10)
			out := record{Name: Nullable[[]byte]{Value: backing[:0]}, Unmapped: 9}
			if err := StreamDecodeFixture(t, tuple, StreamTuple(row), data, &out, 1); err != nil || out.ID != 7 || string(out.Name.Value) != "name" || out.Unmapped != 0 || &out.Name.Value[0] != &backing[0] {
				t.Fatal("tuple", out, err)
			}
			// Omitted nullable suffix must preserve destination capacity and reset Valid.
			short := streamCellBytes(data[4:12])
			if err := StreamDecodeFixture(t, tuple, StreamTuple(row), short, &out, 1); err != nil || out.Name.Valid || cap(out.Name.Value) != 10 {
				t.Fatal("omitted suffix", out, err)
			}
			udt := CodecTestMust(UDTType("ks", "entry", ColumnSpec{Name: "id", Type: integer}, ColumnSpec{Name: "name", Type: text}))
			named := StreamRowInto(func(r *record) { r.Unmapped = 0 }, StreamColumn("id", StreamInt32, func(r *record) *int32 { return &r.ID }), StreamColumn("name", StreamNullable(StreamText), func(r *record) *Nullable[[]byte] { return &r.Name }))
			decoder := StreamUDT(named, UDTOptions{Keyspace: "ks", Name: "entry"})
			if err := StreamDecodeFixture(t, udt, decoder, data, &out, 1); err != nil || out.ID != 7 {
				t.Fatal("UDT", err)
			}
			if _, err := StreamUDT(named, UDTOptions{}).Compile(udt); err == nil {
				t.Fatal("missing identity accepted")
			}
			if _, err := StreamUDT(named, UDTOptions{Keyspace: "ks", Name: "wrong"}).Compile(udt); err == nil {
				t.Fatal("wrong identity accepted")
			}
			strict := StreamRowInto(func(r *record) {}, StreamColumn("id", StreamInt32, func(r *record) *int32 { return &r.ID }))
			if _, err := StreamUDT(strict, UDTOptions{Keyspace: "ks", Name: "entry"}).Compile(udt); err == nil {
				t.Fatal("extra UDT field accepted without policy")
			}
			relaxed := StreamUDT(strict, UDTOptions{Keyspace: "ks", Name: "entry", AllowExtraDatabaseFields: true})
			if err := StreamDecodeFixture(t, udt, relaxed, data, &out, 1); err != nil {
				t.Fatal("checked UDT skip", err)
			}
			for _, typ := range []DataType{tuple, udt} {
				encoded := StreamCell{Bytes: make([]byte, 0, len(data)-4)}
				if err := StreamDecodeFixture(t, typ, StreamEncoded, data, &encoded, 1); err != nil || !bytes.Equal(encoded.Bytes, data[4:]) || !encoded.Type.Equal(typ) {
					t.Fatalf("encoded %s: %x %v", typ, encoded.Bytes, err)
				}
			}
			// Encoded output must still reject malformed nested semantics.
			bad := append([]byte(nil), data...)
			bad[len(bad)-1] = 0xff
			encoded := StreamCell{Bytes: make([]byte, 0, len(data))}
			if err := StreamDecodeFixture(t, tuple, StreamEncoded, bad, &encoded, 1); err == nil {
				t.Fatal("encoded output skipped text validation")
			}
			zero := CodecTestMust(TupleType())
			unit := EmptyRow{}
			empty := streamCellBytes(nil)
			if err := StreamDecodeFixture(t, zero, StreamTuple(StreamRowInto(func(*EmptyRow) {})), empty, &unit, 1); err != nil {
				t.Fatal("zero tuple", err)
			}
			wrapped := MaybeEmpty[EmptyRow]{}
			if err := StreamDecodeFixture(t, zero, StreamEmpty(StreamTuple(StreamRowInto(func(*EmptyRow) {}))), empty, &wrapped, 1); err != nil || !wrapped.Empty {
				t.Fatal("zero tuple Empty", err)
			}
		})
		t.Run("SkippedValuesBoundedAndChecked", func(t *testing.T) {
			boolean, text, blob := FoundationScalar(t, TypeBoolean), FoundationScalar(t, TypeText), FoundationScalar(t, TypeBlob)
			ignored := StreamRowInto(nil, StreamIgnoreColumn[EmptyRow]("ignored"))
			for _, tc := range []struct {
				typ  DataType
				body []byte
			}{{boolean, []byte{2}}, {text, []byte{0xff}}, {CodecTestMust(ListType(boolean)), []byte{0, 0, 0, 1, 0, 0, 0, 1, 2}}} {
				cols := NewColumns(ColumnSpec{Name: "ignored", Type: tc.typ})
				data := streamCellBytes(tc.body)
				r, _ := streamReaderFixture(t, cols, &streamChunkSource{data: data, step: 1}, len(data))
				plan := CodecTestMust(ignored.Compile(cols))
				dst := EmptyRow{}
				if err := plan.DecodeInto(r, &dst); err == nil {
					t.Fatal("malformed ignored value accepted")
				}
			}
			cols := NewColumns(ColumnSpec{Name: "large", Type: blob})
			data := streamCellBytes(make([]byte, 8<<20))
			source := &streamChunkSource{data: data}
			r, scope := streamReaderFixture(t, cols, source, len(data))
			plan := CodecTestMust(StreamRowInto(nil, StreamIgnoreColumn[EmptyRow]("large")).Compile(cols))
			dst := EmptyRow{}
			if err := plan.DecodeInto(r, &dst); err != nil {
				t.Fatal(err)
			}
			if err := scope.RequireEnd(); err != nil {
				t.Fatal(err)
			}
			if len(r.shared.discard) != 127 || source.reads > (len(data)/127)+3 {
				t.Fatal("discard storage/read bound", len(r.shared.discard), source.reads)
			}
		})
	})
	t.Run("Scalar", func(t *testing.T) {
		t.Run("BytesCapacityNullAndText", func(t *testing.T) {
			typ := FoundationScalar(t, TypeText)
			payload := bytes.Repeat([]byte("A€𐍈"), 1024)
			data := streamCellBytes(payload)
			for _, step := range []int{1, 2, 3, 7, 32, 1024} {
				backing := make([]byte, len(payload))
				dst := backing[:0]
				if err := StreamDecodeFixture(t, typ, StreamText, data, &dst, step); err != nil {
					t.Fatal(err)
				}
				if &dst[0] != &backing[0] || !bytes.Equal(dst, payload) {
					t.Fatal("text changed storage/content")
				}
			}
			dst := make([]byte, 0, len(payload)-1)
			source := &streamChunkSource{data: data}
			cols := NewColumns(ColumnSpec{Name: "text", Type: typ})
			r, _ := streamReaderFixture(t, cols, source, len(data))
			plan := CodecTestMust(StreamOneColumn(StreamText).Compile(cols))
			err := plan.DecodeInto(r, &dst)
			if _, ok := errors.AsType[*StreamCapacityError](err); !ok || source.offset != 4 {
				t.Fatalf("capacity consumed body: %v read %d", err, source.offset)
			}
			var invalid []byte = make([]byte, 0, 10)
			for _, bad := range [][]byte{{0xff}, {0xe0, 0x80, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80}, {0xe2, 0x82}} {
				if err := StreamDecodeFixture(t, typ, StreamText, streamCellBytes(bad), &invalid, 1); err == nil {
					t.Fatalf("invalid text %x", bad)
				}
			}
			if err := StreamDecodeFixture(t, FoundationScalar(t, TypeASCII), StreamASCII, streamCellBytes([]byte{0x80}), &invalid, 1); err == nil {
				t.Fatal("invalid ASCII accepted")
			}
			backing := make([]byte, 12)
			nullable := Nullable[[]byte]{Value: backing[:6], Valid: true}
			if err := StreamDecodeFixture(t, typ, StreamNullable(StreamText), []byte{255, 255, 255, 255}, &nullable, 1); err != nil || nullable.Valid || len(nullable.Value) != 0 || cap(nullable.Value) != 12 {
				t.Fatalf("nullable reset: %+v %v", nullable, err)
			}
			if err := StreamDecodeFixture(t, typ, StreamNullable(StreamText), streamCellBytes(nil), &nullable, 1); err != nil || !nullable.Valid || len(nullable.Value) != 0 || cap(nullable.Value) != 12 {
				t.Fatal("present empty lost null distinction", err)
			}
			if err := StreamDecodeFixture(t, typ, StreamText, []byte{255, 255, 255, 254}, &invalid, 1); err == nil {
				t.Fatal("unset result accepted")
			}
			empty := MaybeEmpty[int32]{Value: 1}
			if err := StreamDecodeFixture(t, FoundationScalar(t, TypeInt), StreamEmpty(StreamInt32), streamCellBytes(nil), &empty, 1); err != nil || !empty.Empty {
				t.Fatal("logical Empty", err)
			}
			if _, err := StreamEmpty(StreamText).Compile(typ); err == nil {
				t.Fatal("text Empty accepted")
			}
		})
		t.Run("DirectReadTargets", func(t *testing.T) {
			payload := bytes.Repeat([]byte{0x71}, 500<<10)
			data := streamCellBytes(payload)
			dst := make([]byte, 0, len(payload))
			start := uintptr(unsafe.Pointer(unsafe.SliceData(dst)))
			end := start + uintptr(cap(dst))
			reads := 0
			source := &streamChunkSource{data: data, step: 4093, record: func(p []byte, offset int) {
				if offset < 4 {
					return
				}
				ptr := uintptr(unsafe.Pointer(unsafe.SliceData(p)))
				if ptr < start || ptr+uintptr(len(p)) > end {
					t.Fatal("payload source was read into driver storage")
				}
				reads++
			}}
			typ := FoundationScalar(t, TypeBlob)
			cols := NewColumns(ColumnSpec{Name: "v", Type: typ})
			r, scope := streamReaderFixture(t, cols, source, len(data))
			p := CodecTestMust(StreamOneColumn(StreamBlob).Compile(cols))
			if err := p.DecodeInto(r, &dst); err != nil {
				t.Fatal(err)
			}
			if err := scope.RequireEnd(); err != nil || reads < 2 || !bytes.Equal(dst, payload) {
				t.Fatal("direct ranges", err)
			}
		})
		t.Run("Domains", func(t *testing.T) {
			streamScalarCase(t, TypeBoolean, BoolCodec, StreamBool, true)
			streamScalarCase(t, TypeTinyInt, Int8Codec, StreamInt8, int8(-128))
			streamScalarCase(t, TypeSmallInt, Int16Codec, StreamInt16, int16(-32768))
			streamScalarCase(t, TypeInt, Int32Codec, StreamInt32, int32(math.MinInt32))
			streamScalarCase(t, TypeBigInt, Int64Codec, StreamInt64, int64(math.MinInt64))
			streamScalarCase(t, TypeFloat, Float32Codec, StreamFloat32, float32(-1.25))
			streamScalarCase(t, TypeDouble, Float64Codec, StreamFloat64, math.Inf(-1))
			streamScalarCase(t, TypeCounter, CounterCodec, StreamCounter, Counter(-1))
			streamScalarCase(t, TypeDate, DateCodec, StreamDate, Date(math.MinInt32))
			streamScalarCase(t, TypeTime, TimeCodec, StreamTime, TimeOfDay(nanosecondsPerDay-1))
			streamScalarCase(t, TypeTimestamp, TimestampCodec, StreamTimestamp, Timestamp(math.MinInt64))
			streamScalarCase(t, TypeDuration, DurationCodec, StreamDuration, Duration{Months: math.MinInt32, Days: math.MinInt32, Nanoseconds: math.MinInt64})
			streamScalarCase(t, TypeUUID, UUIDCodec, StreamUUID, UUID{1, 2, 3, 4, 5})
			streamScalarCase(t, TypeTimeUUID, TimeUUIDCodec, StreamTimeUUID, TimeUUID(newTimeUUID()))
			streamScalarCase(t, TypeInet, InetCodec, StreamInet, netip.MustParseAddr("2001:db8::1"))
			streamScalarCase(t, TypeInet, InetCodec, StreamInet, netip.MustParseAddr("127.0.0.1"))
			var badBool bool
			if err := StreamDecodeFixture(t, FoundationScalar(t, TypeBoolean), StreamBool, streamCellBytes([]byte{2}), &badBool, 1); err == nil {
				t.Fatal("invalid boolean accepted")
			}
			var badTime TimeOfDay
			bad := make([]byte, 8)
			binary.BigEndian.PutUint64(bad, uint64(nanosecondsPerDay))
			if err := StreamDecodeFixture(t, FoundationScalar(t, TypeTime), StreamTime, streamCellBytes(bad), &badTime, 1); err == nil {
				t.Fatal("invalid time accepted")
			}
			var id TimeUUID
			if err := StreamDecodeFixture(t, FoundationScalar(t, TypeTimeUUID), StreamTimeUUID, streamCellBytes(make([]byte, 16)), &id, 1); err == nil {
				t.Fatal("non-v1 UUID accepted")
			}
			var duration Duration
			if err := StreamDecodeFixture(t, FoundationScalar(t, TypeDuration), StreamDuration, streamCellBytes([]byte{1, 2, 0}), &duration, 1); err == nil {
				t.Fatal("mixed duration signs accepted")
			}
		})
		t.Run("NumericPayloadStorage", func(t *testing.T) {
			varint := FoundationScalar(t, TypeVarint)
			dst := make([]byte, 0, 6)
			want := []byte{0xff, 0xff, 0x80}
			if err := StreamDecodeFixture(t, varint, StreamVarint, streamCellBytes(want), &dst, 1); err != nil || !bytes.Equal(dst, want) {
				t.Fatal("signed varint bytes", err)
			}
			if err := StreamDecodeFixture(t, varint, StreamVarint, streamCellBytes(nil), &dst, 1); err == nil {
				t.Fatal("empty varint accepted")
			}
			decimal := FoundationScalar(t, TypeDecimal)
			value := Decimal{Scale: -7, Unscaled: CodecTestMust(VarintFromBytes([]byte{0x80, 0}))}
			data := streamEncodedFixture(t, decimal, DecimalCodec, value)
			out := DecimalParts{Unscaled: make([]byte, 0, 2)}
			if err := StreamDecodeFixture(t, decimal, StreamDecimal, data, &out, 1); err != nil || out.Scale != -7 || !bytes.Equal(out.Unscaled, []byte{0x80, 0}) {
				t.Fatal("decimal", out, err)
			}
			source := &streamChunkSource{data: data}
			cols := NewColumns(ColumnSpec{Name: "d", Type: decimal})
			reader, _ := streamReaderFixture(t, cols, source, len(data))
			plan := CodecTestMust(StreamOneColumn(StreamDecimal).Compile(cols))
			short := DecimalParts{Unscaled: make([]byte, 0, 1)}
			if err := plan.DecodeInto(reader, &short); err == nil || source.offset != 4 {
				t.Fatalf("decimal capacity preflight: %v offset %d", err, source.offset)
			}
		})
	})
	t.Run("Validation", func(t *testing.T) {
		t.Run("RejectsUnsupportedScalarKinds", func(t *testing.T) {
			for _, kind := range []Type{TypeDuration + 1, 65535} {
				// Corrupt a private copy: ScalarType descriptors are shared.
				typ := typeFromDescriptor(&dataTypeDescriptor{Kind: kind})
				checks := []func() error{
					func() error { _, err := compileStreamValidation(typ, 0); return err },
					func() error {
						_, err := checkedTypeHeight(typ, 0, make(map[*dataTypeDescriptor]int), make(map[*dataTypeDescriptor]bool))
						return err
					},
				}
				for _, check := range checks {
					err := check()
					if validation, ok := errors.AsType[*ValidationError](err); !ok || validation.Field != "kind" {
						t.Fatalf("unsupported scalar %v must retain kind ValidationError: %v", kind, err)
					}
				}
			}
		})
	})
}

// TestStreamValueReaderAccessors decodes cells with a custom decoder that
// reads the cell's type and state and its body as fixed-width integers.
func TestStreamValueReaderAccessors(t *testing.T) {
	blob := FoundationScalar(t, TypeBlob)
	body := []byte{0x12, 0x34, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	var got streamWords
	for _, step := range []int{1, len(body) + 4} {
		if err := StreamDecodeFixture(t, blob, streamWordsDecoder{}, streamCellBytes(body), &got, step); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if want := (streamWords{Type: blob, State: Present, Short: 0x1234, Long: 0x0123456789abcdef}); !got.Type.Equal(want.Type) || got.State != want.State || got.Short != want.Short || got.Long != want.Long {
			t.Fatalf("step %d: decoded %+v, want %+v", step, got, want)
		}
	}
	if err := StreamDecodeFixture(t, blob, streamWordsDecoder{}, []byte{0xff, 0xff, 0xff, 0xff}, &got, 1); err != nil || got.State != Null || !got.Type.Equal(blob) {
		t.Fatalf("null cell: %+v %v", got, err)
	}
	if err := StreamDecodeFixture(t, blob, streamWordsDecoder{}, streamCellBytes(body[:9]), &got, 1); err == nil {
		t.Fatal("Uint64 read past the end of the cell")
	}
	if err := StreamDecodeFixture(t, blob, streamWordsDecoder{}, streamCellBytes(body[:1]), &got, 1); err == nil {
		t.Fatal("Uint16 read past the end of the cell")
	}
}

// TestStreamResetters resets seeded storage through the Reset of each
// stream decoder: the logical value is cleared and buffer capacity is kept.
func TestStreamResetters(t *testing.T) {
	t.Run("Map", func(t *testing.T) {
		dst := []Entry[[]byte, [][]byte]{{Key: []byte("key"), Value: [][]byte{[]byte("value")}}}
		key := dst[0].Key
		StreamMap(StreamBlob, StreamList(StreamBlob)).Reset(&dst)
		if len(dst) != 0 || cap(dst) != 1 {
			t.Fatalf("map len %d cap %d", len(dst), cap(dst))
		}
		entry := dst[:1][0]
		if len(entry.Key) != 0 || cap(entry.Key) != cap(key) || &entry.Key[:1][0] != &key[0] || len(entry.Value) != 0 || cap(entry.Value) != 1 {
			t.Fatalf("entry not reset in place: %+v", entry)
		}
	})
	t.Run("Decimal", func(t *testing.T) {
		dst := DecimalParts{Unscaled: []byte{1, 2}, Scale: 3}
		StreamDecimal.Reset(&dst)
		if dst.Scale != 0 || len(dst.Unscaled) != 0 || cap(dst.Unscaled) != 2 {
			t.Fatalf("decimal %+v cap %d", dst, cap(dst.Unscaled))
		}
	})
	t.Run("Nullable", func(t *testing.T) {
		dst := Nullable[[]byte]{Valid: true, Value: []byte("text")}
		StreamNullable(StreamText).Reset(&dst)
		if dst.Valid || len(dst.Value) != 0 || cap(dst.Value) != 4 {
			t.Fatalf("nullable %+v", dst)
		}
	})
	t.Run("Empty", func(t *testing.T) {
		dst := MaybeEmpty[[]byte]{Empty: true, Value: []byte("text")}
		StreamEmpty(StreamText).Reset(&dst)
		if dst.Empty || len(dst.Value) != 0 || cap(dst.Value) != 4 {
			t.Fatalf("maybe-empty %+v", dst)
		}
	})
	t.Run("Encoded", func(t *testing.T) {
		dst := StreamCell{Type: FoundationScalar(t, TypeInt), State: Null, Bytes: []byte{1, 2}}
		StreamEncoded.Reset(&dst)
		if !dst.Type.Equal(DataType{}) || dst.State != Present || len(dst.Bytes) != 0 || cap(dst.Bytes) != 2 {
			t.Fatalf("cell %+v", dst)
		}
	})
	t.Run("OneColumn", func(t *testing.T) {
		dst := []byte("text")
		StreamOneColumn(StreamText).(interface{ Reset(*[]byte) }).Reset(&dst)
		if len(dst) != 0 || cap(dst) != 4 {
			t.Fatalf("column len %d cap %d", len(dst), cap(dst))
		}
	})
	t.Run("RowsAndRecords", func(t *testing.T) {
		resets := 0
		row := StreamRowInto(func(*Pair[int32, int32]) { resets++ },
			StreamColumnAt(0, StreamInt32, func(p *Pair[int32, int32]) *int32 { return &p.First }),
			StreamColumnAt(1, StreamInt32, func(p *Pair[int32, int32]) *int32 { return &p.Second }))
		dst := Pair[int32, int32]{1, 2}
		row.(interface{ Reset(*Pair[int32, int32]) }).Reset(&dst)
		if dst != (Pair[int32, int32]{}) || resets != 1 {
			t.Fatalf("row %+v after %d application resets", dst, resets)
		}
		dst = Pair[int32, int32]{3, 4}
		StreamTuple(row).Reset(&dst)
		if dst != (Pair[int32, int32]{}) || resets != 2 {
			t.Fatalf("tuple %+v after %d application resets", dst, resets)
		}
	})
	t.Run("NilDestination", func(t *testing.T) {
		StreamMap(StreamBlob, StreamList(StreamBlob)).Reset(nil)
		StreamDecimal.Reset(nil)
		StreamNullable(StreamText).Reset(nil)
		StreamEmpty(StreamText).Reset(nil)
		StreamEncoded.Reset(nil)
		StreamOneColumn(StreamText).(interface{ Reset(*[]byte) }).Reset(nil)
		row := StreamRowInto(func(*Pair[int32, int32]) { t.Fatal("reset callback ran for a nil destination") })
		row.(interface{ Reset(*Pair[int32, int32]) }).Reset(nil)
		StreamTuple(row).Reset(nil)
	})
}
