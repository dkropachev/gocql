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
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func rowTestType(t testing.TB, kind cqltype.Type) cqltype.DataType {
	t.Helper()
	v, err := cqltype.ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func encodeTestParams[P any](t testing.TB, columns cqltype.Columns, codec ParamEncoder[P], value P) wire.Chain {
	t.Helper()
	plan, err := codec.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	builder := wire.NewBuilder(8)
	w := NewParamWriter(builder, columns, true)
	if err := plan.Encode(w, value); err != nil {
		t.Fatal(err)
	}
	if err := FinishParamWriter(w); err != nil {
		t.Fatal(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

type failingRowEncoder struct{}

func (failingRowEncoder) CheckType(cqltype.DataType) error { return nil }
func (failingRowEncoder) Encode(w *ValueWriter, value int32) error {
	if err := w.Uint32(uint32(value)); err != nil {
		return err
	}
	return fmt.Errorf("test encoder failure")
}

func TestRowsCodec(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		t.Run("ConcurrentAndMetadataSpecific", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: rowTestType(t, cqltype.TypeInt)})
			codec := OneColumn(Int32)
			plan, err := codec.Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			changed := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: rowTestType(t, cqltype.TypeText)})
			if _, err := codec.Compile(changed); err == nil {
				t.Fatal("accepted incompatible changed result type")
			}
			chain := encodeTestParams(t, columns, OneParam(Int32), int32(123))
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 100; j++ {
						value, err := plan.Decode(NewRowReader(columns, chain, nil))
						if err != nil || value != 123 {
							t.Errorf("value=%d err=%v", value, err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
		t.Run("RepeatedMarkersAndAmbiguousAliases", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "key", Type: rowTestType(t, cqltype.TypeInt)}, cqltype.ColumnSpec{Name: "key", Type: rowTestType(t, cqltype.TypeInt)})
			chain := encodeTestParams(t, columns, Params(Param("key", Int32, func(v int32) int32 { return v })), int32(9))
			if _, err := Row(Column("key", Int32, func(v *int32) *int32 { return v })).Compile(columns); err == nil {
				t.Fatal("named duplicate aliases must require positional mapping")
			}
			plan, err := PairRows(1, OneColumn(Int32), OneColumn(Int32)).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			result, err := plan.Decode(NewRowReader(columns, chain, nil))
			if err != nil || result.First != 9 || result.Second != 9 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if _, err := Params(ParamAt(0, Int32, func(v int32) int32 { return v })).Compile(columns); err == nil {
				t.Fatal("unaccounted parameter accepted")
			}
			if _, err := PairRows(-1, OneColumn(Int32), OneColumn(Int32)).Compile(columns); err == nil {
				t.Fatal("negative split accepted")
			}
		})
	})
	t.Run("DecodeInto", func(t *testing.T) {
		t.Run("CompositionPropagatesDestinations", func(t *testing.T) {
			integer := FoundationScalar(t, cqltype.TypeInt)
			cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "a", Type: integer}, cqltype.ColumnSpec{Name: "b", Type: integer})
			body := encodeTestParams(t, cols, PairParams(1, OneParam(Int32), OneParam(Int32)), Pair[int32, int32]{1, 2})
			var dst Pair[int32, int32]
			decodes, into := 0, 0
			left := directTestRowDecoder{&dst.First, &decodes, &into}
			right := directTestRowDecoder{&dst.Second, &decodes, &into}
			plan := CodecTestMust(PairRows(1, left, right).Compile(cols))
			if err := plan.DecodeInto(NewRowReader(cols, body, nil), &dst); err != nil || dst.First != 1 || dst.Second != 2 || decodes != 0 || into != 2 {
				t.Fatal("pair destination", dst, decodes, into, err)
			}
			applied := FoundationScalar(t, cqltype.TypeBoolean)
			casCols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "[applied]", Type: applied}, cqltype.ColumnSpec{Name: "a", Type: integer})
			casBody := encodeTestParams(t, casCols, PairParams(1, OneParam(Bool), OneParam(Int32)), Pair[bool, int32]{false, 7})
			var cas CASResult[int32]
			custom := directTestRowDecoder{&cas.Previous.Value, &decodes, &into}
			casPlan := CodecTestMust(CASRows(custom).Compile(casCols))
			if err := casPlan.DecodeInto(NewRowReader(casCols, casBody, nil), &cas); err != nil || !cas.Previous.Set || cas.Previous.Value != 7 || decodes != 0 {
				t.Fatal("CAS destination", cas, err)
			}
		})
		t.Run("UsesCallerFieldsAndResets", func(t *testing.T) {
			type inner struct {
				Number int32
				Text   cqltype.Text
			}
			type outer struct {
				Inner   inner
				Unused  int
				Padding [128]int64
			}
			integer, text := FoundationScalar(t, cqltype.TypeInt), FoundationScalar(t, cqltype.TypeText)
			tuple := CodecTestMust(cqltype.TupleType(integer, text))
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "record", Type: tuple})
			var dst outer
			checkAddress := true
			innerDecoder := Row(ColumnAt(0, Int32, func(value *inner) *int32 {
				if checkAddress && value != &dst.Inner {
					t.Fatal("record decoded through a complete temporary")
				}
				return &value.Number
			}), ColumnAt(1, TextCodec, func(value *inner) *cqltype.Text { return &value.Text }))
			decoder := Row(Column("record", TupleDecoder(innerDecoder), func(value *outer) *inner {
				if checkAddress && value != &dst {
					t.Fatal("outer row decoded through a temporary")
				}
				return &value.Inner
			}))
			params := OneParam(TupleEncoder(PairParams(1, OneParam(Int32), OneParam(StringCodec))))
			body := encodeTestParams(t, columns, params, Pair[int32, string]{First: 7, Second: "borrowed"})
			lease := wire.NewLease(body, nil)
			plan := CodecTestMust(decoder.Compile(columns))
			dst.Unused = 5
			dst.Padding[100] = 9
			reader := NewRowReader(columns, body, lease)
			if err := plan.DecodeInto(reader, &dst); err != nil {
				t.Fatal(err)
			}
			if err := FinishRowReader(reader); err != nil {
				t.Fatal(err)
			}
			if dst.Inner.Number != 7 || dst.Inner.Text.String() != "borrowed" || dst.Unused != 0 || dst.Padding[100] != 0 {
				t.Fatal("destination state", dst)
			}
			checkAddress = false
			cloned := decoder.Clone(dst)
			lease.Release()
			if cloned.Inner.Text.String() != "borrowed" {
				t.Fatal("schema independent record Clone")
			}
			err := plan.DecodeInto(NewRowReader(columns, body, nil), nil)
			if _, ok := errors.AsType[*cqltype.ValidationError](err); !ok {
				t.Fatal("nil destination", err)
			}
		})
	})
	t.Run("DirectOps", func(t *testing.T) {
		// A writer that owns its input copies blobs into the frame even on a
		// borrowing builder, as ByteSliceCodec does, so the caller may reuse them.
		t.Run("ParamsCopyOwnedInput", func(t *testing.T) {
			columns := directColumns(t)
			plan := CodecTestMust(directParams.Compile(columns))
			for _, p := range []ParamPlan[directRow]{plan, interfaceOnlyParams(plan)} {
				row := directRow{View: cqltype.BorrowBytes([]byte("view")), Owned: []byte("owned"), Blob: []byte("blob")}
				builder := wire.NewBuilder(16)
				w := NewParamWriter(builder, columns, true)
				SetParamOwnership(w, true)
				if err := p.Encode(w, row); err != nil {
					t.Fatal(err)
				}
				chain, err := builder.Freeze()
				if err != nil {
					t.Fatal(err)
				}
				copy(row.Owned, "XXXXX")
				copy(row.Blob, "YYYY")
				out := make([]byte, chain.Len())
				if _, err := chain.CopyTo(out); err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(out, []byte("owned")) || !bytes.Contains(out, []byte("blob")) {
					t.Fatalf("owned input was borrowed: %q", out)
				}
			}
		})
		// Built-in codecs compile into direct ops that write the same cells,
		// framing captures and errors as their codecs, owned or borrowed.
		t.Run("ParamsMatchCodecs", func(t *testing.T) {
			columns := directColumns(t)
			plan := CodecTestMust(directParams.Compile(columns))
			want := []directKind{directInt, directInt, directInt, directInt, directInt, directNone, directBytes, directString, directString, directBytes, directNone}
			for i, op := range plan.(paramsPlan[directRow]).ops {
				if op.kind != want[i] {
					t.Fatalf("parameter %d: op kind %d, want %d", i, op.kind, want[i])
				}
			}
			reference := interfaceOnlyParams(plan)
			view := cqltype.BorrowBytes([]byte("view"))
			rows := map[string]directRow{
				"values":  {I8: -8, I16: -1600, I32: -320000, I64: -64 << 40, Count: 7, View: view, Owned: []byte("owned"), Text: "tëxt", ASCII: "ascii", Blob: []byte{0, 1, 2}, Skipped: 9},
				"zero":    {View: view},
				"utf8":    {View: view, Text: string([]byte{0xff, 0xfe})},
				"ascii":   {View: view, ASCII: "ä"},
				"extreme": {I8: -128, I16: 32767, I32: -1 << 31, I64: 1<<63 - 1, Count: -1, View: view, Blob: make([]byte, 300)},
			}
			for name, row := range rows {
				for _, owned := range []bool{false, true} {
					got := encodeWith(t, plan, columns, row, owned)
					ref := encodeWith(t, reference, columns, row, owned)
					if !bytes.Equal(got.bytes, ref.bytes) || !reflect.DeepEqual(got.cells, ref.cells) || got.err != ref.err || got.index != ref.index {
						t.Fatalf("%s owned=%v:\n got %x %v %q %d\nwant %x %v %q %d", name, owned, got.bytes, got.cells, got.err, got.index, ref.bytes, ref.cells, ref.err, ref.index)
					}
				}
			}
			if got := encodeWith(t, plan, columns, rows["utf8"], true); got.err == "" {
				t.Fatal("invalid UTF-8 was encoded")
			}
		})
		// Built-in codecs compile into direct ops that decode the same values and
		// fail with the same errors as their codecs, keeping the owned blob's
		// capacity.
		t.Run("RowsMatchCodecs", func(t *testing.T) {
			columns := directColumns(t)
			plan := CodecTestMust(directRows.Compile(columns))
			want := []directKind{directInt, directInt, directInt, directInt, directInt, directBlob, directOwnedBlob, directNone, directNone, directNone, directNone}
			for i, op := range plan.(rowPlan[directRow]).ops {
				if op.kind != want[i] {
					t.Fatalf("column %d: op kind %d, want %d", i, op.kind, want[i])
				}
			}
			reference := interfaceOnlyRows(plan)
			full := func(edit func(i int, b []byte) ([]byte, bool)) []byte {
				bodies := [][]byte{{0xf8}, {0xf9, 0xc0}, {0xff, 0xfb, 0x1e, 0x00}, {0xff, 0xff, 0xc0, 0, 0, 0, 0, 0}, {0, 0, 0, 0, 0, 0, 0, 7}, []byte("view"), []byte("owned"), []byte("t"), []byte("a"), {1}, {0, 0, 0, 9}}
				var out []byte
				for i, b := range bodies {
					null := false
					if edit != nil {
						b, null = edit(i, b)
					}
					out = cell(out, b, null)
				}
				return out
			}
			cases := map[string][]byte{
				"values":    full(nil),
				"null int":  full(func(i int, b []byte) ([]byte, bool) { return b, i == 2 }),
				"short int": full(func(i int, b []byte) ([]byte, bool) { return map[bool][]byte{true: {1, 2}, false: b}[i == 3], false }),
				"null blob": full(func(i int, b []byte) ([]byte, bool) { return b, i == 6 }),
				"empty blobs": full(func(i int, b []byte) ([]byte, bool) {
					return map[bool][]byte{true: {}, false: b}[i == 5 || i == 6], false
				}),
				"truncated": full(nil)[:20],
			}
			for name, body := range cases {
				chain, _ := wire.NewChain(wire.Borrow(body))
				run := func(p RowPlan[directRow]) (directRow, string, int) {
					dst := directRow{Owned: make([]byte, 0, 64)}
					reader := NewRowReader(columns, chain, nil)
					err := p.DecodeInto(reader, &dst)
					msg := ""
					if err != nil {
						msg = err.Error()
					}
					return dst, msg, reader.index
				}
				got, gotErr, gotIndex := run(plan)
				ref, refErr, refIndex := run(reference)
				if gotErr != refErr || gotIndex != refIndex || fmt.Sprint(got) != fmt.Sprint(ref) {
					t.Fatalf("%s:\n got %+v %q %d\nwant %+v %q %d", name, got, gotErr, gotIndex, ref, refErr, refIndex)
				}
				if name == "values" && (gotErr != "" || got.I32 != -320000 || got.Count != 7 || string(got.Owned) != "owned" || cap(got.Owned) != 64) {
					t.Fatalf("values decoded as %+v (owned capacity %d)", got, cap(got.Owned))
				}
			}
		})
	})
	t.Run("FusedRow", func(t *testing.T) {
		t.Run("BlobAndGenericFallback", func(t *testing.T) {
			for name, input := range map[string][]byte{
				"present":   rowScalarFrame(5, []byte("bytes")),
				"empty":     rowScalarFrame(0, nil),
				"null":      rowScalarFrame(-1, nil),
				"unset":     rowScalarFrame(-2, nil),
				"truncated": rowScalarFrame(5, []byte("byte")),
			} {
				t.Run(name, func(t *testing.T) { checkRowScalarEquivalent(t, cqltype.TypeBlob, BlobCodec, input, false, false) })
			}
			checkRowScalarEquivalent(t, cqltype.TypeBlob, BlobCodec, rowScalarFrame(1, []byte{1}), false, true)
			checkRowScalarEquivalent(t, cqltype.TypeInt, BlobCodec, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, false)
			checkRowScalarEquivalent(t, cqltype.TypeFloat, Float32, rowScalarFrame(4, []byte{0, 0, 0, 0}), false, false)
			checkRowScalarEquivalent(t, cqltype.TypeBlob, BlobCodec, nil, true, false)

			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, cqltype.TypeBlob)})
			chain, _ := wire.NewChain(wire.Borrow(rowScalarFrame(3, []byte("abc"))))
			lease := wire.NewLease(chain, nil)
			if !lease.Retain() {
				t.Fatal("retain failed")
			}
			reader := NewRowReader(columns, chain, lease)
			var value cqltype.Bytes
			if err := readColumnInto(reader, BlobCodec, &value); err != nil {
				t.Fatal(err)
			}
			lease.Release()
			if !bytes.Equal(value.OwnedBytes(), []byte("abc")) {
				t.Fatal("retained view invalid")
			}
			lease.Release()
			if _, err := value.At(0); err != cqltype.ErrReleased {
				t.Fatalf("released view err=%v", err)
			}
		})
		t.Run("CompiledGuardsAndDestination", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: rowTestType(t, cqltype.TypeInt)})
			compiled, err := compileValueDecoder(Int32, rowTestType(t, cqltype.TypeInt))
			if err != nil {
				t.Fatal(err)
			}
			for _, decoder := range []Decoder[int32]{Int32, compiled} {
				checkRowScalarEquivalent(t, cqltype.TypeBlob, decoder, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, false)
				chain, _ := wire.NewChain(wire.Borrow(rowScalarFrame(3, []byte{1, 2, 3})))
				value := int32(99)
				if err := readColumnInto(NewRowReader(columns, chain, nil), decoder, &value); err == nil || value != 99 {
					t.Fatalf("destination overwritten: value=%d err=%v", value, err)
				}
				check := NewRowReader(columns, chain, nil)
				if err := readColumnInto(check, decoder, nil); err == nil || check.index != 1 {
					t.Fatalf("nil dst err=%v index=%d", err, check.index)
				}
			}
		})
		t.Run("CustomDecoderSnapshot", func(t *testing.T) {
			columns := cqltype.NewColumns(
				cqltype.ColumnSpec{Name: "first", Type: rowTestType(t, cqltype.TypeInt)},
				cqltype.ColumnSpec{Name: "second", Type: rowTestType(t, cqltype.TypeInt)},
			)
			data := append(rowScalarFrame(4, []byte{0, 0, 0, 17}), rowScalarFrame(4, []byte{0, 0, 0, 29})...)
			chain, _ := wire.NewChain(wire.Borrow(data))
			lease := wire.NewLease(chain, nil)
			defer lease.Release()
			reader := NewRowReader(columns, chain, lease)
			var snapshot ValueReader
			var calls int
			custom := capturedRowInt{Decoder: Int32, seen: &snapshot, calls: &calls}
			var first, second int32
			if err := readColumnInto(reader, custom, &first); err != nil {
				t.Fatal(err)
			}
			if err := readColumnInto(reader, Int32, &second); err != nil {
				t.Fatal(err)
			}
			stillFirst, err := Int32.Decode(snapshot)
			if err != nil || first != 17 || second != 29 || stillFirst != 17 || calls != 1 {
				t.Fatalf("custom snapshot: first=%d second=%d captured=%d calls=%d err=%v", first, second, stillFirst, calls, err)
			}
		})
		t.Run("IntegerSemantics", func(t *testing.T) {
			for name, input := range map[string][]byte{
				"positive":         rowScalarFrame(4, []byte{0, 0, 0, 42}),
				"negative":         rowScalarFrame(4, []byte{0xff, 0xff, 0xff, 0xfe}),
				"minimum":          rowScalarFrame(4, []byte{0x80, 0, 0, 0}),
				"null":             rowScalarFrame(-1, nil),
				"unset":            rowScalarFrame(-2, nil),
				"invalid_negative": rowScalarFrame(math.MinInt32, nil),
				"empty":            rowScalarFrame(0, nil),
				"short_width":      rowScalarFrame(3, []byte{1, 2, 3}),
				"long_width":       rowScalarFrame(5, []byte{1, 2, 3, 4, 5}),
				"truncated_body":   rowScalarFrame(4, []byte{1, 2, 3}),
				"truncated_prefix": []byte{0, 0, 0},
			} {
				t.Run(name, func(t *testing.T) { checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, input, false, false) })
			}
			checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, nil, true, false)
			checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, nil, true, true)
			checkRowScalarEquivalent(t, cqltype.TypeInt, Int32, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, true)
			checkRowScalarEquivalent(t, cqltype.TypeBlob, Int32, rowScalarFrame(4, []byte{1, 2, 3, 4}), false, false)
			checkRowScalarEquivalent(t, cqltype.TypeTinyInt, Int8, rowScalarFrame(1, []byte{0x80}), false, false)
			checkRowScalarEquivalent(t, cqltype.TypeSmallInt, Int16, rowScalarFrame(2, []byte{0x80, 0}), false, false)
			checkRowScalarEquivalent(t, cqltype.TypeBigInt, Int64, rowScalarFrame(8, []byte{0x80, 0, 0, 0, 0, 0, 0, 0}), false, false)
			checkRowScalarEquivalent(t, cqltype.TypeCounter, CounterCodec, rowScalarFrame(8, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}), false, false)
		})
	})
	t.Run("Params", func(t *testing.T) {
		t.Run("FailureRollsBackWholePlan", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "a", Type: rowTestType(t, cqltype.TypeInt)}, cqltype.ColumnSpec{Name: "b", Type: rowTestType(t, cqltype.TypeInt)})
			codec := PairParams(1, OneParam(Int32), OneParam[int32](failingRowEncoder{}))
			plan, err := codec.Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			builder := wire.NewBuilder(4)
			if err := builder.Uint8(0xff); err != nil {
				t.Fatal(err)
			}
			w := NewParamWriter(builder, columns, true)
			if err := plan.Encode(w, Pair[int32, int32]{First: 3, Second: 4}); err == nil {
				t.Fatal("expected encoder failure")
			}
			if builder.Len() != 1 || w.index != 0 {
				t.Fatalf("partial parameters escaped: %d bytes, %d parameters", builder.Len(), w.index)
			}
		})
		t.Run("OwnedDetachesOnlyBorrowed", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "blob", Type: rowTestType(t, cqltype.TypeBlob)})
			plan, err := OneParam(ByteSliceCodec).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte("original")
			builder := wire.NewBuilder(8)
			w := NewParamWriter(builder, columns, true)
			w.ownBorrowed = true
			if err := plan.Encode(w, payload); err != nil {
				t.Fatal(err)
			}
			chain, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			payload[0] = 'X'
			value, err := NewRowReader(columns, chain, nil).ReadColumn(BlobCodec)
			if err != nil || string(value.OwnedBytes()) != "original" {
				t.Fatalf("value=%q err=%v", value.OwnedBytes(), err)
			}
		})
	})
	t.Run("Row", func(t *testing.T) {
		t.Run("TruncatedIgnoredCellIsError", func(t *testing.T) {
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "unused", Type: rowTestType(t, cqltype.TypeBlob)})
			plan, err := Row[EmptyRecord](IgnoreColumn[EmptyRecord]("unused")).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			for _, malformed := range [][]byte{{0, 0, 0}, {0, 0, 0, 8, 1}, {255, 255, 255, 254}} {
				chain, _ := wire.NewChain(wire.Borrow(malformed))
				if _, err := plan.Decode(NewRowReader(columns, chain, nil)); err == nil {
					t.Fatalf("accepted malformed ignored cell: %x", malformed)
				}
			}
		})
		t.Run("TypedMappingAndDetachedClone", func(t *testing.T) {
			type input struct {
				Key  int32
				Body []byte
			}
			type output struct {
				Key  int32
				Body cqltype.Bytes
			}
			columns := cqltype.NewColumns(
				cqltype.ColumnSpec{Name: "body", Type: rowTestType(t, cqltype.TypeBlob)},
				cqltype.ColumnSpec{Name: "key", Type: rowTestType(t, cqltype.TypeInt)},
			)
			params := Params(
				Param("key", Int32, func(v input) int32 { return v.Key }),
				Param("body", ByteSliceCodec, func(v input) []byte { return v.Body }),
			)
			payload := []byte("borrowed payload")
			chain := encodeTestParams(t, columns, params, input{Key: 42, Body: payload})
			lease := wire.NewLease(chain, nil)
			rows := Row(
				Column("key", Int32, func(v *output) *int32 { return &v.Key }),
				Column("body", BlobCodec, func(v *output) *cqltype.Bytes { return &v.Body }),
			)
			plan, err := rows.Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			reader := NewRowReader(columns, chain, lease)
			row, err := plan.Decode(reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := FinishRowReader(reader); err != nil {
				t.Fatal(err)
			}
			if err := reader.cursor.RequireEnd(); err != nil {
				t.Fatal(err)
			}
			if row.Key != 42 || !bytes.Equal(row.Body.OwnedBytes(), payload) {
				t.Fatalf("wrong row: %+v", row)
			}
			clone := plan.Clone(row)
			lease.Release()
			if _, err := row.Body.At(0); !errors.Is(err, cqltype.ErrReleased) {
				t.Fatalf("expired row: %v", err)
			}
			payload[0] = 'X'
			if got := string(clone.Body.OwnedBytes()); got != "borrowed payload" {
				t.Fatalf("clone retained borrowed input: %q", got)
			}
		})
	})
}
