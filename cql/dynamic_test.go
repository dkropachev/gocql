// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func dynamicTestMust[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

// dynamicTestCollect reads every member of seq, detached from the page with
// clone, or returns no members and the failure.
func dynamicTestCollect[T any](seq iter.Seq2[T, error], clone func(T) T) ([]T, error) {
	var out []T
	for v, err := range seq {
		if err != nil {
			return nil, err
		}
		out = append(out, clone(v))
	}
	return out, nil
}
func dynamicTestDetach[T any](seq iter.Seq2[T, error], clone func(T) T) []T {
	return dynamicTestMust(dynamicTestCollect(seq, clone))
}
func cloneValue(v Value) Value { return v.Clone() }
func cloneEntry(v ValueEntry) ValueEntry {
	return ValueEntry{Key: v.Key.Clone(), Value: v.Value.Clone()}
}
func cloneField(v ValueField) ValueField {
	return ValueField{Name: v.Name, Value: v.Value.Clone()}
}
func dynamicTestType(kind Kind) DataType {
	return dynamicTestMust(ScalarType(kind))
}
func dynamicTestHex(s string) []byte { return dynamicTestMust(hex.DecodeString(s)) }
func dynamicTestDecode(t *testing.T, typ DataType, body []byte) Value {
	t.Helper()
	encoded := dynamicTestMust(newEncodedValue(typ, Present, BorrowBytes(body)))
	v, err := DecodeValue(DynamicCodec, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func dynamicTestRoundTrip(t *testing.T, value Value, golden string) Value {
	t.Helper()
	encoded, err := encodeValueBorrowed(value.Type(), DynamicCodec, value)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(encoded.Bytes().ownedBytes()); got != golden {
		t.Fatalf("wire body: got %s, want %s", got, golden)
	}
	decoded, err := DecodeValue(DynamicCodec, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Type().Equal(value.Type()) || decoded.State() != value.State() {
		t.Fatalf("type/state changed: %s/%d", decoded.Type(), decoded.State())
	}
	reencoded, err := encodeValueBorrowed(decoded.Type(), DynamicCodec, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded.Bytes().ownedBytes(), encoded.Bytes().ownedBytes()) {
		t.Fatal("reencoding changed canonical wire body")
	}
	return decoded
}

func dynamicTestRowBody(t *testing.T, cols Columns, values []Value) wire.Chain {
	t.Helper()
	builder := wire.NewBuilder(0)
	writer := newParamWriter(builder, cols, false)
	plan := dynamicTestMust(DynamicParams.Compile(cols))
	if err := plan.Encode(writer, values); err != nil {
		t.Fatal(err)
	}
	return dynamicTestMust(builder.Freeze())
}

func TestDynamic(t *testing.T) {
	t.Run("Params", func(t *testing.T) {
		t.Run("NamesCountsAndRollback", func(t *testing.T) {
			integer, text := dynamicTestType(KindInt), dynamicTestType(KindText)
			cols := NewColumns(ColumnSpec{Name: "a", Type: integer}, ColumnSpec{Name: "b", Type: text}, ColumnSpec{Name: "a", Type: integer})
			plan := dynamicTestMust(NamedDynamicParams.Compile(cols))
			builder := wire.NewBuilder(0)
			writer := newParamWriter(builder, cols, true)
			if err := plan.Encode(writer, []ValueField{{Name: "b", Value: TextValue("x")}, {Name: "a", Value: IntValue(9)}}); err != nil {
				t.Fatal(err)
			}
			if err := finishParamWriter(writer); err != nil {
				t.Fatal(err)
			}
			chain := dynamicTestMust(builder.Freeze())
			if got := hex.EncodeToString((bytesView(chain, wire.Ref{})).ownedBytes()); got != "000000040000000900000001780000000400000009" {
				t.Fatalf("server marker order: %s", got)
			}
			tests := []struct {
				name    string
				fields  []ValueField
				message string
			}{
				{"duplicate", []ValueField{{Name: "a", Value: IntValue(1)}, {Name: "a", Value: IntValue(2)}, {Name: "b", Value: TextValue("x")}}, "duplicate"},
				{"missing", []ValueField{{Name: "a", Value: IntValue(1)}}, "missing"},
				{"unused", []ValueField{{Name: "a", Value: IntValue(1)}, {Name: "b", Value: TextValue("x")}, {Name: "c", Value: IntValue(2)}}, "unused"},
				{"wrong type after first write", []ValueField{{Name: "a", Value: IntValue(1)}, {Name: "b", Value: IntValue(2)}}, "type"},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					b := wire.NewBuilder(0)
					if err := b.Uint8(0xaa); err != nil {
						t.Fatal(err)
					}
					w := newParamWriter(b, cols, true)
					err := plan.Encode(w, test.fields)
					if err == nil || !strings.Contains(err.Error(), test.message) {
						t.Fatalf("unexpected failure: %v", err)
					}
					if b.Len() != 1 || w.index != 0 {
						t.Fatalf("failed encoding leaked partial cells: %d bytes, index %d", b.Len(), w.index)
					}
				})
			}
			positional := dynamicTestMust(DynamicParams.Compile(cols))
			if err := positional.Encode(newParamWriter(wire.NewBuilder(0), cols, true), []Value{IntValue(1)}); err == nil {
				t.Fatal("wrong parameter count accepted")
			}
			mismatch := NewColumns(ColumnSpec{Name: "a", Type: text}, ColumnSpec{Name: "b", Type: text}, ColumnSpec{Name: "a", Type: integer})
			if err := positional.Encode(newParamWriter(wire.NewBuilder(0), mismatch, true), []Value{IntValue(1), TextValue("x"), IntValue(1)}); err == nil {
				t.Fatal("stale parameter metadata accepted")
			}
			if _, err := DynamicParams.Compile(NewColumns(ColumnSpec{Name: "x"})); err == nil {
				t.Fatal("invalid metadata compiled")
			}
			if _, err := NamedDynamicParams.Compile(NewColumns(ColumnSpec{Type: integer})); err == nil {
				t.Fatal("unnamed marker accepted by named binding")
			}
			unsetPlan := dynamicTestMust(DynamicParams.Compile(NewColumns(ColumnSpec{Name: "x", Type: integer})))
			w := newParamWriter(wire.NewBuilder(0), NewColumns(ColumnSpec{Name: "x", Type: integer}), false)
			if err := unsetPlan.Encode(w, []Value{UnsetValue(integer)}); err == nil || w.builder.Len() != 0 {
				t.Fatal("v3 unset accepted or did not roll back")
			}
			// Repeated names still validate each physical marker's actual type.
			incompatible := NewColumns(ColumnSpec{Name: "x", Type: integer}, ColumnSpec{Name: "x", Type: text})
			repeated := dynamicTestMust(NamedDynamicParams.Compile(incompatible))
			w = newParamWriter(wire.NewBuilder(0), incompatible, true)
			if err := repeated.Encode(w, []ValueField{{Name: "x", Value: IntValue(1)}}); err == nil || w.builder.Len() != 0 {
				t.Fatal("incompatible repeated marker type accepted or leaked bytes")
			}
		})
	})
	t.Run("Rows", func(t *testing.T) {
		t.Run("ComposeWithTypedRecords", func(t *testing.T) {
			integer, text := dynamicTestType(KindInt), dynamicTestType(KindText)
			tuple := dynamicTestMust(TupleType(integer, text))
			encoded := dynamicTestMust(newEncodedValue(tuple, Present, BorrowBytes(dynamicTestHex("0000000400000001"))))
			tupleRow := dynamicTestMust(DecodeValue(TupleDecoder(DynamicRows), encoded))
			trailing := dynamicTestMust(tupleRow.At(1))
			if trailing.State() != Null || !trailing.Type().Equal(text) {
				t.Fatal("dynamic tuple row lost omitted trailing null")
			}
			udt := dynamicTestMust(UDTType("ks", "record", ColumnSpec{Name: "n", Type: integer}, ColumnSpec{Name: "s", Type: text}))
			encoded = dynamicTestMust(encodeValueBorrowed(udt, UDTEncoder(NamedDynamicParams, UDTOptions{Keyspace: "ks", Name: "record"}), []ValueField{{Name: "s", Value: TextValue("borrowed")}, {Name: "n", Value: IntValue(2)}}))
			lease := wire.NewLease(bytesChain(encoded.Bytes()), nil)
			encoded.body = bytesView(bytesChain(encoded.body), wire.RefOf(lease))
			codec := UDTDecoder(DynamicRows, UDTOptions{Keyspace: "ks", Name: "record"})
			row := dynamicTestMust(DecodeValue(codec, encoded))
			clone := codec.Clone(row)
			lease.Release()
			if _, err := row.Get("s"); !errors.Is(err, ErrReleased) {
				t.Fatalf("nested dynamic row remained readable: %v", err)
			}
			clonedText := dynamicTestMust(dynamicTestMust(clone.Get("s")).Text())
			if clonedText.String() != "borrowed" {
				t.Fatal("unbound record codec did not clone dynamic row")
			}
			cols := NewColumns(ColumnSpec{Name: "n", Type: integer}, ColumnSpec{Name: "s", Type: text})
			chain := dynamicTestRowBody(t, cols, []Value{IntValue(3), TextValue("right")})
			paired := dynamicTestMust(PairRows(OneColumn(Int32Codec), DynamicRows).Compile(cols))
			pair := dynamicTestMust(decodeRow(paired, newRowReader(cols, chain, nil)))
			if pair.First != 3 || pair.Second.Columns().Len() != 1 {
				t.Fatal("paired row did not retain physical subset")
			}
			second := dynamicTestMust(dynamicTestMust(pair.Second.Get("s")).Text())
			if second.String() != "right" {
				t.Fatal("paired row cursor offset changed")
			}
		})
		t.Run("ConcurrentPlansAndReaders", func(t *testing.T) {
			integer, text := dynamicTestType(KindInt), dynamicTestType(KindText)
			cols := NewColumns(ColumnSpec{Name: "n", Type: integer}, ColumnSpec{Name: "s", Type: text}, ColumnSpec{Name: "n", Type: integer})
			input := []ValueField{{Name: "s", Value: TextValue("shared")}, {Name: "n", Value: IntValue(42)}}
			params := dynamicTestMust(NamedDynamicParams.Compile(cols))
			rows := dynamicTestMust(DynamicRows.Compile(cols))
			chain := dynamicTestRowBody(t, cols, []Value{IntValue(42), TextValue("shared"), IntValue(42)})
			lease := wire.NewLease(chain, nil)
			defer lease.Release()
			row := dynamicTestMust(decodeRow(rows, newRowReader(cols, chain, lease)))
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := 0; n < 100; n++ {
						b := wire.NewBuilder(0)
						w := newParamWriter(b, cols, true)
						if err := params.Encode(w, input); err != nil {
							t.Error(err)
							return
						}
						got, err := row.At(n % 3)
						if err != nil {
							t.Error(err)
							return
						}
						if n%3 == 1 {
							s, e := got.Text()
							if e != nil || s.String() != "shared" {
								t.Error("concurrent text access failed", e)
								return
							}
						} else {
							v, e := got.Signed()
							if e != nil || v != 42 {
								t.Error("concurrent integer access failed", e)
								return
							}
						}
						independent, err := decodeRow(rows, newRowReader(cols, chain, lease))
						if err != nil {
							t.Error(err)
							return
						}
						if _, err := independent.At(2); err != nil {
							t.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
		t.Run("CopiedIndexAndClone", func(t *testing.T) {
			blob := BlobType
			specs := make([]ColumnSpec, 128)
			values := make([]Value, 128)
			for i := range specs {
				specs[i] = ColumnSpec{Name: fmt.Sprint("v", i), Type: blob}
				values[i] = BlobValue([]byte{byte(i)})
			}
			cols := NewColumns(specs...)
			chain := dynamicTestRowBody(t, cols, values)
			lease := wire.NewLease(chain, nil)
			plan := CodecTestMust(DynamicRows.Compile(cols))
			row := CodecTestMust(decodeRow(plan, newRowReader(cols, chain, lease)))
			copyRow := row
			if row.physical == nil || row.physical != copyRow.physical || row.physical.cells != nil {
				t.Fatal("eager or unshared physical index")
			}
			start := make(chan struct{})
			var workers sync.WaitGroup
			for worker := 0; worker < 16; worker++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					for j := 0; j < 32; j++ {
						value, e := copyRow.Get("v127")
						if e != nil {
							t.Error(e)
							return
						}
						b, e := value.Bytes()
						if e != nil {
							t.Error(e)
							return
						}
						if got, e := b.At(0); e != nil || got != 127 {
							t.Error(got, e)
							return
						}
					}
				}()
			}
			close(start)
			workers.Wait()
			if len(row.physical.cells) != 128 {
				t.Fatal("index not published once")
			}
			clone := row.Clone()
			if clone.physical == row.physical || clone.physical.cells != nil {
				t.Fatal("Clone retained mutable row index")
			}
			lease.Release()
			if _, e := row.At(127); !errors.Is(e, ErrReleased) {
				t.Fatal("cached index bypassed lease", e)
			}
			if _, e := copyRow.At(127); !errors.Is(e, ErrReleased) {
				t.Fatal("copy cached index bypassed lease", e)
			}
			value := CodecTestMust(clone.Get("v127"))
			owned := CodecTestMust(value.Bytes())
			if got := CodecTestMust(owned.At(0)); got != 127 {
				t.Fatal(got)
			}
		})
		t.Run("PhysicalSchemaLazyAccessAndClone", func(t *testing.T) {
			integer, text := dynamicTestType(KindInt), dynamicTestType(KindText)
			tuple := dynamicTestMust(TupleType(integer, text))
			cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "same", Type: integer}, ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "same", Type: text}, ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "pair", Type: tuple})
			source := dynamicTestRowBody(t, cols, []Value{IntValue(7), TextValue("hello"), dynamicTestMust(TupleValue(tuple, []Value{IntValue(8), TextValue("nested")}))})
			data := (bytesView(source, wire.Ref{})).ownedBytes()
			spans := make([]wire.Span, len(data))
			for i := range data {
				spans[i] = wire.Borrow(data[i : i+1])
			}
			chain := dynamicTestMust(wire.NewChain(spans...))
			lease := wire.NewLease(chain, nil)
			plan := dynamicTestMust(DynamicRows.Compile(cols))
			reader := newRowReader(cols, chain, lease)
			row, err := decodeRow(plan, reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := finishRowReader(reader); err != nil {
				t.Fatal(err)
			}
			if !row.Columns().Equal(cols) || row.Columns().Len() != 3 {
				t.Fatal("physical schema was flattened or replaced")
			}
			if _, err := row.Get("same"); err == nil {
				t.Fatal("ambiguous name selected a column")
			}
			if _, err := row.Get("missing"); err == nil {
				t.Fatal("missing name selected a column")
			}
			if _, err := row.At(-1); err == nil {
				t.Fatal("negative ordinal accepted")
			}
			if _, err := row.At(3); err == nil {
				t.Fatal("out-of-bounds ordinal accepted")
			}
			first := dynamicTestMust(row.At(0))
			if n, e := first.Signed(); e != nil || n != 7 {
				t.Fatal("first duplicate ordinal changed", e)
			}
			second := dynamicTestMust(row.At(1))
			if s, e := second.Text(); e != nil || s.String() != "hello" {
				t.Fatal("second duplicate ordinal changed", e)
			}
			composite := dynamicTestMust(row.Get("pair"))
			if !composite.Type().Equal(tuple) {
				t.Fatal("tuple lost physical type")
			}
			fields := dynamicTestDetach(composite.Elements(), cloneValue)
			if len(fields) != 2 {
				t.Fatal("tuple members missing")
			}
			detached := row.Clone()
			detachedTuple := composite.Clone()
			iterator := dynamicTestMust(composite.iterElements())
			lease.Release()
			if _, err := row.At(0); !errors.Is(err, ErrReleased) {
				t.Fatalf("row access after release: %v", err)
			}
			if _, err := second.Text(); !errors.Is(err, ErrReleased) {
				t.Fatalf("text access after release: %v", err)
			}
			// Binding skips revalidating decoded text but still checks its page.
			if _, err := encodeValueBorrowed(second.Type(), DynamicCodec, second); !errors.Is(err, ErrReleased) {
				t.Fatalf("text bind after release: %v", err)
			}
			if iterator.Next() || !errors.Is(iterator.Err(), ErrReleased) {
				t.Fatalf("iterator after release: %v", iterator.Err())
			}
			foundationPanicReleased(t, func() { row.Clone() })
			foundationPanicReleased(t, func() { composite.Clone() })
			copied := dynamicTestMust(detached.At(1))
			s := dynamicTestMust(copied.Text())
			if s.String() != "hello" {
				t.Fatal("row Clone did not detach")
			}
			values := dynamicTestDetach(detachedTuple.Elements(), cloneValue)
			if n, _ := values[0].Signed(); n != 8 {
				t.Fatal("aggregate Clone did not detach")
			}
			// A row validates all physical cell framing, while scalar content stays lazy.
			boolean := dynamicTestType(KindBoolean)
			badCols := NewColumns(ColumnSpec{Name: "b", Type: boolean})
			badPlan := dynamicTestMust(DynamicRows.Compile(badCols))
			badRow, err := decodeRow(badPlan, newRowReader(badCols, FoundationChain(t, []byte{0, 0, 0, 1, 2}), nil))
			if err != nil {
				t.Fatal("valid cell framing rejected before access", err)
			}
			if _, err := badRow.At(0); err == nil {
				t.Fatal("malformed scalar published on access")
			}
			if _, err := decodeRow(badPlan, newRowReader(badCols, FoundationChain(t, []byte{0, 0, 0, 2, 1}), nil)); err == nil {
				t.Fatal("truncated physical row accepted")
			}
			if _, err := decodeRow(plan, newRowReader(badCols, chain, nil)); err == nil {
				t.Fatal("stale row metadata accepted")
			}
		})
	})
	t.Run("Value", func(t *testing.T) {
		t.Run("AggregateWireAndMetadata", func(t *testing.T) {
			integer, text := dynamicTestType(KindInt), dynamicTestType(KindText)
			mapping := dynamicTestMust(MapType(text, integer))
			tuple := dynamicTestMust(TupleType(text, integer))
			udt := dynamicTestMust(UDTType("ks", "record", ColumnSpec{Name: "name", Type: text}, ColumnSpec{Name: "number", Type: integer}))
			fixed := dynamicTestMust(VectorType(integer, 2))
			pair := dynamicTestMust(VectorValue(integer, IntValue(1), IntValue(2)))
			tests := []struct {
				name  string
				value Value
				wire  string
			}{
				{"list", dynamicTestMust(ListValue(integer, IntValue(1), IntValue(2))), "0000000200000004000000010000000400000002"},
				{"set", dynamicTestMust(SetValue(text, TextValue("a"), TextValue(""))), "00000002000000016100000000"},
				{"map", dynamicTestMust(MapValue(mapping, []ValueEntry{{Key: TextValue("k"), Value: IntValue(1)}})), "00000001000000016b0000000400000001"},
				{"tuple", dynamicTestMust(TupleValue(tuple, []Value{TextValue("a"), IntValue(1)})), "00000001610000000400000001"},
				{"UDT", dynamicTestMust(UDTValue(udt, []ValueField{{Name: "number", Value: IntValue(1)}, {Name: "name", Value: TextValue("a")}})), "00000001610000000400000001"},
				{"fixed vector", pair, "0000000100000002"},
				{"variable vector", dynamicTestMust(VectorValue(text, TextValue("a"), TextValue(""))), "016100"},
				{"nested vector", dynamicTestMust(VectorValue(fixed, pair, pair)), "00000001000000020000000100000002"},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					got := dynamicTestRoundTrip(t, test.value, test.wire)
					switch got.Type().Kind() {
					case KindMap:
						values, err := dynamicTestCollect(got.Entries(), cloneEntry)
						if err != nil || len(values) != 1 {
							t.Fatalf("map entries %d: %v", len(values), err)
						}
						k, _ := values[0].Key.Text()
						n, _ := values[0].Value.Signed()
						if k.String() != "k" || n != 1 {
							t.Fatal("map values changed")
						}
					case KindUDT:
						fields, err := dynamicTestCollect(got.Fields(), cloneField)
						if err != nil || len(fields) != 2 || fields[0].Name != "name" || fields[1].Name != "number" {
							t.Fatal("UDT order or names changed", err)
						}
					default:
						values, err := dynamicTestCollect(got.Elements(), cloneValue)
						if err != nil || len(values) != 2 {
							t.Fatalf("elements %d: %v", len(values), err)
						}
					}
				})
			}
			if _, err := ListValue(integer, TextValue("x")); err == nil {
				t.Fatal("wrong list child type accepted")
			}
			if _, err := TupleValue(tuple, []Value{TextValue("x")}); err == nil {
				t.Fatal("short tuple accepted")
			}
			if _, err := VectorValue(integer); err == nil {
				t.Fatal("empty vector accepted")
			}
			if v := dynamicTestMust(VectorValue(integer, IntValue(1))); !v.Type().Equal(dynamicTestMust(VectorType(integer, 1))) {
				t.Fatal("vector dimension is not the value count")
			}
			if _, err := UDTValue(udt, []ValueField{{Name: "name", Value: TextValue("a")}, {Name: "name", Value: TextValue("b")}}); err == nil {
				t.Fatal("duplicate UDT input name accepted")
			}
			if _, err := UDTValue(udt, []ValueField{{Name: "name", Value: TextValue("a")}, {Name: "unknown", Value: IntValue(1)}}); err == nil {
				t.Fatal("unknown UDT field accepted")
			}
			other := dynamicTestMust(UDTType("ks", "other", ColumnSpec{Name: "name", Type: text}, ColumnSpec{Name: "number", Type: integer}))
			value := tests[4].value
			if _, err := encodeValueBorrowed(other, DynamicCodec, value); err == nil {
				t.Fatal("wrong UDT identity accepted")
			}
			if _, err := encodeValueBorrowed(dynamicTestMust(ListType(text)), DynamicCodec, tests[1].value); err == nil {
				t.Fatal("set variant encoded as list")
			}
		})
		t.Run("BorrowingAndOwnedClone", func(t *testing.T) {
			text := dynamicTestType(KindText)
			list := dynamicTestMust(ListType(text))
			body := dynamicTestHex("0000000200000003e282ac0000000178")
			parts := make([]wire.Span, len(body))
			for i := range body {
				parts[i] = wire.Borrow(body[i : i+1])
			}
			chain := dynamicTestMust(wire.NewChain(parts...))
			lease := wire.NewLease(chain, nil)
			value, err := DynamicCodec.Decode(valueReaderFromLease(list, Present, chain, lease))
			if err != nil {
				t.Fatal(err)
			}
			iterator := dynamicTestMust(value.iterElements())
			var values []Value
			for iterator.Next() {
				values = append(values, iterator.Value())
			}
			if err := iterator.Err(); err != nil {
				t.Fatal(err)
			}
			collected := dynamicTestDetach(value.Elements(), cloneValue)
			euro := dynamicTestMust(values[0].Text())
			if euro.String() != "€" {
				t.Fatal("UTF-8 split across segments changed")
			}
			// The byte view still points directly into the three original fragments.
			count := 0
			err = eachBytes(euro.Bytes(), func(span wire.Span) error {
				if &span.Bytes()[0] != &body[8+count] {
					t.Error("cross-segment dynamic text was copied")
				}
				count++
				return nil
			})
			if err != nil || count != 3 {
				t.Fatalf("fragments %d: %v", count, err)
			}
			cloned := value.Clone()
			scalarClone := values[0].Clone()
			lease.Release()
			if text, err := collected[0].Text(); err != nil || text.String() != "€" {
				t.Fatalf("Clone did not detach: %v", err)
			}
			if _, err := value.iterElements(); !errors.Is(err, ErrReleased) {
				t.Fatalf("released aggregate: %v", err)
			}
			if _, err := values[0].Text(); !errors.Is(err, ErrReleased) {
				t.Fatalf("released child: %v", err)
			}
			clonedValues := dynamicTestDetach(cloned.Elements(), cloneValue)
			if s, _ := clonedValues[0].Text(); s.String() != "€" {
				t.Fatal("lazy Clone did not detach")
			}
			if s, _ := scalarClone.Text(); s.String() != "€" {
				t.Fatal("scalar Clone did not detach")
			}
			blob := dynamicTestType(KindBlob)
			blobs := dynamicTestMust(ListType(blob))
			payload := []byte("payload")
			descriptors := []Value{BlobValue(payload)}
			input := dynamicTestMust(ListValue(blob, descriptors...))
			inputClone := input.Clone()
			borrowed := dynamicTestMust(encodeValueBorrowed(blobs, DynamicCodec, input))
			owned := dynamicTestMust(EncodeValue(blobs, DynamicCodec, input))
			payload[0] = 'X'
			descriptors[0] = BlobValue([]byte("replaced"))
			clonedInputValues := dynamicTestDetach(inputClone.Elements(), cloneValue)
			cloneBytes := dynamicTestMust(clonedInputValues[0].Bytes())
			if string(cloneBytes.ownedBytes()) != "payload" {
				t.Fatal("input Clone kept borrowed descriptors or payload")
			}
			ownedValues := dynamicTestDetach(dynamicTestMust(DecodeValue(DynamicCodec, owned)).Elements(), cloneValue)
			if b, _ := ownedValues[0].Bytes(); string(b.ownedBytes()) != "payload" {
				t.Fatal("owned encoding retained caller payload")
			}
			borrowedValues := dynamicTestDetach(dynamicTestMust(DecodeValue(DynamicCodec, borrowed)).Elements(), cloneValue)
			if b, _ := borrowedValues[0].Bytes(); string(b.ownedBytes()) != "Xayload" {
				t.Fatal("borrowed encoding did not retain source payload")
			}
		})
		t.Run("ChecksTypeWithoutPayload", func(t *testing.T) {
			invalid := DataType{}
			for _, value := range []Value{NullValue(invalid), UnsetValue(invalid)} {
				if _, err := encodeValueBorrowed(dynamicTestType(KindInt), DynamicCodec, value); err == nil {
					t.Fatal("invalid descriptor accepted for payload-free value")
				}
			}
			large := dynamicTestMust(VectorType(dynamicTestType(KindDouble), 65535))
			tooWide := dynamicTestMust(VectorType(large, 65535))
			if err := DynamicCodec.CheckType(tooWide); err == nil {
				t.Fatal("vector aggregate width overflow was accepted")
			}
			for _, value := range []Value{NullValue(tooWide), UnsetValue(tooWide)} {
				if _, err := encodeValueBorrowed(tooWide, DynamicCodec, value); err == nil {
					t.Fatal("invalid vector type bypassed by null/unset")
				}
			}
			if _, err := DynamicRows.Compile(NewColumns(ColumnSpec{Name: "overflow", Type: tooWide})); err == nil {
				t.Fatal("invalid nested vector metadata compiled")
			}
			if _, err := DynamicParams.Compile(NewColumns(ColumnSpec{Name: "overflow", Type: tooWide})); err == nil {
				t.Fatal("invalid nested vector parameter metadata compiled")
			}
		})
		t.Run("MalformedCheckedLazily", func(t *testing.T) {
			integer := dynamicTestType(KindInt)
			boolean := dynamicTestType(KindBoolean)
			text := dynamicTestType(KindText)
			list := dynamicTestMust(ListType(integer))
			mapping := dynamicTestMust(MapType(text, integer))
			tuple := dynamicTestMust(TupleType(integer))
			fixed := dynamicTestMust(VectorType(integer, 2))
			variable := dynamicTestMust(VectorType(text, 2))
			tests := []struct {
				typ  DataType
				body string
			}{
				{boolean, "02"}, {integer, "0001"}, {dynamicTestType(KindBigInt), "00"}, {text, "ff"}, {dynamicTestType(KindASCII), "ff"},
				{dynamicTestType(KindTimeUUID), "00112233445566778899aabbccddeeff"}, {dynamicTestType(KindTime), "ffffffffffffffff"},
				{dynamicTestType(KindDecimal), "00000000"}, {dynamicTestType(KindDuration), "020103"}, {dynamicTestType(KindDuration), "00000000"},
				{dynamicTestType(KindCounter), ""}, {dynamicTestType(KindDuration), ""}, {list, ""}, {list, "ffffffff"}, {list, "000000020000000400000001"},
				{list, "00000000ff"}, {list, "00000001fffffffe"}, {list, "00000001ffffffff"}, {mapping, "00000001000000016b"},
				{tuple, "00000003"}, {tuple, "000000040000000100"}, {fixed, "00000001"}, {fixed, "000000010000000200"}, {variable, "0161"}, {variable, "80016100"},
			}
			for _, test := range tests {
				t.Run(test.typ.String()+"/"+test.body, func(t *testing.T) {
					encoded := dynamicTestMust(newEncodedValue(test.typ, Present, BorrowBytes(dynamicTestHex(test.body))))
					if v, err := DecodeValue(DynamicCodec, encoded); err == nil {
						t.Fatalf("malformed body published a value: %s state %d", v.Type(), v.State())
					}
				})
			}
			// Enclosing framing is valid, but child scalar checks happen when requested.
			listBool := dynamicTestMust(ListType(boolean))
			lazy := dynamicTestDecode(t, listBool, dynamicTestHex("0000000200000001010000000102"))
			it := dynamicTestMust(lazy.iterElements())
			if !it.Next() {
				t.Fatal(it.Err())
			}
			if b, e := it.Value().Bool(); e != nil || !b {
				t.Fatal("first valid child failed", e)
			}
			if it.Next() || it.Err() == nil || it.Next() {
				t.Fatal("malformed lazy child did not permanently stop iterator")
			}
			if descriptorOf(it.Value().Type()) != nil {
				t.Fatal("partial malformed value was published")
			}
			if _, err := encodeValueBorrowed(listBool, DynamicCodec, lazy); err == nil {
				t.Fatal("binding forwarded an unvalidated malformed child")
			}
			if values, err := dynamicTestCollect(lazy.Elements(), cloneValue); err == nil || values != nil {
				t.Fatal("Elements published partial malformed aggregate")
			}
		})
		t.Run("ScalarWireAndAccessors", func(t *testing.T) {
			uuid := dynamicTestMust(ParseUUID("00112233-4455-6677-8899-aabbccddeeff"))
			timeuuid := TimeUUID(dynamicTestMust(ParseUUID("00112233-4455-1677-8899-aabbccddeeff")))
			integer := dynamicTestMust(VarintFromBytes([]byte{0xff, 0x7f}))
			tests := []struct {
				name  string
				value Value
				wire  string
			}{
				{"bool", BooleanValue(true), "01"},
				{"tinyint", TinyIntValue(-1), "ff"},
				{"smallint", SmallIntValue(-2), "fffe"},
				{"int", IntValue(-3), "fffffffd"},
				{"bigint", BigIntValue(-4), "fffffffffffffffc"},
				{"float", FloatValue(1.5), "3fc00000"},
				{"double", DoubleValue(-1.5), "bff8000000000000"},
				{"text", TextValue("€"), "e282ac"},
				{"ascii", ASCIIValue("az"), "617a"},
				{"blob", BlobValue([]byte{0, 1}), "0001"},
				{"uuid", UUIDValue(uuid), "00112233445566778899aabbccddeeff"},
				{"timeuuid", TimeUUIDValue(timeuuid), "00112233445516778899aabbccddeeff"},
				{"inet4", InetValue(netip.MustParseAddr("127.0.0.1")), "7f000001"},
				{"inet6", InetValue(netip.MustParseAddr("::1")), "00000000000000000000000000000001"},
				{"date", DateValue(-1), "7fffffff"},
				{"time", TimeValue(42), "000000000000002a"},
				{"timestamp", TimestampValue(-1), "ffffffffffffffff"},
				{"duration", DurationValue(Duration{Months: 1, Days: 2, Nanoseconds: 3}), "020406"},
				{"varint", VarintValue(integer), "ff7f"},
				{"decimal", DecimalValue(Decimal{Scale: -2, Unscaled: integer}), "fffffffeff7f"},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					got := dynamicTestRoundTrip(t, test.value, test.wire)
					switch got.Type().Kind() {
					case KindBoolean:
						a, e := got.Bool()
						b, _ := test.value.Bool()
						if e != nil || a != b {
							t.Fatalf("Bool: %v %v", a, e)
						}
					case KindTinyInt, KindSmallInt, KindInt, KindBigInt, KindCounter, KindDate, KindTime, KindTimestamp:
						a, e := got.Signed()
						b, _ := test.value.Signed()
						if e != nil || a != b {
							t.Fatalf("Signed: %d %v", a, e)
						}
					case KindFloat, KindDouble:
						a, e := got.Float()
						b, _ := test.value.Float()
						if e != nil || a != b {
							t.Fatalf("Float: %f %v", a, e)
						}
					case KindText, KindASCII:
						a, e := got.Text()
						b, _ := test.value.Text()
						if e != nil || a.String() != b.String() {
							t.Fatalf("Text: %v", e)
						}
					case KindBlob:
						a, e := got.Bytes()
						if e != nil || !bytes.Equal(a.ownedBytes(), []byte{0, 1}) {
							t.Fatalf("Bytes: %v", e)
						}
					case KindUUID, KindTimeUUID:
						a, e := got.UUID()
						b, _ := test.value.UUID()
						if e != nil || a != b {
							t.Fatalf("UUID: %v", e)
						}
					case KindInet:
						a, e := got.Inet()
						b, _ := test.value.Inet()
						if e != nil || a != b {
							t.Fatalf("Inet: %v", e)
						}
					case KindDuration:
						a, e := got.Duration()
						b, _ := test.value.Duration()
						if e != nil || a != b {
							t.Fatalf("Duration: %v", e)
						}
					case KindVarint:
						a, e := got.Varint()
						if e != nil || a.BigInt().Int64() != -129 {
							t.Fatalf("Varint: %v", e)
						}
					case KindDecimal:
						a, e := got.Decimal()
						if e != nil || a.Scale != -2 || a.Unscaled.BigInt().Int64() != -129 {
							t.Fatalf("Decimal: %v", e)
						}
					}
				})
			}
			legacy := dynamicTestType(Kind(0x000A))
			encoded, err := encodeValueBorrowed(legacy, DynamicCodec, TextValue("v"))
			if err != nil {
				t.Fatal(err)
			}
			value, err := DecodeValue(DynamicCodec, encoded)
			if err != nil || value.Type().Kind() != KindText {
				t.Fatal("legacy text ID did not decode as text", err)
			}
			if _, err := IntValue(1).Text(); err == nil {
				t.Fatal("wrong accessor accepted")
			}
			if _, err := NullValue(dynamicTestType(KindBoolean)).Bool(); err == nil {
				t.Fatal("null boolean accessor accepted")
			}
			// The constructors do not fail; an invalid payload fails when
			// it is bound, and a text one also when it is read.
			for name, v := range map[string]Value{
				"non-ASCII":            ASCIIValue("é"),
				"invalid UTF-8":        TextValue(string([]byte{0xff})),
				"wrong UUID version":   TimeUUIDValue(TimeUUID(uuid)),
				"invalid inet":         InetValue(netip.Addr{}),
				"time outside one day": TimeValue(TimeOfDay(nanosecondsPerDay)),
				"mixed duration signs": DurationValue(Duration{Months: 1, Days: -1}),
			} {
				if _, err := EncodeValue(v.Type(), DynamicCodec, v); err == nil {
					t.Fatalf("%s accepted at encode", name)
				}
			}
			if _, err := ASCIIValue("é").Text(); err == nil {
				t.Fatal("Text accepted a non-ASCII ascii value")
			}
		})
		t.Run("StatesAndTrailingFields", func(t *testing.T) {
			integer, text := dynamicTestType(KindInt), dynamicTestType(KindText)
			list := dynamicTestMust(ListType(integer))
			for _, typ := range []DataType{integer, text, list} {
				encoded, err := encodeValueBorrowed(typ, DynamicCodec, NullValue(typ))
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := DecodeValue(DynamicCodec, encoded)
				if err != nil || decoded.State() != Null {
					t.Fatal("null state lost", err)
				}
				unset, err := encodeValueBorrowed(typ, DynamicCodec, UnsetValue(typ))
				if err != nil || unset.State() != Unset {
					t.Fatal("standalone unset failed", err)
				}
				if _, err := DecodeValue(DynamicCodec, unset); err == nil {
					t.Fatal("result unset accepted")
				}
			}
			for _, kind := range []Kind{KindBoolean, KindTinyInt, KindSmallInt, KindInt, KindBigInt, KindFloat, KindDouble, KindDate, KindTime, KindTimestamp, KindUUID, KindTimeUUID, KindInet, KindVarint, KindDecimal} {
				typ := dynamicTestType(kind)
				empty := dynamicTestMust(EmptyValue(typ))
				dynamicTestRoundTrip(t, empty, "")
				got := dynamicTestDecode(t, typ, nil)
				if got.State() != EmptyCell {
					t.Fatalf("%s wire empty lost", typ)
				}
			}
			for _, kind := range []Kind{KindCounter, KindDuration, KindASCII, KindText, KindBlob} {
				if _, err := EmptyValue(dynamicTestType(kind)); err == nil {
					t.Fatalf("unsupported Empty for %s accepted", kind)
				}
			}
			for _, value := range []Value{TextValue(""), BlobValue(nil), ASCIIValue("")} {
				dynamicTestRoundTrip(t, value, "")
			}
			dynamicTestRoundTrip(t, dynamicTestMust(ListValue(integer)), "00000000")
			if _, err := ListValue(integer, UnsetValue(integer)); err == nil {
				t.Fatal("nested unset accepted")
			}
			if _, err := ListValue(integer, NullValue(integer)); err == nil {
				t.Fatal("null collection element accepted")
			}
			tuple := dynamicTestMust(TupleType(integer, text))
			truncated := dynamicTestDecode(t, tuple, dynamicTestHex("0000000400000001"))
			values, err := dynamicTestCollect(truncated.Elements(), cloneValue)
			if err != nil || len(values) != 2 || values[1].State() != Null || !values[1].Type().Equal(text) {
				t.Fatal("missing trailing tuple member did not retain typed null", err)
			}
			udt := dynamicTestMust(UDTType("ks", "record", ColumnSpec{Name: "x", Type: integer}, ColumnSpec{Name: "y", Type: text}))
			omitted := dynamicTestDecode(t, udt, nil)
			fields, err := dynamicTestCollect(omitted.Fields(), cloneField)
			if err != nil || len(fields) != 2 || fields[0].Value.State() != Null || fields[1].Value.State() != Null {
				t.Fatal("omitted UDT trailing fields", err)
			}
			if _, err := TupleValue(tuple, []Value{IntValue(1), UnsetValue(text)}); err == nil {
				t.Fatal("unset tuple member accepted")
			}
		})
	})
}

// TestDynamicCodecContracts runs the Codec methods of DynamicCodec and of the
// plans it compiles that the encode and decode paths skip: a caller's own
// codec that wraps DynamicCodec reaches Encode, and Clone and CheckType are
// part of every Decoder and Encoder.
func TestDynamicCodecContracts(t *testing.T) {
	blob, integer := dynamicTestType(KindBlob), dynamicTestType(KindInt)
	t.Run("WrappedEncode", func(t *testing.T) {
		// The wrapper hides DynamicCodec's compiled form, so EncodeValue
		// checks the type and calls Encode on every value.
		wrapped := struct{ Codec[Value] }{DynamicCodec}
		encoded := dynamicTestMust(EncodeValue(integer, wrapped, IntValue(7)))
		if got := hex.EncodeToString(encoded.Bytes().ownedBytes()); got != "00000007" {
			t.Fatalf("wire body %s", got)
		}
		if decoded := dynamicTestMust(DecodeValue(wrapped, encoded)); dynamicTestMust(decoded.Signed()) != 7 {
			t.Fatalf("decoded %v", decoded)
		}
		if _, err := EncodeValue(integer, wrapped, BlobValue([]byte("x"))); err == nil {
			t.Fatal("Encode accepted a blob value for an int cell")
		}
		if err := DynamicCodec.Encode(nil, IntValue(7)); err == nil {
			t.Fatal("Encode accepted a nil writer")
		}
	})
	t.Run("Clone", func(t *testing.T) {
		body := []byte("payload")
		value := dynamicTestDecode(t, blob, body)
		cloned := DynamicCodec.Clone(value)
		decoder := dynamicTestMust(compileValueDecoder(DynamicCodec, blob))
		compiledClone := decoder.Clone(value)
		body[0] = 'P'
		if got := dynamicTestMust(value.Bytes()).ownedBytes(); string(got) != "Payload" {
			t.Fatalf("decoded value %q does not borrow the body", got)
		}
		for name, v := range map[string]Value{"Codec": cloned, "Compiled": compiledClone} {
			if got := dynamicTestMust(v.Bytes()).ownedBytes(); string(got) != "payload" {
				t.Errorf("%s Clone = %q, want a copy taken before the body changed", name, got)
			}
		}
	})
	t.Run("CompiledCheckType", func(t *testing.T) {
		decoder := dynamicTestMust(compileValueDecoder(DynamicCodec, integer))
		encoder := dynamicTestMust(compileValueEncoder(DynamicCodec, integer))
		for name, check := range map[string]func(DataType) error{"Decoder": decoder.CheckType, "Encoder": encoder.CheckType} {
			if err := check(integer); err != nil {
				t.Errorf("%s CheckType(int) = %v", name, err)
			}
			if err := check(blob); err == nil {
				t.Errorf("%s CheckType(blob) accepted a type other than the compiled one", name)
			}
		}
	})
}
