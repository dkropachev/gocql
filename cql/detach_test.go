// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestDetachRow(t *testing.T) {
	t.Run("MixedOwnedAndNestedBorrowed", func(t *testing.T) {
		type row struct {
			View   Bytes
			Owned  []byte
			Nested []Text
		}
		list := CodecTestMust(ListType(TextType))
		columns := NewColumns(
			ColumnSpec{Name: "view", Type: BlobType},
			ColumnSpec{Name: "owned", Type: BlobType},
			ColumnSpec{Name: "nested", Type: list},
		)
		text := CodecTestMust(BorrowText("nested"))
		input := row{View: BorrowBytes([]byte("view")), Owned: []byte("owned"), Nested: []Text{text}}
		encoder := Params(
			Param("view", BlobCodec, func(r row) Bytes { return r.View }),
			Param("owned", ByteSliceCodec, func(r row) []byte { return r.Owned }),
			Param("nested", ListEncoder(TextCodec), func(r row) []Text { return r.Nested }),
		)
		body := encodeTestParams(t, columns, encoder, input)
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		plan := CodecTestMust(RowInto(func(*row) {},
			Column("view", BlobCodec, func(r *row) *Bytes { return &r.View }),
			Column("owned", ByteSliceCodec, func(r *row) *[]byte { return &r.Owned }),
			Column("nested", ListCodec(TextCodec), func(r *row) *[]Text { return &r.Nested }),
		).Compile(columns))
		dst := row{Owned: make([]byte, 0, 64), Nested: make([]Text, 1)}
		ownedBacking, nestedBacking := &dst.Owned[:cap(dst.Owned)][0], &dst.Nested[0]
		if err := plan.DecodeInto(newRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		borrowed := dst.View
		detachRowFor(plan, nil, &dst)
		lease.Release()
		if string(dst.View.OwnedBytes()) != "view" || string(dst.Owned) != "owned" || dst.Nested[0].String() != "nested" {
			t.Fatalf("detached row: %+v", dst)
		}
		if &dst.Owned[0] != ownedBacking || &dst.Nested[0] != nestedBacking {
			t.Fatal("detaching copied an already-owned destination buffer")
		}
		if _, err := borrowed.At(0); !errors.Is(err, ErrReleased) {
			t.Fatalf("original view is still alive: %v", err)
		}
	})
	t.Run("CustomCloneFallback", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "v", Type: BlobType})
		body := encodeTestParams(t, columns, OneParam(ByteSliceCodec), []byte("custom"))
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		// The wrapper hides the built-in plan's own detach.
		plan := struct{ CompiledRow[Bytes] }{CodecTestMust(OneColumn(BlobCodec).Compile(columns))}
		calls := 0
		decoder := cloneCountingDecoder{RowDecoder: OneColumn(BlobCodec), calls: &calls}
		var dst Bytes
		if err := plan.DecodeInto(newRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		detachRowFor[Bytes](plan, decoder, &dst)
		lease.Release()
		if calls != 1 || string(dst.OwnedBytes()) != "custom" {
			t.Fatalf("clone calls %d, value %q", calls, dst.OwnedBytes())
		}
	})
	t.Run("BorrowedNumericScalars", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "v", Type: VarintType})
		input := CodecTestMust(VarintFromBytes([]byte{42}))
		body := encodeTestParams(t, columns, OneParam(VarintCodec), input)
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		plan := CodecTestMust(OneColumn(VarintCodec).Compile(columns))
		var dst Varint
		if err := plan.DecodeInto(newRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		detachRowFor(plan, nil, &dst)
		lease.Release()
		if dst.BigInt().Int64() != 42 {
			t.Fatal("varint expired")
		}
	})

	t.Run("DynamicRow", func(t *testing.T) {
		columns := NewColumns(ColumnSpec{Name: "v", Type: BlobType})
		body := encodeTestParams(t, columns, OneParam(ByteSliceCodec), []byte("dynamic"))
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		plan := CodecTestMust(DynamicRows.Compile(columns))
		var dst DynamicRow
		if err := plan.DecodeInto(newRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		detachRowFor(plan, DynamicRows, &dst)
		lease.Release()
		value, err := dst.Get("v")
		if err != nil {
			t.Fatal(err)
		}
		blob, err := value.Bytes()
		if err != nil || string(blob.OwnedBytes()) != "dynamic" {
			t.Fatal(blob, err)
		}
	})
}

func TestDetachNestedValues(t *testing.T) {
	type row struct {
		Name    Nullable[Text]
		Amount  Decimal
		Mapping []Entry[Text, Bytes]
		Record  Pair[Text, Bytes]
	}
	mapping := CodecTestMust(MapType(TextType, BlobType))
	record := CodecTestMust(TupleType(TextType, BlobType))
	columns := NewColumns(
		ColumnSpec{Name: "name", Type: TextType},
		ColumnSpec{Name: "amount", Type: DecimalType},
		ColumnSpec{Name: "mapping", Type: mapping},
		ColumnSpec{Name: "record", Type: record},
	)
	text := CodecTestMust(BorrowText("name"))
	blob := BorrowBytes([]byte("payload"))
	pairParams := PairParams(OneParam(TextCodec), OneParam(BlobCodec))
	pairRows := PairRows(OneColumn(TextCodec), OneColumn(BlobCodec))
	input := row{Name: Nullable[Text]{Valid: true, Value: text}, Amount: Decimal{Scale: 2, Unscaled: CodecTestMust(VarintFromBytes([]byte{42}))}, Mapping: []Entry[Text, Bytes]{{Key: text, Value: blob}}, Record: Pair[Text, Bytes]{First: text, Second: blob}}
	params := Params(
		Param("name", NullableEncoder(TextCodec), func(r row) Nullable[Text] { return r.Name }),
		Param("amount", DecimalCodec, func(r row) Decimal { return r.Amount }),
		Param("mapping", MapEncoder(TextCodec, BlobCodec), func(r row) []Entry[Text, Bytes] { return r.Mapping }),
		Param("record", TupleEncoder(pairParams), func(r row) Pair[Text, Bytes] { return r.Record }),
	)
	decoder := Row(
		Column("name", NullableCodec(TextCodec), func(r *row) *Nullable[Text] { return &r.Name }),
		Column("amount", DecimalCodec, func(r *row) *Decimal { return &r.Amount }),
		Column("mapping", MapCodec(TextCodec, BlobCodec), func(r *row) *[]Entry[Text, Bytes] { return &r.Mapping }),
		Column("record", TupleDecoder(pairRows), func(r *row) *Pair[Text, Bytes] { return &r.Record }),
	)
	body := encodeTestParams(t, columns, params, input)
	lease := wire.NewLease(body, nil)
	defer lease.Release()
	plan := CodecTestMust(decoder.Compile(columns))
	var dst row
	if err := plan.DecodeInto(newRowReader(columns, body, lease), &dst); err != nil {
		t.Fatal(err)
	}
	detachRowFor(plan, nil, &dst)
	lease.Release()
	if !dst.Name.Valid || dst.Name.Value.String() != "name" || dst.Amount.Unscaled.BigInt().Int64() != 42 || dst.Amount.Scale != 2 ||
		dst.Mapping[0].Key.String() != "name" || string(dst.Mapping[0].Value.OwnedBytes()) != "payload" ||
		dst.Record.First.String() != "name" || string(dst.Record.Second.OwnedBytes()) != "payload" {
		t.Fatal("nested result changed or expired")
	}
}

type cloneCountingDecoder struct {
	RowDecoder[Bytes]
	calls *int
}

func (d cloneCountingDecoder) Clone(v Bytes) Bytes { *d.calls++; return v.Clone() }
