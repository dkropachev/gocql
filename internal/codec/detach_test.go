// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestDetachRow(t *testing.T) {
	t.Run("MixedOwnedAndNestedBorrowed", func(t *testing.T) {
		type row struct {
			View   cqltype.Bytes
			Owned  []byte
			Nested []cqltype.Text
		}
		list := CodecTestMust(cqltype.ListType(cqltype.TextType))
		columns := cqltype.NewColumns(
			cqltype.ColumnSpec{Name: "view", Type: cqltype.BlobType},
			cqltype.ColumnSpec{Name: "owned", Type: cqltype.BlobType},
			cqltype.ColumnSpec{Name: "nested", Type: list},
		)
		text := CodecTestMust(cqltype.BorrowText("nested"))
		input := row{View: cqltype.BorrowBytes([]byte("view")), Owned: []byte("owned"), Nested: []cqltype.Text{text}}
		encoder := Params(
			Param("view", BlobCodec, func(r row) cqltype.Bytes { return r.View }),
			Param("owned", BytesEncoder, func(r row) []byte { return r.Owned }),
			Param("nested", ListEncoder(TextCodec), func(r row) []cqltype.Text { return r.Nested }),
		)
		body := encodeTestParams(t, columns, encoder, input)
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		plan := CodecTestMust(RowInto(func(*row) {},
			Column("view", BlobCodec, func(r *row) *cqltype.Bytes { return &r.View }),
			Column("owned", OwnedBlobCodec, func(r *row) *[]byte { return &r.Owned }),
			Column("nested", ListCodec(TextCodec), func(r *row) *[]cqltype.Text { return &r.Nested }),
		).Compile(columns))
		dst := row{Owned: make([]byte, 0, 64), Nested: make([]cqltype.Text, 1)}
		ownedBacking, nestedBacking := &dst.Owned[:cap(dst.Owned)][0], &dst.Nested[0]
		if err := plan.DecodeInto(NewRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		borrowed := dst.View
		DetachRow(plan, &dst)
		lease.Release()
		if string(dst.View.OwnedBytes()) != "view" || string(dst.Owned) != "owned" || dst.Nested[0].String() != "nested" {
			t.Fatalf("detached row: %+v", dst)
		}
		if &dst.Owned[0] != ownedBacking || &dst.Nested[0] != nestedBacking {
			t.Fatal("detaching copied an already-owned destination buffer")
		}
		if _, err := borrowed.At(0); !errors.Is(err, cqltype.ErrReleased) {
			t.Fatalf("original view is still alive: %v", err)
		}
	})
	t.Run("CustomCloneFallback", func(t *testing.T) {
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.BlobType})
		body := encodeTestParams(t, columns, OneParam(BytesEncoder), []byte("custom"))
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		base := CodecTestMust(OneColumn(BlobCodec).Compile(columns))
		calls := 0
		plan := cloneCountingPlan{RowPlan: base, calls: &calls}
		var dst cqltype.Bytes
		if err := plan.DecodeInto(NewRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		DetachRow[cqltype.Bytes](plan, &dst)
		lease.Release()
		if calls != 1 || string(dst.OwnedBytes()) != "custom" {
			t.Fatalf("clone calls %d, value %q", calls, dst.OwnedBytes())
		}
	})
	t.Run("BorrowedNumericScalars", func(t *testing.T) {
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.VarintType})
		input := CodecTestMust(VarintFromBytes([]byte{42}))
		body := encodeTestParams(t, columns, OneParam(VarintCodec), input)
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		plan := CodecTestMust(OneColumn(VarintCodec).Compile(columns))
		var dst Varint
		if err := plan.DecodeInto(NewRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		DetachRow(plan, &dst)
		lease.Release()
		if dst.BigInt().Int64() != 42 {
			t.Fatal("varint expired")
		}
	})

	t.Run("DynamicRow", func(t *testing.T) {
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.BlobType})
		body := encodeTestParams(t, columns, OneParam(BytesEncoder), []byte("dynamic"))
		lease := wire.NewLease(body, nil)
		defer lease.Release()
		plan := CodecTestMust(DynamicRows.Compile(columns))
		var dst DynamicRow
		if err := plan.DecodeInto(NewRowReader(columns, body, lease), &dst); err != nil {
			t.Fatal(err)
		}
		DetachRow(plan, &dst)
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
		Name    cqltype.Nullable[cqltype.Text]
		Amount  Decimal
		Mapping []Entry[cqltype.Text, cqltype.Bytes]
		Record  Pair[cqltype.Text, cqltype.Bytes]
	}
	mapping := CodecTestMust(cqltype.MapType(cqltype.TextType, cqltype.BlobType))
	record := CodecTestMust(cqltype.TupleType(cqltype.TextType, cqltype.BlobType))
	columns := cqltype.NewColumns(
		cqltype.ColumnSpec{Name: "name", Type: cqltype.TextType},
		cqltype.ColumnSpec{Name: "amount", Type: cqltype.DecimalType},
		cqltype.ColumnSpec{Name: "mapping", Type: mapping},
		cqltype.ColumnSpec{Name: "record", Type: record},
	)
	text := CodecTestMust(cqltype.BorrowText("name"))
	blob := cqltype.BorrowBytes([]byte("payload"))
	pairParams := PairParams(1, OneParam(TextCodec), OneParam(BlobCodec))
	pairRows := PairRows(1, OneColumn(TextCodec), OneColumn(BlobCodec))
	input := row{Name: cqltype.Nullable[cqltype.Text]{Valid: true, Value: text}, Amount: Decimal{Scale: 2, Unscaled: CodecTestMust(VarintFromBytes([]byte{42}))}, Mapping: []Entry[cqltype.Text, cqltype.Bytes]{{Key: text, Value: blob}}, Record: Pair[cqltype.Text, cqltype.Bytes]{First: text, Second: blob}}
	params := Params(
		Param("name", NullableEncoder(TextCodec), func(r row) cqltype.Nullable[cqltype.Text] { return r.Name }),
		Param("amount", DecimalCodec, func(r row) Decimal { return r.Amount }),
		Param("mapping", MapEncoder(TextCodec, BlobCodec), func(r row) []Entry[cqltype.Text, cqltype.Bytes] { return r.Mapping }),
		Param("record", TupleEncoder(pairParams), func(r row) Pair[cqltype.Text, cqltype.Bytes] { return r.Record }),
	)
	decoder := Row(
		Column("name", NullableCodec(TextCodec), func(r *row) *cqltype.Nullable[cqltype.Text] { return &r.Name }),
		Column("amount", DecimalCodec, func(r *row) *Decimal { return &r.Amount }),
		Column("mapping", MapCodec(TextCodec, BlobCodec), func(r *row) *[]Entry[cqltype.Text, cqltype.Bytes] { return &r.Mapping }),
		Column("record", TupleDecoder(pairRows), func(r *row) *Pair[cqltype.Text, cqltype.Bytes] { return &r.Record }),
	)
	body := encodeTestParams(t, columns, params, input)
	lease := wire.NewLease(body, nil)
	defer lease.Release()
	plan := CodecTestMust(decoder.Compile(columns))
	var dst row
	if err := plan.DecodeInto(NewRowReader(columns, body, lease), &dst); err != nil {
		t.Fatal(err)
	}
	DetachRow(plan, &dst)
	lease.Release()
	if !dst.Name.Valid || dst.Name.Value.String() != "name" || dst.Amount.Unscaled.BigInt().Int64() != 42 || dst.Amount.Scale != 2 ||
		dst.Mapping[0].Key.String() != "name" || string(dst.Mapping[0].Value.OwnedBytes()) != "payload" ||
		dst.Record.First.String() != "name" || string(dst.Record.Second.OwnedBytes()) != "payload" {
		t.Fatal("nested result changed or expired")
	}
}

type cloneCountingPlan struct {
	RowPlan[cqltype.Bytes]
	calls *int
}

func (p cloneCountingPlan) Clone(v cqltype.Bytes) cqltype.Bytes { *p.calls++; return v.Clone() }
