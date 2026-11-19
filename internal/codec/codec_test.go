// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// TestParamCellLayout proves CaptureParamCells may hand cql a []ParamCell as
// its own cell type.
func TestParamCellLayout(t *testing.T) {
	pub, rep := paramCellType(), reflect.TypeFor[ParamCell]()
	if pub.Size() != rep.Size() || pub.Align() != rep.Align() || pub.NumField() != rep.NumField() {
		t.Fatalf("cql cell %v (size %d) and ParamCell (size %d) differ", pub, pub.Size(), rep.Size())
	}
	for i := range pub.NumField() {
		p, r := pub.Field(i), rep.Field(i)
		if p.Offset != r.Offset || p.Type != r.Type || p.Name != r.Name {
			t.Fatalf("field %d: cql has %s %v at %d, ParamCell has %s %v at %d", i, p.Name, p.Type, p.Offset, r.Name, r.Type, r.Offset)
		}
	}
}

// TestDetachRowBuiltinPlans checks that DetachRow's link reaches each
// built-in plan's detachRowAt with the destination type it asserts, and that
// another plan falls back to Clone.
func TestDetachRowBuiltinPlans(t *testing.T) {
	one := cql.NewColumns(cql.ColumnSpec{Name: "a", Type: cql.TextType})
	two := cql.NewColumns(cql.ColumnSpec{Name: "a", Type: cql.TextType}, cql.ColumnSpec{Name: "b", Type: cql.IntType})
	type row struct{ A cql.Text }

	structPlan, err := cql.RowInto(func(*row) {}, cql.Column("a", cql.TextCodec, func(r *row) *cql.Text { return &r.A })).Compile(one)
	if err != nil {
		t.Fatal(err)
	}
	single, err := cql.OneColumn(cql.TextCodec).Compile(one)
	if err != nil {
		t.Fatal(err)
	}
	paired, err := cql.PairRows(cql.OneColumn(cql.TextCodec), cql.OneColumn(cql.Int32Codec)).Compile(two)
	if err != nil {
		t.Fatal(err)
	}
	converted, err := cql.ConvertRows(cql.OneColumn(cql.TextCodec), func(v cql.Text) string { return v.String() }, func(s string) string { return s }).Compile(one)
	if err != nil {
		t.Fatal(err)
	}

	var r row
	var text cql.Text
	var pair cql.Pair[cql.Text, int32]
	var s string
	for name, got := range map[string]bool{
		"struct": detachRow(structPlan, &r),
		"single": detachRow(single, &text),
		"paired": detachRow(paired, &pair),
	} {
		if !got {
			t.Errorf("%s plan: DetachRow fell back to Clone", name)
		}
	}
	if detachRow(converted, &s) {
		t.Error("converted plan: detached as a built-in plan")
	}
}
