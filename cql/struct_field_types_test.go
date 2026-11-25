// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"math/big"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

type fieldStatus string
type fieldTags []string
type fieldCount int

// fieldTypes has fields of the types struct mappings map without a tag
// beyond those of a built-in codec.
type fieldTypes struct {
	Set     []int32
	Vector  []float32
	List    []string
	Status  fieldStatus
	Tags    fieldTags
	Count   fieldCount
	N       int
	Name    *string
	Missing *string
	Big     *big.Int
	Text    cql.Text
	Ints    []int
	Counts  map[string]int
	ByN     map[int]string
}

func TestStructFieldTypes(t *testing.T) {
	must := func(typ cql.DataType, err error) cql.DataType {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return typ
	}
	columns := cql.NewColumns(
		cql.ColumnSpec{Name: "set", Type: must(cql.SetType(cql.IntType))},
		cql.ColumnSpec{Name: "vector", Type: must(cql.VectorType(cql.FloatType, 2))},
		cql.ColumnSpec{Name: "list", Type: must(cql.ListType(cql.TextType))},
		cql.ColumnSpec{Name: "status", Type: cql.ASCIIType},
		cql.ColumnSpec{Name: "tags", Type: must(cql.SetType(cql.TextType))},
		cql.ColumnSpec{Name: "count", Type: cql.BigIntType},
		cql.ColumnSpec{Name: "n", Type: cql.CounterType},
		cql.ColumnSpec{Name: "name", Type: cql.TextType},
		cql.ColumnSpec{Name: "missing", Type: cql.TextType},
		cql.ColumnSpec{Name: "big", Type: cql.VarintType},
		cql.ColumnSpec{Name: "text", Type: cql.TextType},
		cql.ColumnSpec{Name: "ints", Type: must(cql.ListType(cql.BigIntType))},
		cql.ColumnSpec{Name: "counts", Type: must(cql.MapType(cql.TextType, cql.BigIntType))},
		cql.ColumnSpec{Name: "by_n", Type: must(cql.MapType(cql.BigIntType, cql.TextType))},
	)
	name := "n"
	big := new(big.Int).Lsh(big.NewInt(1), 80)
	value := fieldTypes{
		Set: []int32{1, 2}, Vector: []float32{1.5, 2.5}, List: []string{"a"},
		Status: "ok", Tags: fieldTags{"x", "y"}, Count: 7, N: -3,
		Name: &name, Big: big, Text: mustBorrowText(t, "t"),
		Ints: []int{4, -5}, Counts: map[string]int{"a": 1}, ByN: map[int]string{-2: "b"},
	}
	row := ruleDecode(t, columns, cql.MustRowOf[fieldTypes](), ruleEncode(t, columns, cql.MustParamsOf[fieldTypes](), value))
	if len(row.Set) != 2 || row.Set[1] != 2 || len(row.Vector) != 2 || row.Vector[1] != 2.5 || len(row.List) != 1 {
		t.Fatalf("slices: %+v", row)
	}
	if row.Status != "ok" || len(row.Tags) != 2 || row.Tags[1] != "y" || row.Count != 7 || row.N != -3 {
		t.Fatalf("named and int fields: %+v", row)
	}
	if row.Name == nil || *row.Name != "n" || row.Missing != nil {
		t.Fatalf("pointer fields: %v %v", row.Name, row.Missing)
	}
	if row.Big == nil || row.Big.Cmp(big) != 0 {
		t.Fatalf("big: %v", row.Big)
	}
	if s, err := row.Text.Copy(); err != nil || s != "t" {
		t.Fatalf("text: %q %v", s, err)
	}
	// int maps bigint as a list element and as a map key or value too.
	if len(row.Ints) != 2 || row.Ints[1] != -5 || len(row.Counts) != 1 || row.Counts["a"] != 1 || len(row.ByN) != 1 || row.ByN[-2] != "b" {
		t.Fatalf("int collections: %v %v %v", row.Ints, row.Counts, row.ByN)
	}

	// A named type's value is checked like its underlying type's: ascii
	// rejects non-ASCII.
	bad := value
	bad.Status = "é"
	plan, err := cql.MustParamsOf[fieldTypes]().Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cql.EncodeParamsForTest(columns, plan, bad, true); err == nil {
		t.Fatal("encoded non-ASCII into ascii")
	}
	// A slice field takes the column's kind, so a map column is an error.
	wrong := cql.NewColumns(cql.ColumnSpec{Name: "set", Type: must(cql.MapType(cql.IntType, cql.IntType))})
	type onlySet struct{ Set []int32 }
	if _, err := cql.MustRowOf[onlySet]().Compile(wrong); err == nil {
		t.Fatal("a slice field compiled for a map column")
	}
}

func mustBorrowText(t *testing.T, s string) cql.Text {
	t.Helper()
	text, err := cql.BorrowText(s)
	if err != nil {
		t.Fatal(err)
	}
	return text
}
