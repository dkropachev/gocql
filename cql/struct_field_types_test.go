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

type fieldStatuses []fieldStatus
type fieldID cql.UUID

// namedFieldTypes holds named types inside slices, pointers, Nullable,
// BindValue and maps, which map like the types they are defined from.
type namedFieldTypes struct {
	List    []fieldStatus
	Set     []fieldStatus `cql:"set,codec=SetCodec"`
	Named   fieldStatuses
	Ptr     *fieldStatus
	Nil     *fieldStatus
	Maybe   cql.Nullable[fieldStatus]
	Counts  map[fieldStatus]fieldCount
	ByCount map[fieldCount]fieldStatus
}

func TestStructNamedElementTypes(t *testing.T) {
	must := func(typ cql.DataType, err error) cql.DataType {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return typ
	}
	columns := cql.NewColumns(
		cql.ColumnSpec{Name: "list", Type: must(cql.ListType(cql.ASCIIType))},
		cql.ColumnSpec{Name: "set", Type: must(cql.SetType(cql.TextType))},
		cql.ColumnSpec{Name: "named", Type: must(cql.VectorType(cql.TextType, 2))},
		cql.ColumnSpec{Name: "ptr", Type: cql.TextType},
		cql.ColumnSpec{Name: "nil", Type: cql.TextType},
		cql.ColumnSpec{Name: "maybe", Type: cql.TextType},
		cql.ColumnSpec{Name: "counts", Type: must(cql.MapType(cql.TextType, cql.BigIntType))},
		cql.ColumnSpec{Name: "by_count", Type: must(cql.MapType(cql.BigIntType, cql.TextType))},
	)
	ok := fieldStatus("ok")
	value := namedFieldTypes{
		List: []fieldStatus{"a", "b"}, Set: []fieldStatus{"s"}, Named: fieldStatuses{"x", "y"},
		Ptr: &ok, Maybe: cql.Nullable[fieldStatus]{Value: "m", Valid: true},
		Counts: map[fieldStatus]fieldCount{"c": 3}, ByCount: map[fieldCount]fieldStatus{4: "d"},
	}
	row := ruleDecode(t, columns, cql.MustRowOf[namedFieldTypes](), ruleEncode(t, columns, cql.MustParamsOf[namedFieldTypes](), value))
	if len(row.List) != 2 || row.List[1] != "b" || len(row.Set) != 1 || row.Set[0] != "s" || len(row.Named) != 2 || row.Named[1] != "y" {
		t.Fatalf("slices: %+v", row)
	}
	if row.Ptr == nil || *row.Ptr != "ok" || row.Nil != nil || row.Maybe != value.Maybe {
		t.Fatalf("pointer and nullable: %+v", row)
	}
	if len(row.Counts) != 1 || row.Counts["c"] != 3 || len(row.ByCount) != 1 || row.ByCount[4] != "d" {
		t.Fatalf("maps: %v %v", row.Counts, row.ByCount)
	}

	// Elements are checked like those of the underlying type: ascii rejects
	// non-ASCII, and codec=SetCodec requires a set.
	plan, err := cql.MustParamsOf[namedFieldTypes]().Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	bad := value
	bad.List = []fieldStatus{"é"}
	if _, err := cql.EncodeParamsForTest(columns, plan, bad, true); err == nil {
		t.Fatal("encoded non-ASCII into a list of ascii")
	}
	type setOnly struct {
		Set []fieldStatus `cql:"set,codec=SetCodec"`
	}
	list := cql.NewColumns(cql.ColumnSpec{Name: "set", Type: must(cql.ListType(cql.TextType))})
	if _, err := cql.MustRowOf[setOnly]().Compile(list); err == nil {
		t.Fatal("codec=SetCodec compiled for a list")
	}

	// A BindValue of a named type binds parameters.
	type bound struct{ V cql.BindValue[fieldStatus] }
	one := cql.NewColumns(cql.ColumnSpec{Name: "v", Type: cql.TextType})
	if _, err := cql.ParamsOf[bound](); err != nil {
		t.Fatal(err)
	}
	ruleEncode(t, one, cql.MustParamsOf[bound](), bound{V: cql.BindValue[fieldStatus]{Value: "v", State: cql.Present}})

	// WithCodec for the named type replaces its slices, as it does the type.
	upper := cql.WithCodec(cql.ConvertCodec(cql.StringCodec,
		func(v fieldStatus) string { return string(v) + "!" }, func(v string) fieldStatus { return fieldStatus(v) }))
	type listOnly struct{ List []fieldStatus }
	lists := cql.NewColumns(cql.ColumnSpec{Name: "list", Type: must(cql.ListType(cql.TextType))})
	got := ruleDecode(t, lists, cql.MustRowOf[listOnly](), ruleEncode(t, lists, cql.MustParamsOf[listOnly](upper), listOnly{List: []fieldStatus{"a"}}))
	if len(got.List) != 1 || got.List[0] != "a!" {
		t.Fatalf("WithCodec: %v", got.List)
	}

	// A type defined from a named type other than a predeclared one maps
	// like neither, also as an element.
	type ids struct{ IDs []fieldID }
	if _, err := cql.RowOf[ids](); err == nil {
		t.Fatal("mapped a slice of a type defined from cql.UUID")
	}
}
