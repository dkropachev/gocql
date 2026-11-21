// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

func TestSchemaDataType(t *testing.T) {
	ks := &metadata.Keyspace{Name: "ks", Types: map[string]*metadata.Type{
		"addr":   {Keyspace: "ks", Name: "addr", FieldNames: []string{"street", "zip"}, FieldTypes: []string{"text", "int"}},
		"Quoted": {Keyspace: "ks", Name: "Quoted", FieldNames: []string{"v"}, FieldTypes: []string{"frozen<addr>"}},
	}}
	for source, want := range map[string]string{
		"int":                          "int",
		"varchar":                      "varchar",
		"frozen<list<int>>":            "frozen<list<int>>",
		"map<text, frozen<set<uuid>>>": "map<text,frozen<set<uuid>>>",
		"frozen<tuple<int, text>>":     "frozen<tuple<int,text>>",
		"vector<float, 3>":             "vector<float,3>",
		"frozen<addr>":                 "frozen<ks.addr>",
		"ks.addr":                      "ks.addr",
		`frozen<"Quoted">`:             "frozen<ks.Quoted>",
	} {
		got, err := InternalSchemaDataType(ks, source)
		if err != nil {
			t.Errorf("%s: %v", source, err)
			continue
		}
		if got.String() != want {
			t.Errorf("%s: got %s, want %s", source, got, want)
		}
	}
	addr, _ := InternalSchemaDataType(ks, "addr")
	if addr.Kind() != cql.TypeUDT || addr.Fields().Len() != 2 {
		t.Errorf("addr fields: %v", addr)
	}
	for _, bad := range []string{"", "nosuch", "list<int, int>", "map<int>", "'org.Custom'", "other.addr", "int>", "vector<float, 0>"} {
		if _, err := InternalSchemaDataType(ks, bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

// TestSchemaDataTypeExpansion checks that a chain of UDTs, each with two
// fields of the next, is rejected once it expands past the budget instead of
// doubling the work at every level.
func TestSchemaDataTypeExpansion(t *testing.T) {
	ks := &metadata.Keyspace{Name: "ks", Types: map[string]*metadata.Type{}}
	const levels = 60
	for i := range levels {
		next := "int"
		if i+1 < levels {
			next = fmt.Sprintf("u%d", i+1)
		}
		name := fmt.Sprintf("u%d", i)
		ks.Types[name] = &metadata.Type{Keyspace: "ks", Name: name, FieldNames: []string{"a", "b"}, FieldTypes: []string{next, next}}
	}
	if _, err := InternalSchemaDataType(ks, "u0"); err == nil {
		t.Fatal("a type of 2^60 nodes was built")
	}
	// A chain short enough to stay within the budget still resolves.
	if _, err := InternalSchemaDataType(ks, fmt.Sprintf("u%d", levels-10)); err != nil {
		t.Fatal(err)
	}
}

// FuzzSchemaDataType feeds schema type strings, as system_schema rows hold
// them, to the schema type builder, with UDTs that may refer to each other
// or to themselves. It must not panic or hang.
func FuzzSchemaDataType(f *testing.F) {
	f.Add("frozen<map<text, frozen<list<a>>>>", "int", "frozen<a>")
	f.Add("tuple<int, vector<float, 3>, b>", "frozen<b>, int", "text")
	f.Add("a", "frozen<b>", "frozen<a>")
	f.Add(`frozen<"a">`, "list<frozen<b>>", "map<frozen<b>, frozen<b>>")
	f.Fuzz(func(t *testing.T, source, a, b string) {
		ks := &metadata.Keyspace{Name: "ks", Types: map[string]*metadata.Type{
			"a": {Keyspace: "ks", Name: "a", FieldNames: []string{"x", "y"}, FieldTypes: []string{a, b}},
			"b": {Keyspace: "ks", Name: "b", FieldNames: []string{"x", "y"}, FieldTypes: []string{b, a}},
		}}
		if typ, err := InternalSchemaDataType(ks, source); err == nil {
			_ = typ.String()
		}
	})
}

// TestColumnOrderFromSchema checks the decoding of
// system_schema.columns.clustering_order.
func TestColumnOrderFromSchema(t *testing.T) {
	for text, want := range map[string]metadata.ColumnOrder{
		"asc":  metadata.ColumnOrderAsc,
		"desc": metadata.ColumnOrderDesc,
		"DESC": metadata.ColumnOrderDesc,
		"none": metadata.ColumnOrderNone,
		"":     metadata.ColumnOrderNone,
	} {
		if got := columnOrderFromSchema(text); got != want {
			t.Errorf("columnOrderFromSchema(%q) = %v, want %v", text, got, want)
		}
	}
}
