// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestSchemaDataType(t *testing.T) {
	ks := &KeyspaceMetadata{Name: "ks", Types: map[string]*TypeMetadata{
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
	if addr.Kind() != cqltype.TypeUDT || addr.Fields().Len() != 2 {
		t.Errorf("addr fields: %v", addr)
	}
	for _, bad := range []string{"", "nosuch", "list<int, int>", "map<int>", "'org.Custom'", "other.addr", "int>", "vector<float, 0>"} {
		if _, err := InternalSchemaDataType(ks, bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}
