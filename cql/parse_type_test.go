// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"strings"
	"testing"
)

func TestParseDataType(t *testing.T) {
	for in, want := range map[string]string{
		"int":                                 "int",
		" VarChar ":                           "text",
		"map<text, frozen<list<int>>>":        "map<text,frozen<list<int>>>",
		"tuple<int,text, set<uuid>>":          "tuple<int,text,set<uuid>>",
		"vector<float, 3>":                    "vector<float,3>",
		"list<vector<double,2>>":              "list<vector<double,2>>",
		"FROZEN<map<timeuuid, tuple<blob>>>":  "frozen<map<timeuuid,tuple<blob>>>",
		"set<frozen<tuple<ascii, duration>>>": "set<frozen<tuple<ascii,duration>>>",
		"map<date, list<tinyint>>":            "map<date,list<tinyint>>",
		"list<decimal>":                       "list<decimal>",
	} {
		got, err := ParseDataType(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got.String() != want {
			t.Errorf("%q: got %s, want %s", in, got, want)
		}
	}
	if !MustParseDataType("varchar").Equal(TextType) || !MustParseDataType("list<int>").Equal(MustParseDataType("list< int >")) {
		t.Fatal("parsed types differ")
	}
	if !MustParseDataType("frozen<list<int>>").Frozen() {
		t.Fatal("frozen not recorded")
	}
	for _, bad := range []string{"", "address", "ks.address", "list<int", "list<int, int>", "map<int>", "vector<float>", "vector<float, 0>", "vector<float, 2, 3>", "int int", "list", strings.Repeat("list<", 70) + "int" + strings.Repeat(">", 70)} {
		_, err := ParseDataType(bad)
		if _, ok := errors.AsType[*ValidationError](err); !ok {
			t.Errorf("%q: got %v, want a ValidationError", bad, err)
		}
	}
}
