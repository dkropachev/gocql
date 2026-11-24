// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package structmap

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseFieldTag(t *testing.T) {
	for _, test := range []struct {
		name, field, tag string
		exported         bool
		want             FieldTag
		ok, fails        bool
	}{
		{name: "untagged", field: "UserID", exported: true, want: FieldTag{Column: "user_id"}, ok: true},
		{name: "renamed", field: "A", tag: `cql:"user_id"`, exported: true, want: FieldTag{Column: "user_id"}, ok: true},
		{name: "codec only", field: "A", tag: `cql:",codec=Int32Codec"`, exported: true, want: FieldTag{Column: "a", Codec: "Int32Codec"}, ok: true},
		{name: "codec expression", field: "A", tag: `cql:"a,codec=MapCodec(StringCodec, Int64Codec),stream=S"`, exported: true,
			want: FieldTag{Column: "a", Codec: "MapCodec(StringCodec, Int64Codec)", Stream: "S"}, ok: true},
		{name: "skipped", field: "A", tag: `cql:"-"`, exported: true},
		{name: "other tags only", field: "A", tag: `json:"x"`, exported: true, want: FieldTag{Column: "a"}, ok: true},
		{name: "unexported", field: "a"},
		{name: "unexported skipped", field: "a", tag: `cql:"-"`},
		{name: "unexported tagged", field: "a", tag: `cql:"a"`, fails: true},
		{name: "unexported empty tag", field: "a", tag: `cql:""`, fails: true},
		{name: "unknown option", field: "A", tag: `cql:"a,omitempty"`, exported: true, fails: true},
		{name: "empty codec", field: "A", tag: `cql:"a,codec="`, exported: true, fails: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok, err := ParseFieldTag(test.field, test.exported, reflect.StructTag(test.tag))
			if (err != nil) != test.fails || ok != test.ok || got != test.want {
				t.Fatalf("got %+v, %v, %v", got, ok, err)
			}
		})
	}
}

func TestSnakeCase(t *testing.T) {
	for name, want := range map[string]string{
		"A":             "a",
		"ID":            "id",
		"Name":          "name",
		"CreatedAt":     "created_at",
		"UserID":        "user_id",
		"HTTPServer":    "http_server",
		"UserIDs":       "user_i_ds", // as gocqlx; tag such a field
		"Field2Name":    "field2_name",
		"Already_Snake": "already_snake",
		"lower":         "lower",
		"ÉtéÀ":          "été_à",
	} {
		if got := SnakeCase(name); got != want {
			t.Errorf("SnakeCase(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestEmbeddedField(t *testing.T) {
	for _, test := range []struct {
		name, tag         string
		pointer, isStruct bool
		want              Embedding
		fails             bool
	}{
		{name: "struct", isStruct: true, want: EmbedFlatten},
		{name: "non-struct", want: EmbedField},
		{name: "skipped pointer", tag: `cql:"-"`, pointer: true, isStruct: true, want: EmbedSkip},
		{name: "pointer", pointer: true, isStruct: true, fails: true},
		{name: "tagged struct", tag: `cql:"base"`, isStruct: true, fails: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := EmbeddedField("E", reflect.StructTag(test.tag), test.pointer, test.isStruct)
			if (err != nil) != test.fails || got != test.want {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

func TestResolveFieldConflicts(t *testing.T) {
	keep, err := ResolveFieldConflicts([]FieldCandidate{
		{Column: "id", Path: "Base.ID", Depth: 1},
		{Column: "id", Path: "ID", Depth: 0},
		{Column: "owner", Path: "Base.Owner", Depth: 1},
	})
	if err != nil || !reflect.DeepEqual(keep, []int{1, 2}) {
		t.Fatalf("got %v, %v", keep, err)
	}
	if _, err := ResolveFieldConflicts([]FieldCandidate{{Column: "x", Path: "A"}, {Column: "x", Path: "B"}}); err == nil {
		t.Fatal("same-depth conflict accepted")
	}
	// A shallower field hides a same-depth pair declared before it, as in Go.
	keep, err = ResolveFieldConflicts([]FieldCandidate{
		{Column: "x", Path: "A.X", Depth: 1},
		{Column: "x", Path: "B.X", Depth: 1},
		{Column: "x", Path: "X", Depth: 0},
	})
	if err != nil || !reflect.DeepEqual(keep, []int{2}) {
		t.Fatalf("got %v, %v", keep, err)
	}
	_, err = ResolveFieldConflicts([]FieldCandidate{
		{Column: "x", Path: "A.X", Depth: 1},
		{Column: "x", Path: "X", Depth: 0},
		{Column: "x", Path: "Y", Depth: 0},
	})
	if err == nil || !strings.Contains(err.Error(), "fields X and Y") {
		t.Fatalf("got %v, want the conflict of X and Y", err)
	}
}
