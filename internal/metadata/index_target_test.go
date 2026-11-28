// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"testing"

	"github.com/scylladb/gocql/v2/metadata"
)

func TestParseIndexTarget(t *testing.T) {
	t.Parallel()
	for target, want := range map[string]struct {
		column string
		kind   metadata.IndexTargetKind
	}{
		"owner":                         {"owner", metadata.IndexTargetColumn},
		`"MyCol"`:                       {"MyCol", metadata.IndexTargetColumn},
		`"a""b"`:                        {`a"b`, metadata.IndexTargetColumn},
		"keys(m)":                       {"m", metadata.IndexTargetKeys},
		"values(l)":                     {"l", metadata.IndexTargetValues},
		"entries(m)":                    {"m", metadata.IndexTargetEntries},
		`full("Frozen")`:                {"Frozen", metadata.IndexTargetFull},
		`"keys(m)"`:                     {"keys(m)", metadata.IndexTargetColumn},
		`{"pk":["p"],"ck":["v"]}`:       {"v", metadata.IndexTargetColumn},
		`{"pk":["p"],"ck":["keys(m)"]}`: {"m", metadata.IndexTargetKeys},
		`{"pk":["p"],"ck":["a","b"]}`:   {"", metadata.IndexTargetUnknown},
		`{"pk":["p"]`:                   {"", metadata.IndexTargetUnknown},
		"":                              {"", metadata.IndexTargetUnknown},
		"other(m)":                      {"", metadata.IndexTargetUnknown},
		`"unterminated`:                 {"", metadata.IndexTargetUnknown},
		`"a"b"`:                         {"", metadata.IndexTargetUnknown},
		"a, b":                          {"", metadata.IndexTargetUnknown},
		"keys()":                        {"", metadata.IndexTargetUnknown},
	} {
		column, kind := InternalParseIndexTarget(target)
		if column != want.column || kind != want.kind {
			t.Errorf("InternalParseIndexTarget(%q) = %q, %v; want %q, %v", target, column, kind, want.column, want.kind)
		}
	}
}

// TestIndexBackingView checks that a ScyllaDB index's backing view, and its
// columns, go to Index.BackingView rather than Keyspace.Views, and that a
// view named like a backing view of another table's index stays a view.
func TestIndexBackingView(t *testing.T) {
	t.Parallel()
	keyspace := &metadata.Keyspace{Name: "ks"}
	tables := []metadata.Table{{Keyspace: "ks", Name: "t"}, {Keyspace: "ks", Name: "u"}}
	indexes := []metadata.Index{
		{Keyspace: "ks", Name: "by_v", Table: "t", Options: map[string]string{"target": "v"}},
		{Keyspace: "ks", Name: "other", Table: "t", Options: map[string]string{"target": "keys(m)"}},
	}
	views := []metadata.View{
		{Keyspace: "ks", Name: "by_v_index", BaseTable: "t"},
		{Keyspace: "ks", Name: "other_index", BaseTable: "u"},
	}
	columns := []metadata.Column{
		{Keyspace: "ks", Table: "by_v_index", Name: "v", Kind: metadata.ColumnKindPartitionKey},
		{Keyspace: "ks", Table: "other_index", Name: "x", Kind: metadata.ColumnKindPartitionKey},
	}
	compileMetadata(keyspace, tables, columns, nil, nil, nil, indexes, views, nil)
	ix := keyspace.Indexes["by_v"]
	if ix == nil || ix.Target != "v" || ix.TargetKind != metadata.IndexTargetColumn {
		t.Fatalf("Indexes[by_v] = %+v", ix)
	}
	if ix.BackingView == nil || ix.BackingView.Name != "by_v_index" || len(ix.BackingView.PartitionKey) != 1 || ix.BackingView.Columns["v"] == nil {
		t.Fatalf("by_v BackingView = %+v", ix.BackingView)
	}
	if _, ok := keyspace.Views["by_v_index"]; ok {
		t.Error("a backing view is listed in Views")
	}
	other := keyspace.Indexes["other"]
	if other.BackingView != nil || other.Target != "m" || other.TargetKind != metadata.IndexTargetKeys {
		t.Errorf("Indexes[other] = %+v", other)
	}
	if v := keyspace.Views["other_index"]; v == nil || v.Columns["x"] == nil {
		t.Errorf("a view of another table named like a backing view: %+v", v)
	}
}
