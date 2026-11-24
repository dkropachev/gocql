// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata_test

import (
	"slices"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing/routingtest"
)

func TestEmptySnapshot(t *testing.T) {
	for name, s := range map[string]*metadata.Snapshot{"nil": nil, "zero": {}} {
		if s.Generation() != 0 || s.Hosts() != nil || s.Diagnostics() != nil {
			t.Errorf("%s: reports a cluster", name)
		}
		if _, ok := s.Keyspace("ks"); ok {
			t.Errorf("%s: Keyspace found a keyspace", name)
		}
		if s.Keyspaces() != nil {
			t.Errorf("%s: Keyspaces lists keyspaces", name)
		}
		if _, ok := s.Table("ks", "t"); ok {
			t.Errorf("%s: Table found a table", name)
		}
		if _, ok := s.Host(cql.UUID{1}); ok {
			t.Errorf("%s: Host found a host", name)
		}
		if _, err := s.Replicas("ks", "t", 0); err == nil {
			t.Errorf("%s: Replicas succeeded", name)
		}
		if _, err := s.Token("ks", "t"); err == nil {
			t.Errorf("%s: Token succeeded", name)
		}
		if _, err := s.TokenRanges("ks", "t"); err == nil {
			t.Errorf("%s: TokenRanges succeeded", name)
		}
	}
}

// TestSnapshotLookups checks Keyspaces, Table and Host, and that what they
// return is a copy.
func TestSnapshotLookups(t *testing.T) {
	column := &metadata.Column{Name: "id", Kind: metadata.ColumnKindPartitionKey, Type: "int"}
	table := &metadata.Table{Name: "t", Columns: map[string]*metadata.Column{"id": column}, PartitionKey: []*metadata.Column{column}}
	hosts := []metadata.HostSnapshot{{HostID: cql.UUID{1}, Tokens: []int64{1}}, {HostID: cql.UUID{2}, Tokens: []int64{2}}}
	s := routingtest.NewSnapshot(routingtest.Cluster{
		Hosts: hosts,
		Keyspaces: map[string]*metadata.Keyspace{
			"b": {Name: "b", Tables: map[string]*metadata.Table{"t": table}, Views: map[string]*metadata.View{"v": {Name: "v"}}},
			"a": {Name: "a"},
		},
	})
	if got := s.Keyspaces(); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("Keyspaces() = %v", got)
	}
	got, ok := s.Table("b", "t")
	if !ok || got.Name != "t" || got.PartitionKey[0].Name != "id" {
		t.Fatalf("Table(b, t) = %+v, %v", got, ok)
	}
	got.PartitionKey[0].Name = "changed"
	got.Columns["id"].Name = "changed"
	if again, _ := s.Table("b", "t"); again.PartitionKey[0].Name != "id" || again.Columns["id"].Name != "id" {
		t.Error("Table returned the snapshot's table, not a copy")
	}
	for _, miss := range [][2]string{{"a", "t"}, {"b", "v"}, {"c", "t"}} {
		if _, ok := s.Table(miss[0], miss[1]); ok {
			t.Errorf("Table(%s, %s) found a table", miss[0], miss[1])
		}
	}
	h, ok := s.Host(cql.UUID{2})
	if !ok || h.HostID != (cql.UUID{2}) || !slices.Equal(h.Tokens, []int64{2}) {
		t.Fatalf("Host(2) = %+v, %v", h, ok)
	}
	h.Tokens[0] = 9
	if again, _ := s.Host(cql.UUID{2}); again.Tokens[0] != 2 {
		t.Error("Host returned the snapshot's tokens, not a copy")
	}
	if _, ok := s.Host(cql.UUID{3}); ok {
		t.Error("Host found an unknown host")
	}
}
