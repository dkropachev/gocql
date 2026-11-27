// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
)

// TestSchemaEventView checks that a table event names a view when the
// event's snapshot, or for a drop the one before it, holds a view of that
// name, and a table otherwise.
func TestSchemaEventView(t *testing.T) {
	t.Parallel()
	snapshot := func(views, tables []string) *routing.MetadataSnapshot {
		ks := &metadata.Keyspace{Name: "ks", Views: map[string]*metadata.View{}, Tables: map[string]*metadata.Table{}}
		for _, v := range views {
			ks.Views[v] = &metadata.View{}
		}
		for _, n := range tables {
			ks.Tables[n] = &metadata.Table{}
		}
		return routing.NewMetadataSnapshot(routing.SnapshotState{InternalKeyspaces: map[string]*metadata.Keyspace{"ks": ks}})
	}
	empty := snapshot(nil, nil)
	withView := snapshot([]string{"v"}, []string{"t"})
	for name, c := range map[string]struct {
		cur, prev *routing.MetadataSnapshot
		object    string
		want      bool
	}{
		"created view":   {withView, empty, "v", true},
		"dropped view":   {empty, withView, "v", true},
		"table":          {withView, withView, "t", false},
		"unknown":        {empty, empty, "v", false},
		"no prev":        {empty, nil, "v", false},
		"current wins":   {snapshot(nil, []string{"v"}), withView, "v", false},
		"other keyspace": {withView, nil, "x", false},
	} {
		if got := isV2View("ks", c.object, c.cur, c.prev); got != c.want {
			t.Errorf("%s: isV2View = %v, want %v", name, got, c.want)
		}
	}
}
