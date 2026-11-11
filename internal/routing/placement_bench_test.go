// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"
	"testing"
)

func BenchmarkPlacementTree(b *testing.B) {
	const tables = 256
	keys := make([]InternalMetadataTable, tables)
	var root *tabletPlacementTree
	for i := range keys {
		keys[i] = InternalMetadataTable{InternalKeyspace: fmt.Sprintf("keyspace_%d", i%8), InternalTable: fmt.Sprintf("table_%03d", i)}
		root = root.Put(keys[i], InternalTabletPlacement{InternalIdentity: keys[i].InternalTable})
	}
	b.Run("GetTable", func(b *testing.B) {
		i := 0
		for b.Loop() {
			if root.Get(keys[i%tables]) == nil {
				b.Fatal("missing table")
			}
			i++
		}
	})
	b.Run("PutTable", func(b *testing.B) {
		i := 0
		for b.Loop() {
			_ = root.Put(keys[i%tables], InternalTabletPlacement{})
			i++
		}
	})
}
