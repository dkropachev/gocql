// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

func TestMirrorLayouts(t *testing.T) {
	if err := link.CheckLayout[metadata.Route, Route](); err != nil {
		t.Error(err)
	}
	if err := link.CheckLayout[routing.PartitionKey, partitionKeyRep](); err != nil {
		t.Error(err)
	}
	if metadata.AnyShard != AnyShard {
		t.Errorf("AnyShard differs: metadata %d, engine %d", metadata.AnyShard, AnyShard)
	}
}

// countingPartitioner wraps a partitioner and counts its calls.
type countingPartitioner struct {
	routing.TokenPartitioner
	calls *int
}

func (c countingPartitioner) Token(k routing.PartitionKey) (int64, error) {
	*c.calls++
	return c.TokenPartitioner.Token(k)
}

// TestMurmur3FastPath checks that only routing's own Murmur3 partitioner takes
// the engine's hashing shortcut, whatever value routing.Murmur3TokenPartitioner
// holds.
func TestMurmur3FastPath(t *testing.T) {
	if !isMurmur3Partitioner(routing.Murmur3TokenPartitioner) {
		t.Fatal("the built-in Murmur3 partitioner misses the fast path")
	}
	calls := 0
	wrapped := countingPartitioner{routing.Murmur3TokenPartitioner, &calls}
	if isMurmur3Partitioner(wrapped) || isMurmur3Partitioner(routing.CDCTokenPartitioner) {
		t.Fatal("another partitioner takes the Murmur3 fast path")
	}
}
