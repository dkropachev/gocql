// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/model"
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
	// A public route without a shard is the engine's AnyShard route, and a
	// shard the engine cannot hold degrades to it too.
	for _, shard := range []cql.Option[uint32]{{}, {Value: 1 << 31, Set: true}} {
		r := metadata.Route{}.WithShardID(shard)
		if got := link.Mirror[Route](&r).shard; got != AnyShard {
			t.Errorf("WithShard(%v) holds shard %d, want AnyShard %d", shard, got, AnyShard)
		}
	}
	r := metadata.Route{}.WithShardID(cql.Option[uint32]{Value: 7, Set: true})
	if got := link.Mirror[Route](&r).shard; got != 7 {
		t.Errorf("WithShard(7) holds shard %d", got)
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

// uncomparablePartitioner cannot be compared with ==: comparing it with
// another value of its type panics.
type uncomparablePartitioner struct{ salt []int64 }

func (uncomparablePartitioner) Token(routing.PartitionKey) (int64, error) { return 42, nil }

// TestMurmur3FastPath checks that only routing's own Murmur3 partitioner takes
// the engine's hashing shortcut, whatever value routing.Murmur3TokenPartitioner
// holds.
func TestMurmur3FastPath(t *testing.T) {
	cells := []model.EncodedCell{routingCell(t, cql.IntType, cql.Int32Codec, int32(7))}
	builtin, err := InternalComputeRoutingToken(cells, []int{0}, routing.Murmur3TokenPartitioner)
	if err != nil || !builtin.Set {
		t.Fatal(builtin, err)
	}

	t.Run("ReplacedVariable", func(t *testing.T) {
		calls := 0
		original := routing.Murmur3TokenPartitioner
		routing.Murmur3TokenPartitioner = countingPartitioner{original, &calls}
		t.Cleanup(func() { routing.Murmur3TokenPartitioner = original })
		got, err := InternalComputeRoutingToken(cells, []int{0}, routing.Murmur3TokenPartitioner)
		if err != nil || got != builtin {
			t.Fatalf("token %v, %v; want %v", got, err, builtin)
		}
		if calls != 1 {
			t.Fatalf("the wrapping partitioner ran %d times, want 1", calls)
		}
	})

	t.Run("Uncomparable", func(t *testing.T) {
		original := routing.Murmur3TokenPartitioner
		routing.Murmur3TokenPartitioner = uncomparablePartitioner{}
		t.Cleanup(func() { routing.Murmur3TokenPartitioner = original })
		got, err := InternalComputeRoutingToken(cells, []int{0}, uncomparablePartitioner{salt: []int64{1}})
		if err != nil || !got.Set || got.Value != 42 {
			t.Fatalf("token %v, %v; want 42", got, err)
		}
	})
}

// TestPublicRouteShard checks the public shard accessors on a route with a host.
func TestPublicRouteShard(t *testing.T) {
	routes := routing.HostRoutes(v2TestMetadata().Public())
	if len(routes) == 0 {
		t.Fatal("no host routes")
	}
	r := routes[0]
	if r.ShardID().Set || r.Target().ShardID.Set {
		t.Fatalf("a host route has shard %v", r.ShardID())
	}
	three := cql.Option[uint32]{Value: 3, Set: true}
	r3 := r.WithShardID(three)
	if r3.ShardID() != three || r3.Target().ShardID != three || r3.HostID() != r.HostID() {
		t.Fatalf("WithShardID(3): shard %v, target %+v", r3.ShardID(), r3.Target())
	}
	if back := r3.WithShardID(cql.Option[uint32]{}); back.ShardID().Set {
		t.Fatalf("WithShardID(unset) kept shard %v", back.ShardID())
	}
	if owner := r.ShardOf(42); r.WithShardID(owner).ShardID() != owner {
		t.Fatalf("WithShardID(ShardOf(42)) = %v, want %v", r.WithShardID(owner).ShardID(), owner)
	}
}
