// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"errors"
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

// appPolicy is an application policy that routes nowhere and counts its
// calls, with optional Validate and Observe.
type appPolicy struct {
	picks, fallbacks int
}

func (p *appPolicy) Pick(routing.Info, *metadata.Snapshot) (metadata.Route, bool) {
	p.picks++
	return metadata.Route{}, false
}

func (p *appPolicy) Fallback(routing.Info, *metadata.Snapshot) routing.Plan {
	p.fallbacks++
	return routing.PlanOf()
}

type validatingPolicy struct {
	appPolicy
	err       error
	validates int
	observed  int
}

func (p *validatingPolicy) Validate(*metadata.Snapshot) error { p.validates++; return p.err }
func (p *validatingPolicy) Observe(routing.AttemptEvent)      { p.observed++ }

// TestPinnedBeforePolicy checks that a request pinned to a host follows the
// pinned plan without consulting an application's policy.
func TestPinnedBeforePolicy(t *testing.T) {
	m := v2TestMetadata()
	host := SnapshotData(m).InternalHosts[2].ID
	app := &appPolicy{}
	for _, policy := range []RoutingPolicy{EngineOf(app), SingleTargetRouting(Target{HostID: SnapshotData(m).InternalHosts[0].ID})} {
		plan := NewPlan(policy, routing.Info{HostID: cql.Some(host), ShardID: cql.Some[uint32](1)}, m)
		target, ok := nextTarget(plan)
		if !ok || target.HostID != host || target.ShardID != cql.Some[uint32](1) {
			t.Fatalf("%T: pinned plan routed to %+v, %v", policy, target, ok)
		}
		if _, ok := nextTarget(plan); ok {
			t.Fatalf("%T: a pinned plan has a second route", policy)
		}
	}
	if app.picks != 0 || app.fallbacks != 0 {
		t.Fatalf("the policy was consulted: %d picks, %d fallbacks", app.picks, app.fallbacks)
	}
	if _, ok := nextTarget(NewPlan(EngineOf(app), routing.Info{}, m)); ok || app.picks != 1 || app.fallbacks != 1 {
		t.Fatalf("an unpinned request: %d picks, %d fallbacks", app.picks, app.fallbacks)
	}
}

// TestApplicationPolicyInterfaces checks that Validate and Observe are
// optional, and that Validate runs once per snapshot until it passes.
func TestApplicationPolicyInterfaces(t *testing.T) {
	m, next := v2TestMetadata(), v2TestMetadata()
	plain := EngineOf(&appPolicy{})
	if err := plain.Validate(m); err != nil {
		t.Fatal(err)
	}
	plain.Observe(routing.AttemptEvent{})

	v := &validatingPolicy{err: errors.New("not yet")}
	p := EngineOf(v)
	for range 2 {
		if err := p.Validate(m); err == nil {
			t.Fatal("a failing Validate passed")
		}
	}
	v.err = nil
	for range 3 {
		if err := p.Validate(m); err != nil {
			t.Fatal(err)
		}
	}
	if v.validates != 3 {
		t.Fatalf("Validate ran %d times, want 3: twice failing, once passing", v.validates)
	}
	if err := p.Validate(next); err != nil || v.validates != 4 {
		t.Fatalf("a new snapshot: %v, %d validations", err, v.validates)
	}
	p.Observe(routing.AttemptEvent{})
	if v.observed != 1 {
		t.Fatalf("Observe ran %d times", v.observed)
	}
	if PublicPolicy(p) != routing.Policy(v) {
		t.Fatal("PublicPolicy does not return the application's policy")
	}
}

func TestTokenPartitionerFunc(t *testing.T) {
	cells := []model.EncodedCell{routingCell(t, cql.IntType, cql.Int32Codec, int32(7))}
	var got int64
	f := routing.TokenPartitionerFunc(func(k routing.PartitionKey) (int64, error) { got = k.Len(); return 9, nil })
	token, err := InternalComputeRoutingToken(cells, []int{0}, f)
	if err != nil || token != cql.Some[int64](9) || got != 4 {
		t.Fatalf("token %v, %v, key length %d", token, err, got)
	}
}
