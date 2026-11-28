// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// staleTabletSnapshot is a snapshot of the tablet table ks.t whose
// placement holds one tablet, (0, 100] on hosts 1 and 2, and whose
// placement epoch signal is signal.
func staleTabletSnapshot(signal *atomic.Uint64) *MetadataSnapshot {
	hosts := []HostSnapshot{
		{HostID: cql.UUID{15: 1}, Datacenter: "dc1", Enabled: true, Up: true, Connected: true},
		{HostID: cql.UUID{15: 2}, Datacenter: "dc1", Enabled: true, Up: true, Connected: true},
		{HostID: cql.UUID{15: 3}, Datacenter: "dc1", Enabled: true, Up: true, Connected: true},
	}
	span := Span{Start: 0, End: 100, Replicas: []Target{
		{HostID: cql.UUID{15: 1}, ShardID: cql.Some[uint32](2)},
		{HostID: cql.UUID{15: 2}, ShardID: cql.Some[uint32](3)},
	}}
	var ranges *InternalTabletRangeTree
	ranges = ranges.Put(span.End, &InternalTabletRange{InternalSpan: span})
	k := InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "t"}
	var placements *tabletPlacementTree
	placements = placements.Put(k, InternalIndexTabletPlacement(InternalTabletPlacement{InternalRanges: ranges}))
	return NewMetadataSnapshot(SnapshotState{
		InternalHosts:           hosts,
		InternalHostIndex:       InternalBuildRoutingHostIndex(hosts),
		InternalKeyspaces:       map[string]*metadata.Keyspace{"ks": {Name: "ks", TabletBased: true, Tables: map[string]*metadata.Table{"t": {Name: "t"}}}},
		InternalTablets:         map[InternalMetadataTable][]Span{k: nil},
		InternalPlacements:      placements,
		InternalPlacementEpoch:  signal.Load(),
		InternalPlacementSignal: signal,
	})
}

// TestStalePlacementPublicLookups is the regression test for public lookups
// on a snapshot whose placement epoch the session moved past, as it does
// when more than 1024 tablet hints are pending (internal/cluster's
// QueueTabletFeedback): Replicas, ReplicaRoutes, AppendReplicaRoutes and
// TokenRanges answered from no placement at all, for every snapshot the
// application held, until the next publication. They answer from the
// snapshot's own tablets; only the built-in plan skips them.
func TestStalePlacementPublicLookups(t *testing.T) {
	var signal atomic.Uint64
	m := staleTabletSnapshot(&signal)
	info := routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](50), Kind: cql.OrdinaryStatement, Consistency: cql.Quorum}
	check := func(when string) {
		t.Helper()
		replicas, err := m.Public().Replicas("ks", "t", 50)
		if err != nil || len(replicas) != 2 || replicas[0].Host.HostID != (cql.UUID{15: 1}) || replicas[0].ShardID != cql.Some[uint32](2) {
			t.Fatalf("%s: Replicas = %v, %v", when, replicas, err)
		}
		if routes := routing.ReplicaRoutes(m.Public(), info); len(routes) != 2 || routes[1].ShardID() != cql.Some[uint32](3) {
			t.Fatalf("%s: ReplicaRoutes = %v", when, routes)
		}
		if routes := routing.AppendReplicaRoutes(make([]metadata.Route, 0, 4), m.Public(), info); len(routes) != 2 {
			t.Fatalf("%s: AppendReplicaRoutes = %v", when, routes)
		}
		ranges, complete, err := m.Public().TokenRanges("ks", "t")
		if err != nil || len(ranges) != 1 || ranges[0].End != 100 || complete {
			t.Fatalf("%s: TokenRanges = %v, %v, %v", when, ranges, complete, err)
		}
		if _, err := m.Public().Replicas("ks", "t", 500); !errors.Is(err, metadata.ErrPlacementUnknown) {
			t.Fatalf("%s: Replicas outside every tablet: %v", when, err)
		}
	}
	check("current snapshot")
	policy, err := NewRoutingPolicy(routing.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if plan := policy.Fallback(info, m); firstHost(plan) > 2 {
		t.Fatal("the current snapshot's plan did not start with a replica")
	}
	// The session forgets every tablet: the snapshot is stale.
	signal.Add(1)
	check("stale snapshot")
	// The built-in plan alone skips a stale snapshot's tablets.
	if l := InternalLookupTablet(m, info); l.node() != nil || !l.tracked {
		t.Fatal("the plan's lookup found a tablet in a stale snapshot")
	}
	plan := policy.Fallback(info, m).(*InternalLazyRoutingPlan)
	plan.Next()
	if plan.replicas != nil {
		t.Fatalf("the stale snapshot's plan kept replicas %v", plan.replicas)
	}
}

// firstHost returns the last byte of the ID of plan's first route's host.
func firstHost(plan QueryPlan) byte {
	r, ok := plan.Next()
	if !ok {
		return 0
	}
	return r.HostID()[15]
}

// TestTokenRangesComplete checks that TokenRanges reports a tablet table
// complete only when its tablets cover the ring without a gap, and a token
// ring once it has a range.
func TestTokenRangesComplete(t *testing.T) {
	for _, c := range []struct {
		name    string
		tablets []Span
		want    bool
	}{
		{"none", nil, false},
		{"one", []Span{{Start: math.MinInt64, End: math.MaxInt64}}, true},
		{"two", []Span{{Start: math.MinInt64, End: 0}, {Start: 0, End: math.MaxInt64}}, true},
		{"gap", []Span{{Start: math.MinInt64, End: 0}, {Start: 10, End: math.MaxInt64}}, false},
		{"head", []Span{{Start: 0, End: math.MaxInt64}}, false},
		{"tail", []Span{{Start: math.MinInt64, End: 0}}, false},
	} {
		m := NewMetadataSnapshot(SnapshotState{
			InternalKeyspaces: map[string]*metadata.Keyspace{"ks": {Name: "ks", TabletBased: true, Tables: map[string]*metadata.Table{"t": {Name: "t"}}}},
			InternalTablets:   map[InternalMetadataTable][]Span{{InternalKeyspace: "ks", InternalTable: "t"}: c.tablets},
		})
		if _, complete, err := m.TokenRanges("ks", "t"); err != nil || complete != c.want {
			t.Errorf("%s: complete %v, %v; want %v", c.name, complete, err, c.want)
		}
	}
	ring := vnodeCacheSnapshot(1)
	if _, complete, err := ring.TokenRanges("ks", "t"); err != nil || !complete {
		t.Errorf("token ring: complete %v, %v", complete, err)
	}
}
