// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"math"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
)

// vnodeCacheSnapshot is a snapshot of keyspace ks, with table t, whose one
// vnode range is replicated to hosts.
func vnodeCacheSnapshot(hosts ...byte) *MetadataSnapshot {
	replicas := make([]Target, len(hosts))
	for i, h := range hosts {
		replicas[i] = Target{HostID: cqltype.UUID{15: h}}
	}
	return NewMetadataSnapshot(SnapshotState{
		InternalKeyspaces: map[string]*implMetadata.KeyspaceMetadata{"ks": {Name: "ks", Tables: map[string]*implMetadata.TableMetadata{"t": {Name: "t"}}}},
		InternalRanges:    map[string][]TokenRange{"ks": {{Start: math.MinInt64, End: math.MaxInt64, Replicas: replicas}}},
	})
}

func cachedReplicas(m *MetadataSnapshot, c *InternalPlacementCache) []Target {
	info := model.RoutingInfo{Keyspace: "ks", Table: "t", Token: cqltype.Option[int64]{Value: 7, Set: true}}
	l := InternalLookupTabletCached(m, info, c)
	r, _ := m.lookupReplicas(&l, info.Keyspace, info.Table, info.Token.Value)
	return r
}

func replicaHosts(r []Target) []byte {
	out := make([]byte, len(r))
	for i, t := range r {
		out[i] = t.HostID[15]
	}
	return out
}

// TestRoutingCacheReusesRanges checks that a prepared statement's requests
// on one snapshot look its vnode ranges up once: the ranges found by the
// first request serve the next ones.
func TestRoutingCacheReusesRanges(t *testing.T) {
	var c InternalPlacementCache
	m := vnodeCacheSnapshot(1, 2, 3)
	if got := replicaHosts(cachedReplicas(m, &c)); string(got) != "\x01\x02\x03" {
		t.Fatalf("first route: replicas %v", got)
	}
	// Snapshots are never changed once published; changing this one's ranges
	// shows whether the second request looked them up again.
	SnapshotData(m).InternalRanges = map[string][]TokenRange{"ks": {{Start: math.MinInt64, End: math.MaxInt64, Replicas: []Target{{HostID: cqltype.UUID{15: 9}}}}}}
	if got := replicaHosts(cachedReplicas(m, &c)); string(got) != "\x01\x02\x03" {
		t.Fatalf("second route on the same snapshot looked the ranges up again: replicas %v", got)
	}
}

// TestRoutingCacheInvalidatesOnSnapshotChange checks that a new snapshot,
// here one with the keyspace's replication factor raised, replaces the
// ranges a statement cached for the previous snapshot.
func TestRoutingCacheInvalidatesOnSnapshotChange(t *testing.T) {
	var c InternalPlacementCache
	before := vnodeCacheSnapshot(1, 2)
	if got := replicaHosts(cachedReplicas(before, &c)); string(got) != "\x01\x02" {
		t.Fatalf("RF 2: replicas %v", got)
	}
	after := vnodeCacheSnapshot(1, 2, 3)
	if got := replicaHosts(cachedReplicas(after, &c)); string(got) != "\x01\x02\x03" {
		t.Fatalf("RF 3 snapshot routed with RF 2 ranges: replicas %v", got)
	}
	// A request still holding the earlier snapshot routes by it.
	if got := replicaHosts(cachedReplicas(before, &c)); string(got) != "\x01\x02" {
		t.Fatalf("earlier snapshot: replicas %v", got)
	}
	// A table missing from the snapshot has no replicas.
	dropped := vnodeCacheSnapshot(1, 2, 3)
	SnapshotData(dropped).InternalKeyspaces["ks"].Tables = nil
	if got := cachedReplicas(dropped, &c); got != nil {
		t.Fatalf("dropped table: replicas %v", got)
	}
}

// TestRoutingCacheStalePlacementSignal checks that a statement whose vnode
// ranges are cached for a snapshot routes to no replicas once a tablet
// update bumps the placement signal past that snapshot's epoch, as an
// uncached lookup does.
func TestRoutingCacheStalePlacementSignal(t *testing.T) {
	var c InternalPlacementCache
	var signal atomic.Uint64
	m := vnodeCacheSnapshot(1, 2, 3)
	SnapshotData(m).InternalPlacementSignal = &signal
	if got := replicaHosts(cachedReplicas(m, &c)); string(got) != "\x01\x02\x03" {
		t.Fatalf("warm route: replicas %v", got)
	}
	signal.Add(1)
	if got := cachedReplicas(m, &c); got != nil {
		t.Fatalf("route after the placement signal moved on: replicas %v", got)
	}
}

// TestRoutingCacheTabletRanges checks that a table whose snapshot ranges are
// its tablets, without a tablet placement, routes by them when cached: a
// token outside every tablet has no replicas rather than wrapping around.
func TestRoutingCacheTabletRanges(t *testing.T) {
	var c InternalPlacementCache
	m := vnodeCacheSnapshot(1, 2, 3)
	SnapshotData(m).InternalTablets = map[InternalMetadataTable][]TokenRange{
		{InternalKeyspace: "ks", InternalTable: "t"}: {{Start: 0, End: 100, Replicas: []Target{{HostID: cqltype.UUID{15: 4}}}}},
	}
	for range 2 {
		if got := replicaHosts(cachedReplicas(m, &c)); string(got) != "\x04" {
			t.Fatalf("token in a tablet: replicas %v", got)
		}
	}
	info := model.RoutingInfo{Keyspace: "ks", Table: "t", Token: cqltype.Option[int64]{Value: 200, Set: true}}
	l := InternalLookupTabletCached(m, info, &c)
	if l.cached() == nil {
		t.Fatal("the lookup did not carry the cached ranges")
	}
	if r, _ := m.lookupReplicas(&l, info.Keyspace, info.Table, info.Token.Value); r != nil {
		t.Fatalf("token outside every tablet: replicas %v", replicaHosts(r))
	}
}

// TestRoutingCacheReleasesSnapshot checks that a statement's cache does not
// keep the snapshot it last routed by alive.
func TestRoutingCacheReleasesSnapshot(t *testing.T) {
	var c InternalPlacementCache
	released := make(chan struct{})
	func() {
		m := vnodeCacheSnapshot(1, 2, 3)
		runtime.AddCleanup(m, func(ch chan struct{}) { close(ch) }, released)
		if got := cachedReplicas(m, &c); len(got) != 3 {
			t.Fatalf("replicas %v", got)
		}
	}()
	for range 10 {
		runtime.GC()
		select {
		case <-released:
			if c.entry.Load() == nil {
				t.Fatal("the cache entry was dropped")
			}
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("the placement cache kept the snapshot alive")
}
