// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"sync"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/streams"
)

func hostRouteConn(host *HostInfo, shard int) *driverConn {
	c := &driverConn{InternalStreams: streams.New(), InternalHost: host}
	f := new(ScyllaConnectionFeatures)
	h := new(ScyllaHostFeatures)
	implEndpoint.ScyllaHostFeaturesData(h).InternalNrShards = hostRoutingShards
	f.ScyllaHostFeatures = *h
	implEndpoint.ScyllaConnectionFeaturesData(f).InternalShard = shard
	c.InternalScyllaSupported = *f
	return c
}

// A plan routes a replica to the shard owning the token and then to any
// shard of the same host, unless the request is conditional.
func TestHostRoutePlanFallsBackToAnyShardOfHost(t *testing.T) {
	s := hostRoutingSession(t, false)
	m := s.Metadata()
	p := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true})).(*routing.InternalRoutingPolicy)
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 42, Set: true}, Consistency: Quorum, Kind: OrdinaryStatement}
	var storage routing.InternalLazyRoutingPlan
	plan := p.InternalPlanWith(info, m, &storage, nil)
	first, ok := plan.Next()
	if !ok || first.Shard == AnyShard || first.Shard != first.ShardOf(42) {
		t.Fatal("first route is not to the token's shard", first, ok)
	}
	second, ok := plan.Next()
	if !ok || second.HostID() != first.HostID() || second.Shard != AnyShard {
		t.Fatal("no fallback to any shard of the host", first.Target(), second.Target(), ok)
	}
	if third, ok := plan.Next(); !ok || third.HostID() == first.HostID() {
		t.Fatal("host routed to again", third, ok)
	}
	info.Conditional = true
	plan = p.InternalPlanWith(info, m, &storage, nil)
	first, _ = plan.Next()
	if second, ok := plan.Next(); !ok || second.HostID() == first.HostID() {
		t.Fatal("conditional request fell back to another shard", second, ok)
	}
}

// A route picks only its shard's connection, and its host's least busy one
// for any shard; it has no pool once its host's pool is removed and gets the
// new one when the host gets a pool again.
func TestHostRoutePicksShardAndFollowsPool(t *testing.T) {
	s := hostRoutingSession(t, false)
	pools := implCluster.SessionData(s).InternalPool
	id := routing.SnapshotData(s.Metadata()).InternalHosts[0].ID
	route := routing.InternalNewRoute(pools.InternalRoute(id), 2)
	conn := implCluster.InternalSessionPickRoute(s, route, RoutingInfo{})
	if conn == nil || implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard != 2 {
		t.Fatal("picked another shard", conn)
	}
	if implCluster.InternalSessionPickRoute(s, route.WithShard(hostRoutingShards), RoutingInfo{}) != nil {
		t.Fatal("picked a shard the host lacks")
	}
	if implCluster.InternalSessionPickRoute(s, route.WithShard(AnyShard), RoutingInfo{}) == nil {
		t.Fatal("no connection for any shard")
	}
	old := route.InternalHost().Pool()
	// Remove an empty stand-in, so that closing it closes no test connection.
	pools.InternalMu.Lock()
	pools.InternalHostConnPools[id] = &hostConnPool{InternalHost: old.InternalHost, InternalConnPicker: nopConnPicker{}}
	pools.InternalMu.Unlock()
	pools.InternalRemoveHost(id)
	if route.InternalHost().Pool() != nil || implCluster.InternalSessionPickRoute(s, route, RoutingInfo{}) != nil {
		t.Fatal("route kept a removed pool")
	}
	pools.InternalMu.Lock()
	pools.InternalHostConnPools[id] = old
	pools.InternalMu.Unlock()
	if route.InternalHost().Pool() != old {
		t.Fatal("route did not find its host's new pool")
	}
}

// Picks read a typed pool's published connections while connections come
// and go.
func TestHostRoutePickWhilePoolChanges(t *testing.T) {
	s := hostRoutingSession(t, false)
	pools := implCluster.SessionData(s).InternalPool
	id := routing.SnapshotData(s.Metadata()).InternalHosts[0].ID
	host := pools.InternalRoute(id)
	pool := host.Pool()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			c := hostRouteConn(pool.InternalHost, i%hostRoutingShards)
			_ = pool.InternalConnPicker.Put(c)
			pool.InternalConnPicker.Remove(c)
		}
	}()
	for i := 0; i < 20000; i++ {
		r := routing.InternalNewRoute(host, int32(i%hostRoutingShards))
		if r.Shard%2 == 0 {
			r = r.WithShard(AnyShard)
		}
		_ = implCluster.InternalSessionPickRoute(s, r, RoutingInfo{})
	}
	cancel()
	wg.Wait()
}
