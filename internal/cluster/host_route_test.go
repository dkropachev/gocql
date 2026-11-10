// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func hostRouteConn(host *endpoint.HostInfo, shard int) *transport.Conn {
	c := &transport.Conn{InternalStreams: streams.New(), InternalHost: host}
	f := new(endpoint.ScyllaConnectionFeatures)
	h := new(endpoint.ScyllaHostFeatures)
	implEndpoint.ScyllaHostFeaturesData(h).InternalNrShards = hostRoutingShards
	f.ScyllaHostFeatures = *h
	implEndpoint.ScyllaConnectionFeaturesData(f).InternalShard = shard
	c.InternalScyllaSupported = *f
	return c
}

const hostRoutingShards = 4

// hostRoutingSession is a session whose hosts 1-4 of v2TestMetadata each have
// a ready typed pool of one connection per shard, with tablet or vnode
// placement for ks.table whose replicas are hosts 1, 2 and 3.
func hostRoutingSession(b testing.TB, tablets bool) *Core {
	b.Helper()
	s := testSession(b)
	m := s.Metadata()
	k := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
	if tablets {
		root, _ := implRouting.InternalUpdateTabletRange(nil, testHint(b, -1<<63+1, 1<<63-1, cqltype.Option[uint64]{Value: 1, Set: true}, hTarget(1, 1), hTarget(2, 2), hTarget(3, 3)))
		routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, k, routing.InternalIndexTabletPlacement(implRouting.InternalTabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}), implRouting.InternalComparePlacementTable)
	} else {
		routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = false
		routing.SnapshotData(m).InternalPlacements = nil
	}
	SessionData(s).InternalLogger = logging.InternalNopLogger{}
	SessionData(s).InternalHostSource = &InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{}}
	SessionData(s).InternalPool = &implPool.InternalPolicyConnPool{InternalHostConnPools: map[cqltype.UUID]*implPool.InternalHostConnPool{}}
	config := settings.DefaultConfig("localhost")
	SessionData(s).InternalConnCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityConfigPointer()}
	for _, snapshot := range routing.SnapshotData(m).InternalHosts {
		host := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(host).InternalHostId = snapshot.ID
		implEndpoint.HostInfoData(host).InternalDataCenter = snapshot.Datacenter
		implEndpoint.HostInfoData(host).InternalRack = snapshot.Rack
		implEndpoint.HostInfoData(host).InternalState.Store(endpoint.NodeUp)
		SessionData(s).InternalHostSource.InternalHosts[snapshot.ID.String()] = host
		pool := &implPool.InternalHostConnPool{InternalHost: host}
		pool.InternalServices = s.NewPoolServices()
		pool.InternalStarted = true
		pool.InternalState = implPool.InternalPoolReady
		for shard := 0; shard < hostRoutingShards; shard++ {
			c := &transport.Conn{InternalStreams: streams.New(), InternalHost: host}
			c.InternalScyllaSupported = *func() *endpoint.ScyllaConnectionFeatures {
				f := new(endpoint.ScyllaConnectionFeatures)
				f.ScyllaHostFeatures = *func() *endpoint.ScyllaHostFeatures {
					h := new(endpoint.ScyllaHostFeatures)
					implEndpoint.ScyllaHostFeaturesData(h).InternalNrShards = hostRoutingShards
					return h
				}()
				implEndpoint.ScyllaConnectionFeaturesData(f).InternalShard = shard
				return f
			}()
			if shard == 0 {
				pool.InternalConnPicker = codecTestMust(implPool.InternalNewTypedConnPicker(c, config.Pool))
			}
			if err := pool.InternalConnPicker.Put(c); err != nil {
				b.Fatal(err)
			}
		}
		SessionData(s).InternalPool.InternalHostConnPools[snapshot.ID] = pool
	}
	// As a published snapshot does, index the hosts with the session's routes.
	routing.SnapshotData(m).InternalHostIndex = sessionRoutingHostIndex(s, routing.SnapshotData(m).InternalHosts)
	return s
}

func TestHostRoute(t *testing.T) {
	t.Run("Pick", func(t *testing.T) {
		// A route picks only its shard's connection, and its host's least busy one
		// for any shard; it has no pool once its host's pool is removed and gets the
		// new one when the host gets a pool again.
		t.Run("ShardAndFollowsPool", func(t *testing.T) {
			s := hostRoutingSession(t, false)
			pools := SessionData(s).InternalPool
			id := routing.SnapshotData(s.Metadata()).InternalHosts[0].ID
			route := routing.InternalNewRoute(pools.InternalRoute(id), 2)
			conn := sessionPickRoute(s, route, model.RoutingInfo{})
			if conn == nil || implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard != 2 {
				t.Fatal("picked another shard", conn)
			}
			if sessionPickRoute(s, route.WithShard(hostRoutingShards), model.RoutingInfo{}) != nil {
				t.Fatal("picked a shard the host lacks")
			}
			if sessionPickRoute(s, route.WithShard(implRouting.AnyShard), model.RoutingInfo{}) == nil {
				t.Fatal("no connection for any shard")
			}
			old := route.InternalHost().Pool()
			// Remove an empty stand-in, so that closing it closes no test connection.
			pools.InternalMu.Lock()
			pools.InternalHostConnPools[id] = &implPool.InternalHostConnPool{InternalHost: old.InternalHost, InternalConnPicker: implPool.InternalNopConnPicker{}}
			pools.InternalMu.Unlock()
			pools.InternalRemoveHost(id)
			if route.InternalHost().Pool() != nil || sessionPickRoute(s, route, model.RoutingInfo{}) != nil {
				t.Fatal("route kept a removed pool")
			}
			pools.InternalMu.Lock()
			pools.InternalHostConnPools[id] = old
			pools.InternalMu.Unlock()
			if route.InternalHost().Pool() != old {
				t.Fatal("route did not find its host's new pool")
			}
		})

		// Picks read a typed pool's published connections while connections come
		// and go.
		t.Run("WhilePoolChanges", func(t *testing.T) {
			s := hostRoutingSession(t, false)
			pools := SessionData(s).InternalPool
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
					r = r.WithShard(implRouting.AnyShard)
				}
				_ = sessionPickRoute(s, r, model.RoutingInfo{})
			}
			cancel()
			wg.Wait()
		})
	})

	// A plan routes a replica to the shard owning the token and then to any
	// shard of the same host, unless the request is conditional.
	t.Run("Plan", func(t *testing.T) {
		s := hostRoutingSession(t, false)
		m := s.Metadata()
		p := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true})).(*routing.InternalRoutingPolicy)
		info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Value: 42, Set: true}, Consistency: cqltype.Quorum, Kind: cqltype.OrdinaryStatement}
		var storage routing.InternalLazyRoutingPlan
		plan := p.InternalPlanWith(info, m, &storage, nil)
		first, ok := plan.Next()
		if !ok || first.Shard == implRouting.AnyShard || first.Shard != first.ShardOf(42) {
			t.Fatal("first route is not to the token's shard", first, ok)
		}
		second, ok := plan.Next()
		if !ok || second.HostID() != first.HostID() || second.Shard != implRouting.AnyShard {
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
	})
}
