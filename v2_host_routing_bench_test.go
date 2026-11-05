// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/streams"
)

const hostRoutingShards = 4

// hostRoutingSession is a session whose hosts 1-4 of v2TestMetadata each have
// a ready typed pool of one connection per shard, with tablet or vnode
// placement for ks.table whose replicas are hosts 1, 2 and 3.
func hostRoutingSession(b *testing.B, tablets bool) *Session {
	b.Helper()
	s := step06Session(b)
	m := s.Metadata()
	k := metadataTable{InternalKeyspace: "ks", InternalTable: "table"}
	if tablets {
		root, _ := updateTabletRange(nil, step06Hint(b, -1<<63+1, 1<<63-1, Option[uint64]{Value: 1, Set: true}, hTarget(1, 1), hTarget(2, 2), hTarget(3, 3)))
		routing.SnapshotData(m).InternalPlacements = placementPut(routing.SnapshotData(m).InternalPlacements, k, routing.InternalIndexTabletPlacement(tabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}), comparePlacementTable)
	} else {
		routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = false
		routing.SnapshotData(m).InternalPlacements = nil
	}
	implCluster.SessionData(s).InternalLogger = nopLogger{}
	implCluster.SessionData(s).InternalHostSource = &ringDescriber{InternalHosts: map[string]*HostInfo{}}
	implCluster.SessionData(s).InternalPool = &policyConnPool{InternalHostConnPools: map[UUID]*hostConnPool{}}
	config := DefaultConfig("localhost")
	implCluster.SessionData(s).InternalConnCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config)}
	for _, snapshot := range routing.SnapshotData(m).InternalHosts {
		host := new(HostInfo)
		implEndpoint.HostInfoData(host).InternalHostId = snapshot.ID
		implEndpoint.HostInfoData(host).InternalDataCenter = snapshot.Datacenter
		implEndpoint.HostInfoData(host).InternalRack = snapshot.Rack
		implEndpoint.HostInfoData(host).InternalState.Store(NodeUp)
		implCluster.SessionData(s).InternalHostSource.InternalHosts[snapshot.ID.String()] = host
		pool := &hostConnPool{InternalHost: host}
		pool.InternalServices = newPoolServices(s)
		pool.InternalStarted = true
		pool.InternalState = poolReady
		for shard := 0; shard < hostRoutingShards; shard++ {
			c := &driverConn{InternalStreams: streams.New(), InternalHost: host}
			c.InternalScyllaSupported = *func() *ScyllaConnectionFeatures {
				f := new(ScyllaConnectionFeatures)
				f.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
					h := new(ScyllaHostFeatures)
					implEndpoint.ScyllaHostFeaturesData(h).InternalNrShards = hostRoutingShards
					return h
				}()
				implEndpoint.ScyllaConnectionFeaturesData(f).InternalShard = shard
				return f
			}()
			if shard == 0 {
				pool.InternalConnPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
			}
			if err := pool.InternalConnPicker.Put(c); err != nil {
				b.Fatal(err)
			}
		}
		implCluster.SessionData(s).InternalPool.InternalHostConnPools[snapshot.ID] = pool
	}
	return s
}

// BenchmarkHostRouting does a token-aware request's host path: it builds the
// routing plan and selects a connection for its first target, as an
// execution's first attempt does, without sending anything.
func BenchmarkHostRouting(b *testing.B) {
	for _, placement := range []string{"tablets", "vnodes"} {
		for _, dc := range []string{"", "a"} {
			name := placement + "/any-dc"
			if dc != "" {
				name = placement + "/local-dc"
			}
			b.Run(name, func(b *testing.B) {
				s := hostRoutingSession(b, placement == "tablets")
				p := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, Datacenter: dc})).(*routing.InternalRoutingPolicy)
				tokens := make([]int64, 4096)
				for i := range tokens {
					tokens[i] = int64(uint64(i) * 0x9e3779b97f4a7c15)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var plan routing.InternalLazyRoutingPlan
					var cache routing.InternalPlacementCache
					i := 0
					for pb.Next() {
						m := s.Metadata()
						info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: Quorum}
						i++
						tablet := routing.InternalLookupTabletCached(m, info, &cache)
						conn, err := implCluster.InternalSessionSelectTypedConn(s, ctx, info, p.InternalPlanWith(info, m, &plan, &tablet))
						if err != nil || conn == nil {
							b.Fatal("no connection", err)
						}
					}
				})
			})
		}
	}
}
