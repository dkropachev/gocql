// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
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

const hostRoutingShards = 4

// hostRoutingSession is a session whose hosts 1-4 of v2TestMetadata each have
// a ready typed pool of one connection per shard, with tablet or vnode
// placement for ks.table whose replicas are hosts 1, 2 and 3.
func hostRoutingSession(b testing.TB, tablets bool) *Session {
	b.Helper()
	s := step06Session(b)
	m := s.Metadata()
	k := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
	if tablets {
		root, _ := implRouting.InternalUpdateTabletRange(nil, step06Hint(b, -1<<63+1, 1<<63-1, cql.Option[uint64]{Value: 1, Set: true}, hTarget(1, 1), hTarget(2, 2), hTarget(3, 3)))
		routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, k, routing.InternalIndexTabletPlacement(implRouting.InternalTabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}), implRouting.InternalComparePlacementTable)
	} else {
		routing.SnapshotData(m).InternalKeyspaces["ks"].TabletBased = false
		routing.SnapshotData(m).InternalPlacements = nil
	}
	SessionData(s).InternalLogger = logging.InternalNopLogger{}
	SessionData(s).InternalHostSource = &InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{}}
	SessionData(s).InternalPool = &implPool.InternalPolicyConnPool{InternalHostConnPools: map[cqltype.UUID]*implPool.InternalHostConnPool{}}
	config := settings.DefaultConfig("localhost")
	SessionData(s).InternalConnCfg = &transport.ConnConfig{TypedConfig: settings.InternalConnectivityConfigPointer(config)}
	for _, snapshot := range routing.SnapshotData(m).InternalHosts {
		host := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(host).InternalHostId = snapshot.ID
		implEndpoint.HostInfoData(host).InternalDataCenter = snapshot.Datacenter
		implEndpoint.HostInfoData(host).InternalRack = snapshot.Rack
		implEndpoint.HostInfoData(host).InternalState.Store(endpoint.NodeUp)
		SessionData(s).InternalHostSource.InternalHosts[snapshot.ID.String()] = host
		pool := &implPool.InternalHostConnPool{InternalHost: host}
		pool.InternalServices = InternalNewPoolServices(s)
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
	routing.SnapshotData(m).InternalHostIndex = InternalSessionRoutingHostIndex(s, routing.SnapshotData(m).InternalHosts)
	return s
}

// BenchmarkHostRouting does a token-aware ordinary request's host path: it builds the
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
				p := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, Datacenter: dc})).(*routing.InternalRoutingPolicy)
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
						info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: cqltype.Quorum, Kind: cqltype.OrdinaryStatement}
						i++
						tablet := routing.InternalLookupTabletCached(m, info, &cache)
						conn, err := InternalSessionSelectTypedConn(s, ctx, info, p.InternalPlanWith(info, m, &plan, &tablet))
						if err != nil || conn == nil {
							b.Fatal("no connection", err)
						}
					}
				})
			})
		}
	}
}
