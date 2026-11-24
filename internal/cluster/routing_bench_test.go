// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

func BenchmarkRouting(b *testing.B) {
	// BenchmarkRouting/FirstRoute builds a worker-owned plan for an ordinary
	// statement and takes its first target, the pick every request makes.
	b.Run("FirstRoute", func(b *testing.B) {
		for _, count := range []int{3, 30} {
			for _, rack := range []string{"", "r1"} {
				for _, shuffle := range []bool{true, false} {
					b.Run(fmt.Sprintf("hosts%d/rack=%q/shuffle=%v", count, rack, shuffle), func(b *testing.B) {
						m := v2TestMetadata()
						routing.SnapshotData(m).InternalHosts = make([]metadata.HostSnapshot, count)
						for i := range routing.SnapshotData(m).InternalHosts {
							h := v2TestHost(0, "a", fmt.Sprintf("r%d", i%3+1))
							binary.BigEndian.PutUint64(h.HostID[8:], uint64(i+1))
							routing.SnapshotData(m).InternalHosts[i] = h
						}
						routing.SnapshotData(m).InternalHostIndex = implRouting.InternalBuildRoutingHostIndex(routing.SnapshotData(m).InternalHosts)
						h := testHint(b, 0, 100, cql.Option[uint64]{}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
						root, _ := implRouting.InternalUpdateTabletRange(nil, h)
						routing.SnapshotData(m).InternalPlacements = routing.SnapshotData(m).InternalPlacements.Put(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalRanges: root})
						info := routing2.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cql.Quorum, Kind: cql.OrdinaryStatement}
						p := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Datacenter: "a", Rack: rack, FixedReplicaOrder: !shuffle, NoLeaderAware: true})).(*implRouting.InternalRoutingPolicy)
						var storage routing.InternalLazyRoutingPlan
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							target, ok := implRouting.InternalNextTarget(p.InternalPlanInto(info, m, &storage))
							if !ok {
								b.Fatal("no target")
							}
							benchmarkTarget = target
						}
					})
				}
			}
		}
	})

	b.Run("FirstTarget", func(b *testing.B) {
		for _, count := range []int{3, 30, 300} {
			for _, kind := range []string{"ordinary", "lwt", "leader", "latency", "pinned"} {
				b.Run(fmt.Sprintf("hosts%d/%s", count, kind), func(b *testing.B) {
					m := v2TestMetadata()
					routing.SnapshotData(m).InternalHosts = make([]metadata.HostSnapshot, count)
					for i := range routing.SnapshotData(m).InternalHosts {
						h := v2TestHost(0, "a", "r1")
						binary.BigEndian.PutUint64(h.HostID[8:], uint64(i+1))
						routing.SnapshotData(m).InternalHosts[i] = h
					}
					routing.SnapshotData(m).InternalHostIndex = implRouting.InternalBuildRoutingHostIndex(routing.SnapshotData(m).InternalHosts)
					h := testHint(b, 0, 100, cql.Option[uint64]{Value: 7, Set: true}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
					root, _ := implRouting.InternalUpdateTabletRange(nil, h)
					routing.SnapshotData(m).InternalPlacements = routing.SnapshotData(m).InternalPlacements.Put(implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalRanges: root})
					info := routing2.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cql.Quorum}
					options := routing2.Options{Datacenter: "a", Rack: "r1", NoLeaderAware: kind != "leader"}
					if kind == "lwt" {
						info.Kind = cql.ConditionalStatement
					}
					if kind == "leader" {
						routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
					}
					if kind == "latency" {
						options.Latency.Enabled = true
					}
					if kind == "pinned" {
						info.HostID = cql.Option[cql.UUID]{Value: routing.SnapshotData(m).InternalHosts[0].HostID, Set: true}
					}
					p := codecTestMust(implRouting.NewRoutingPolicy(options))
					if kind == "latency" {
						p.(*implRouting.InternalRoutingPolicy).InternalDecisions.Store(&implRouting.InternalRoutingDecisions{InternalSlow: map[cql.UUID]time.Time{routing.SnapshotData(m).InternalHosts[2].HostID: time.Now().Add(time.Hour)}})
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := p.Validate(m); err != nil {
							b.Fatal(err)
						}
						plan := implRouting.NewPlan(p, info, m)
						target, ok := implRouting.InternalNextTarget(plan)
						if !ok {
							b.Fatal("no target")
						}
						benchmarkTarget = target
					}
				})
			}
		}
	})

	// BenchmarkRouting/Host does a token-aware ordinary request's host path: it builds the
	// routing plan and selects a connection for its first target, as an
	// execution's first attempt does, without sending anything.
	b.Run("Host", func(b *testing.B) {
		for _, placement := range []string{"tablets", "vnodes"} {
			for _, dc := range []string{"", "a"} {
				name := placement + "/any-dc"
				if dc != "" {
					name = placement + "/local-dc"
				}
				b.Run(name, func(b *testing.B) {
					s := hostRoutingSession(b, placement == "tablets")
					p := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Datacenter: dc, FixedReplicaOrder: true, NoLeaderAware: true})).(*routing.InternalRoutingPolicy)
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
							info := routing2.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: cql.Quorum, Kind: cql.OrdinaryStatement}
							i++
							tablet := routing.InternalLookupTabletCached(m, info, &cache)
							conn, err := s.SelectTypedConn(ctx, info, p.InternalPlanWith(info, m, &plan, &tablet))
							if err != nil || conn == nil {
								b.Fatal("no connection", err)
							}
						}
					})
				})
			}
		}
	})
}
