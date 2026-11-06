// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

var step06BenchmarkTarget model.Target

func BenchmarkStep06FirstTarget(b *testing.B) {
	for _, count := range []int{3, 30, 300} {
		for _, kind := range []string{"ordinary", "lwt", "leader", "latency", "pinned"} {
			b.Run(fmt.Sprintf("hosts%d/%s", count, kind), func(b *testing.B) {
				m := v2TestMetadata()
				routing.SnapshotData(m).InternalHosts = make([]model.HostSnapshot, count)
				for i := range routing.SnapshotData(m).InternalHosts {
					h := v2TestHost(0, "a", "r1")
					binary.BigEndian.PutUint64(h.ID[8:], uint64(i+1))
					routing.SnapshotData(m).InternalHosts[i] = h
				}
				routing.SnapshotData(m).InternalHostIndex = implRouting.InternalBuildRoutingHostIndex(routing.SnapshotData(m).InternalHosts)
				h := step06Hint(b, 0, 100, cql.Option[uint64]{Value: 7, Set: true}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
				root, _ := implRouting.InternalUpdateTabletRange(nil, h)
				routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalRanges: root}, implRouting.InternalComparePlacementTable)
				info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cqltype.Quorum}
				options := implRouting.RoutingOptions{TokenAware: true, Datacenter: "a", Rack: "r1", ShuffleReplicas: true, LeaderAware: kind == "leader"}
				if kind == "lwt" {
					info.Conditional = true
				}
				if kind == "leader" {
					routing.SnapshotData(m).InternalKeyspaces["ks"].ConsistencyMode = "global"
				}
				if kind == "latency" {
					options.Latency.Enabled = true
				}
				if kind == "pinned" {
					info.HostID = cql.Option[cqltype.UUID]{Value: routing.SnapshotData(m).InternalHosts[0].ID, Set: true}
				}
				p := codecTestMust(implRouting.NewRoutingPolicy(options))
				if kind == "latency" {
					p.(*implRouting.InternalRoutingPolicy).InternalDecisions.Store(&implRouting.InternalRoutingDecisions{InternalSlow: map[cqltype.UUID]time.Time{routing.SnapshotData(m).InternalHosts[2].ID: time.Now().Add(time.Hour)}})
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
					step06BenchmarkTarget = target
				}
			})
		}
	}
}
func BenchmarkStep06PlacementUpdate(b *testing.B) {
	for _, tables := range []int{1, 1024} {
		for _, ranges := range []int{1, 1024, 16384} {
			for _, duplicate := range []bool{false, true} {
				b.Run(fmt.Sprintf("tables%d/ranges%d/duplicate%t", tables, ranges, duplicate), func(b *testing.B) {
					s := step06Session(b)
					p := InternalSessionPeripheral(s)
					m := s.Metadata()
					var root *implRouting.InternalTabletRangeTree
					for i := 0; i < ranges; i++ {
						root, _ = implRouting.InternalUpdateTabletRange(root, step06Hint(b, int64(i*2), int64(i*2+1), cql.Option[uint64]{Value: uint64(i), Set: true}, hTarget(1, 0)))
					}
					k := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
					routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, k, implRouting.InternalTabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}, implRouting.InternalComparePlacementTable)
					for i := 1; i < tables; i++ {
						other := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: fmt.Sprintf("other%d", i)}
						routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, other, implRouting.InternalTabletPlacement{InternalRanges: root}, implRouting.InternalComparePlacementTable)
					}
					info := model.RoutingInfo{Keyspace: "ks", Table: "table"}
					h := step06Hint(b, 0, 1, cql.Option[uint64]{Value: 0, Set: true}, hTarget(1, 0))
					updates := make(map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback, 1)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						current := p.InternalSnapshot.Load()
						stamp := InternalSessionCaptureTabletFeedback(s, info, current)
						r, _ := routing.InternalMetadataSnapshotTabletRoot(current, k)
						InternalSessionResolveTabletFeedback(&stamp, k)
						stamp.InternalSnapshot = nil
						if !duplicate {
							h = h.WithVersion(uint64(i + 1))
						}
						updates[InternalV2TabletFeedbackKey{InternalKeyspace: "ks", InternalTable: "table", InternalEnd: 1}] = InternalV2PendingTabletFeedback{InternalHint: h, InternalStamp: stamp, InternalObservation: implRouting.InternalObserveTabletRanges(r, 0, 1)}
						InternalSessionPublishTabletFeedback(s, p, updates, false)
					}
				})
			}
		}
	}
}
func BenchmarkStep06FeedbackQueue(b *testing.B) {
	for _, ranges := range []int{1, 1024} {
		b.Run(fmt.Sprintf("hints%d", ranges), func(b *testing.B) {
			s := step06Session(b)
			info := model.RoutingInfo{Keyspace: "ks", Table: "table"}
			stamp := InternalSessionCaptureTabletFeedback(s, info, s.Metadata())
			hints := make([]InternalV2PendingTabletFeedback, ranges)
			for i := range hints {
				hints[i].InternalHint = step06Hint(b, int64(i*2), int64(i*2+1), cql.Option[uint64]{}, hTarget(1, 0))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				InternalSessionQueueTabletFeedback(s, hints[i%ranges].InternalHint, stamp)
			}
		})
	}
}

// BenchmarkRoutingFirstRoute builds a worker-owned plan for an ordinary
// statement and takes its first target, the pick every request makes.
func BenchmarkRoutingFirstRoute(b *testing.B) {
	for _, count := range []int{3, 30} {
		for _, rack := range []string{"", "r1"} {
			for _, shuffle := range []bool{true, false} {
				b.Run(fmt.Sprintf("hosts%d/rack=%q/shuffle=%v", count, rack, shuffle), func(b *testing.B) {
					m := v2TestMetadata()
					routing.SnapshotData(m).InternalHosts = make([]model.HostSnapshot, count)
					for i := range routing.SnapshotData(m).InternalHosts {
						h := v2TestHost(0, "a", fmt.Sprintf("r%d", i%3+1))
						binary.BigEndian.PutUint64(h.ID[8:], uint64(i+1))
						routing.SnapshotData(m).InternalHosts[i] = h
					}
					routing.SnapshotData(m).InternalHostIndex = implRouting.InternalBuildRoutingHostIndex(routing.SnapshotData(m).InternalHosts)
					h := step06Hint(b, 0, 100, cql.Option[uint64]{}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
					root, _ := implRouting.InternalUpdateTabletRange(nil, h)
					routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}, implRouting.InternalTabletPlacement{InternalRanges: root}, implRouting.InternalComparePlacementTable)
					info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cqltype.Quorum, Kind: routing.OrdinaryStatement}
					p := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true, Datacenter: "a", Rack: rack, ShuffleReplicas: shuffle})).(*implRouting.InternalRoutingPolicy)
					var storage routing.InternalLazyRoutingPlan
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						target, ok := implRouting.InternalNextTarget(p.InternalPlanInto(info, m, &storage))
						if !ok {
							b.Fatal("no target")
						}
						step06BenchmarkTarget = target
					}
				})
			}
		}
	}
}
