// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/tablets"

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

func step06Payload(h tablets.TabletInfo) []byte {
	field := func(dst, b []byte) []byte { return append(binary.BigEndian.AppendUint32(dst, uint32(len(b))), b...) }
	var b []byte
	b = field(b, binary.BigEndian.AppendUint64(nil, uint64(h.FirstToken())))
	b = field(b, binary.BigEndian.AppendUint64(nil, uint64(h.LastToken())))
	list := binary.BigEndian.AppendUint32(nil, uint32(len(h.ReplicasUnsafe())))
	for _, r := range h.ReplicasUnsafe() {
		id := r.HostUUIDValue()
		tuple := field(nil, id[:])
		tuple = field(tuple, binary.BigEndian.AppendUint32(nil, uint32(r.ShardID())))
		list = field(list, tuple)
	}
	b = field(b, list)
	if v, ok := h.Version(); ok {
		b = field(b, binary.BigEndian.AppendUint64(nil, v))
	}
	return b
}

// BenchmarkTabletLookup does an execution's tablet work for a token-aware
// request: it captures the feedback stamp, picks the tablet version block
// and finds the replicas of the first target, among tables tables of
// tablets tablets each, at tokens spread over the ring.
func BenchmarkTabletLookup(b *testing.B) {
	for _, tables := range []int{1, 64} {
		for _, count := range []int{16, 1024, 16384} {
			b.Run(fmt.Sprintf("tables%d/tablets%d", tables, count), func(b *testing.B) {
				s := step06Session(b)
				m := s.Metadata()
				step := uint64(math.MaxUint64) / uint64(count)
				var root *implRouting.InternalTabletRangeTree
				for i := 0; i < count; i++ {
					first := math.MinInt64 + int64(uint64(i)*step)
					root, _ = implRouting.InternalUpdateTabletRange(root, step06Hint(b, first, first+int64(step), cql.Option[uint64]{Value: uint64(i), Set: true}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0)))
				}
				k := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
				routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, k, routing.InternalIndexTabletPlacement(implRouting.InternalTabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}), implRouting.InternalComparePlacementTable)
				for i := 1; i < tables; i++ {
					other := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: fmt.Sprintf("other%d", i)}
					routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, other, routing.InternalIndexTabletPlacement(implRouting.InternalTabletPlacement{InternalRanges: root}), implRouting.InternalComparePlacementTable)
				}
				p := codecTestMust(implRouting.NewRoutingPolicy(implRouting.RoutingOptions{TokenAware: true})).(*routing.InternalRoutingPolicy)
				var plan routing.InternalLazyRoutingPlan
				// The request is a prepared statement's, whose placement cache
				// finds its table.
				var cache routing.InternalPlacementCache
				tokens := make([]int64, 4096)
				for i := range tokens {
					tokens[i] = int64(uint64(i) * 0x9e3779b97f4a7c15)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: cqltype.Quorum}
					stamp := InternalSessionCaptureTabletFeedback(s, info, m)
					tablet := routing.InternalLookupTabletCached(m, info, &cache)
					block := tablet.VersionBlock(info)
					target, ok := implRouting.InternalNextTarget(p.InternalPlanWith(info, m, &plan, &tablet))
					if !ok || !stamp.InternalValid {
						b.Fatal("no target")
					}
					step06BenchmarkTarget, step06BenchmarkBlock = target, block
				}
			})
		}
	}
}

var step06BenchmarkBlock byte

// BenchmarkTabletFeedbackCaptureParallel captures the tablet feedback stamp
// of a request from every benchmark goroutine at once, as concurrent
// executions do before sending.
func BenchmarkTabletFeedbackCaptureParallel(b *testing.B) {
	s := step06Session(b)
	m := s.Metadata()
	info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cqltype.Quorum}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !InternalSessionCaptureTabletFeedback(s, info, m).InternalValid {
				b.Fatal("stale stamp")
			}
		}
	})
}

// BenchmarkTabletHintParse parses a tablets-routing-v2 custom payload of a
// tablet with three replicas, as each response carrying a hint does.
func BenchmarkTabletHintParse(b *testing.B) {
	h := step06Hint(b, -100, 100, cql.Option[uint64]{Value: 7, Set: true}, hTarget(1, 0), hTarget(2, 1), hTarget(3, 2))
	raw := step06Payload(h)
	value := cqltype.BorrowBytes(raw)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := transport.InternalParseTypedTabletPayload(value, "ks", "table", true); err != nil {
			b.Fatal(err)
		}
	}
}
