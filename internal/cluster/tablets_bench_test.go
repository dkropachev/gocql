// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"fmt"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func BenchmarkTablets(b *testing.B) {
	// BenchmarkTablets/FeedbackCaptureParallel captures the tablet feedback stamp
	// of a request from every benchmark goroutine at once, as concurrent
	// executions do before sending.
	b.Run("FeedbackCaptureParallel", func(b *testing.B) {
		s := testSession(b)
		m := s.Metadata()
		info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Value: 1, Set: true}, Consistency: cqltype.Quorum}
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if !InternalSessionCaptureTabletFeedback(s, info, m).InternalValid {
					b.Fatal("stale stamp")
				}
			}
		})
	})

	b.Run("FeedbackQueue", func(b *testing.B) {
		for _, ranges := range []int{1, 1024} {
			b.Run(fmt.Sprintf("hints%d", ranges), func(b *testing.B) {
				s := testSession(b)
				info := model.RoutingInfo{Keyspace: "ks", Table: "table"}
				stamp := InternalSessionCaptureTabletFeedback(s, info, s.Metadata())
				hints := make([]InternalV2PendingTabletFeedback, ranges)
				for i := range hints {
					hints[i].InternalHint = testHint(b, int64(i*2), int64(i*2+1), cqltype.Option[uint64]{}, hTarget(1, 0))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					InternalSessionQueueTabletFeedback(s, hints[i%ranges].InternalHint, stamp)
				}
			})
		}
	})

	// BenchmarkTablets/HintParse parses a tablets-routing-v2 custom payload of a
	// tablet with three replicas, as each response carrying a hint does.
	b.Run("HintParse", func(b *testing.B) {
		h := testHint(b, -100, 100, cqltype.Option[uint64]{Value: 7, Set: true}, hTarget(1, 0), hTarget(2, 1), hTarget(3, 2))
		raw := testPayload(h)
		value := cqltype.BorrowBytes(raw)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := transport.InternalParseTypedTabletPayload(value, "ks", "table", true); err != nil {
				b.Fatal(err)
			}
		}
	})

	// BenchmarkTablets/Lookup does an execution's tablet work for a token-aware
	// request: it captures the feedback stamp, picks the tablet version block
	// and finds the replicas of the first target, among tables tables of
	// tablets tablets each, at tokens spread over the ring.
	b.Run("Lookup", func(b *testing.B) {
		for _, tables := range []int{1, 64} {
			for _, count := range []int{16, 1024, 16384} {
				b.Run(fmt.Sprintf("tables%d/tablets%d", tables, count), func(b *testing.B) {
					s := testSession(b)
					m := s.Metadata()
					step := uint64(math.MaxUint64) / uint64(count)
					var root *implRouting.InternalTabletRangeTree
					for i := 0; i < count; i++ {
						first := math.MinInt64 + int64(uint64(i)*step)
						root, _ = implRouting.InternalUpdateTabletRange(root, testHint(b, first, first+int64(step), cqltype.Option[uint64]{Value: uint64(i), Set: true}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0)))
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
						info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: cqltype.Quorum}
						stamp := InternalSessionCaptureTabletFeedback(s, info, m)
						tablet := routing.InternalLookupTabletCached(m, info, &cache)
						block := tablet.VersionBlock(info)
						target, ok := implRouting.InternalNextTarget(p.InternalPlanWith(info, m, &plan, &tablet))
						if !ok || !stamp.InternalValid {
							b.Fatal("no target")
						}
						benchmarkTarget, benchmarkBlock = target, block
					}
				})
			}
		}
	})

	b.Run("PlacementUpdate", func(b *testing.B) {
		for _, tables := range []int{1, 1024} {
			for _, ranges := range []int{1, 1024, 16384} {
				for _, duplicate := range []bool{false, true} {
					b.Run(fmt.Sprintf("tables%d/ranges%d/duplicate%t", tables, ranges, duplicate), func(b *testing.B) {
						s := testSession(b)
						p := InternalSessionPeripheral(s)
						m := s.Metadata()
						var root *implRouting.InternalTabletRangeTree
						for i := 0; i < ranges; i++ {
							root, _ = implRouting.InternalUpdateTabletRange(root, testHint(b, int64(i*2), int64(i*2+1), cqltype.Option[uint64]{Value: uint64(i), Set: true}, hTarget(1, 0)))
						}
						k := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
						routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, k, implRouting.InternalTabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}, implRouting.InternalComparePlacementTable)
						for i := 1; i < tables; i++ {
							other := implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: fmt.Sprintf("other%d", i)}
							routing.SnapshotData(m).InternalPlacements = implRouting.InternalPlacementPut(routing.SnapshotData(m).InternalPlacements, other, implRouting.InternalTabletPlacement{InternalRanges: root}, implRouting.InternalComparePlacementTable)
						}
						info := model.RoutingInfo{Keyspace: "ks", Table: "table"}
						h := testHint(b, 0, 1, cqltype.Option[uint64]{Value: 0, Set: true}, hTarget(1, 0))
						updates := make(map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback, 1)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							current := p.InternalSnapshot.Load()
							stamp := InternalSessionCaptureTabletFeedback(s, info, current)
							r, _ := routing.InternalMetadataSnapshotTabletRoot(current, k)
							sessionResolveTabletFeedback(&stamp, k)
							stamp.InternalSnapshot = nil
							if !duplicate {
								h = h.WithVersion(uint64(i + 1))
							}
							updates[InternalV2TabletFeedbackKey{InternalKeyspace: "ks", InternalTable: "table", InternalEnd: 1}] = InternalV2PendingTabletFeedback{InternalHint: h, InternalStamp: stamp, observation: implRouting.InternalObserveTabletRanges(r, 0, 1)}
							InternalSessionPublishTabletFeedback(s, p, updates, false)
						}
					})
				}
			}
		}
	})
}
