// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"fmt"
	"math"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

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
				var root *tabletRangeTree
				for i := 0; i < count; i++ {
					first := math.MinInt64 + int64(uint64(i)*step)
					root, _ = updateTabletRange(root, step06Hint(b, first, first+int64(step), Option[uint64]{Value: uint64(i), Set: true}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0)))
				}
				k := metadataTable{InternalKeyspace: "ks", InternalTable: "table"}
				routing.SnapshotData(m).InternalPlacements = placementPut(routing.SnapshotData(m).InternalPlacements, k, routing.InternalIndexTabletPlacement(tabletPlacement{InternalIdentity: "identity-1", InternalRanges: root}), comparePlacementTable)
				for i := 1; i < tables; i++ {
					other := metadataTable{InternalKeyspace: "ks", InternalTable: fmt.Sprintf("other%d", i)}
					routing.SnapshotData(m).InternalPlacements = placementPut(routing.SnapshotData(m).InternalPlacements, other, routing.InternalIndexTabletPlacement(tabletPlacement{InternalRanges: root}), comparePlacementTable)
				}
				p := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true})).(*routing.InternalRoutingPolicy)
				var plan routing.InternalLazyRoutingPlan
				tokens := make([]int64, 4096)
				for i := range tokens {
					tokens[i] = int64(uint64(i) * 0x9e3779b97f4a7c15)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: Quorum}
					stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, m)
					tablet := routing.InternalLookupTablet(m, info)
					block := tablet.VersionBlock(info)
					target, ok := p.InternalPlanWith(info, m, &plan, &tablet).Next()
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
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: 1, Set: true}, Consistency: Quorum}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !implCluster.InternalSessionCaptureTabletFeedback(s, info, m).InternalValid {
				b.Fatal("stale stamp")
			}
		}
	})
}
