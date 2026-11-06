// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"fmt"
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/transport"
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
