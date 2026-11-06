// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/cqltype"

	"github.com/scylladb/gocql/v2/internal/model"

	"testing"

	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

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
						info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Value: tokens[i&4095], Set: true}, Consistency: cqltype.Quorum, Kind: cqltype.OrdinaryStatement}
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
