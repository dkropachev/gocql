// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func BenchmarkLatency(b *testing.B) {
	b.Run("PublicationClaims", func(b *testing.B) {
		for _, kind := range []string{"warm", "boundary", "competing-boundary"} {
			b.Run(kind, func(b *testing.B) {
				policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{UpdateInterval: time.Minute}}}
				now := time.Now()
				policy.claimLatencyUpdate(now)
				b.ReportAllocs()
				b.ResetTimer()
				var claims atomic.Int64
				if kind == "competing-boundary" {
					policy.InternalOptions.Latency.UpdateInterval = time.Nanosecond
					policy.updateAfter.Store(nil)
					b.RunParallel(func(pb *testing.PB) {
						var local int64
						for pb.Next() {
							if policy.claimLatencyUpdate(time.Now()) {
								local++
							}
						}
						claims.Add(local)
					})
					b.StopTimer()
					b.ReportMetric(float64(claims.Load())/float64(b.N), "claims/op")
					b.ReportMetric(float64(claims.Load()), "total-claims")
					return
				}
				var local int64
				for i := 0; i < b.N; i++ {
					if kind == "boundary" {
						now = now.Add(2 * time.Minute)
					}
					if policy.claimLatencyUpdate(now) {
						local++
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(local)/float64(b.N), "claims/op")
				b.ReportMetric(float64(local), "total-claims")
			})
		}
	})

	b.Run("ProbeDeadline", func(b *testing.B) {
		for _, test := range deadlineCases() {
			b.Run(fmt.Sprintf("%s/claim-and-reset", test.name), func(b *testing.B) {
				host := cqltype.UUID{15: 1}
				sample := &InternalRoutingLatency{}
				policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{RetryPeriod: test.period}}}
				policy.InternalLatencies.Store(host, sample)
				plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: &InternalRoutingDecisions{InternalSlow: map[cqltype.UUID]time.Time{host: test.now.Add(-time.Second)}}, InternalNow: test.now}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sample.mu.Lock()
					sample.nextProbe = test.now.Add(-time.Nanosecond)
					sample.mu.Unlock()
					if plan.InternalPenalty(Target{HostID: host}) {
						b.Fatal("missing probe")
					}
				}
			})
		}
	})
}
