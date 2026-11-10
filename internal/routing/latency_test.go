// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

func TestLatency(t *testing.T) {
	t.Run("ProbeDeadline", func(t *testing.T) {
		t.Run("Range", func(t *testing.T) {
			for _, test := range deadlineCases() {
				t.Run(test.name, func(t *testing.T) {
					policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, Scale: time.Millisecond, RetryPeriod: test.period, UpdateInterval: time.Hour}}}
					host := cqltype.UUID{15: 1}
					sample := &InternalRoutingLatency{nextProbe: test.now.Add(-time.Nanosecond)}
					policy.InternalLatencies.Store(host, sample)
					decisions := &InternalRoutingDecisions{InternalSlow: map[cqltype.UUID]time.Time{host: test.now.Add(-time.Nanosecond)}}
					start := make(chan struct{})
					var group sync.WaitGroup
					var probes atomic.Int32
					for range 32 {
						group.Add(1)
						go func() {
							defer group.Done()
							<-start
							plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: decisions, InternalNow: test.now}
							if !plan.InternalPenalty(Target{HostID: host}) {
								probes.Add(1)
							}
						}()
					}
					close(start)
					group.Wait()
					if probes.Load() != 1 {
						t.Fatal("concurrent probes", probes.Load())
					}
					sample.mu.Lock()
					deadline := sample.nextProbe
					sample.mu.Unlock()
					if !deadline.Equal(test.now.Add(test.period)) {
						t.Fatal("configured duration narrowed", deadline, test.now.Add(test.period))
					}
					// A backwards clock cannot expire a claim. An abandoned claim expires
					// at the exact configured time, including dates beyond UnixNano's range.
					plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: decisions, InternalNow: test.now.Add(-time.Second)}
					if !plan.InternalPenalty(Target{HostID: host}) {
						t.Fatal("clock reversal expired claim")
					}
					plan.InternalNow = deadline.Add(-time.Nanosecond)
					if !plan.InternalPenalty(Target{HostID: host}) {
						t.Fatal("claim expired early")
					}
					plan.InternalNow = deadline
					if plan.InternalPenalty(Target{HostID: host}) {
						t.Fatal("abandoned claim did not expire")
					}
					policy.Observe(AttemptEvent{Target: Target{HostID: host}, Started: test.now, Latency: time.Millisecond})
					sample.mu.Lock()
					observed := sample.nextProbe
					sample.mu.Unlock()
					want := test.now.Add(time.Millisecond).Add(test.period)
					if !observed.Equal(want) {
						t.Fatal("Observe deadline narrowed", observed, want)
					}
				})
			}
		})
	})

	t.Run("PublicationDeadline", func(t *testing.T) {
		t.Run("Range", func(t *testing.T) {
			for _, test := range deadlineCases() {
				t.Run(test.name, func(t *testing.T) {
					policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{UpdateInterval: test.period}}}
					start := make(chan struct{})
					var group sync.WaitGroup
					var claims atomic.Int32
					for range 32 {
						group.Add(1)
						go func() {
							defer group.Done()
							<-start
							if policy.claimLatencyUpdate(test.now) {
								claims.Add(1)
							}
						}()
					}
					close(start)
					group.Wait()
					if claims.Load() != 1 {
						t.Fatal("concurrent publication claims", claims.Load())
					}
					winner := policy.updateAfter.Load()
					if winner == nil || !winner.Equal(test.now.Add(test.period)) {
						t.Fatal("publication deadline narrowed", winner)
					}
					for _, clock := range []time.Time{test.now.Add(-time.Hour), test.now, test.now.Add(test.period).Add(-time.Nanosecond)} {
						if policy.claimLatencyUpdate(clock) {
							t.Fatal("early/backwards publication claim", clock)
						}
						if policy.updateAfter.Load() != winner {
							t.Fatal("losing claim replaced winner")
						}
					}
					if !policy.claimLatencyUpdate(*winner) {
						t.Fatal("publication did not become due")
					}
					policy.updateAfter.Store(nil)
					if !policy.claimLatencyUpdate(test.now) {
						t.Fatal("new minimum could not reschedule publication")
					}
				})
			}
		})
	})

	t.Run("ClaimLatencyUpdate", func(t *testing.T) {
		t.Run("WarmAllocations", func(t *testing.T) {
			now := time.Now()
			policy := &InternalRoutingPolicy{InternalOptions: RoutingOptions{Latency: LatencyOptions{RetryPeriod: time.Minute, UpdateInterval: time.Hour}}}
			if !policy.claimLatencyUpdate(now) {
				t.Fatal("initial publication claim")
			}
			if got := testing.AllocsPerRun(1000, func() {
				if policy.claimLatencyUpdate(now) {
					t.Fatal("warm claim succeeded")
				}
			}); got != 0 {
				t.Fatal("warm publication allocated", got)
			}
			host := cqltype.UUID{15: 1}
			sample := &InternalRoutingLatency{nextProbe: now.Add(time.Minute)}
			policy.InternalLatencies.Store(host, sample)
			plan := &InternalLazyRoutingPlan{policy: policy, InternalSlow: &InternalRoutingDecisions{InternalSlow: map[cqltype.UUID]time.Time{host: now.Add(-time.Second)}}, InternalNow: now}
			if got := testing.AllocsPerRun(1000, func() {
				if !plan.InternalPenalty(Target{HostID: host}) {
					t.Fatal("held probe claim lost")
				}
			}); got != 0 {
				t.Fatal("held probe check allocated", got)
			}
			if got := testing.AllocsPerRun(1000, func() {
				sample.mu.Lock()
				sample.nextProbe = now.Add(-time.Nanosecond)
				sample.mu.Unlock()
				if plan.InternalPenalty(Target{HostID: host}) {
					t.Fatal("expired probe not claimed")
				}
			}); got != 0 {
				t.Fatal("expired probe selection allocated", got)
			}
		})
	})

	t.Run("Probe", func(t *testing.T) {
		t.Run("Reservation", func(t *testing.T) {
			policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: time.Minute, UpdateInterval: time.Hour}})).(*InternalRoutingPolicy)
			metadata := v2TestMetadata()
			slow := cqltype.UUID{15: 1}
			healthy := cqltype.UUID{15: 2}
			// Exactly one replica keeps first-target selection deterministic. A healthy
			// nonreplica must be preferred while another plan owns the retry probe.
			SnapshotData(metadata).InternalRanges["ks"][0].Replicas = []model.Target{{HostID: slow}}
			policy.Observe(AttemptEvent{Target: model.Target{HostID: healthy}, Started: time.Now().Add(-2 * time.Minute), Latency: time.Millisecond})
			policy.Observe(AttemptEvent{Target: model.Target{HostID: slow}, Started: time.Now().Add(-2 * time.Minute), Latency: 10 * time.Millisecond})
			// Publish both real samples; the regular hourly updater is deliberately idle.
			policy.InternalDecisions.Store(&InternalRoutingDecisions{InternalSlow: map[cqltype.UUID]time.Time{slow: time.Now().Add(-time.Minute)}})
			info := model.RoutingInfo{Keyspace: "ks", Table: "table", Token: cqltype.Option[int64]{Set: true}, Kind: cqltype.OrdinaryStatement}
			// Constructing an abandoned plan must not reserve a probe.
			_ = NewPlan(policy, info, metadata)
			start := make(chan struct{})
			var group sync.WaitGroup
			var probes atomic.Int32
			for range 32 {
				group.Add(1)
				go func() {
					defer group.Done()
					<-start
					target, ok := InternalNextTarget(NewPlan(policy, info, metadata))
					if !ok {
						t.Error("missing target")
						return
					}
					if target.HostID == slow {
						probes.Add(1)
					}
				}()
			}
			close(start)
			group.Wait()
			if got := probes.Load(); got != 1 {
				t.Fatalf("expired slow replica got %d concurrent first-target probes; want one", got)
			}
			// An emitted target abandoned before Observe cannot lock out future probes.
			// Advancing the plan clock avoids a timing-dependent minute-long test.
			future := time.Now().Add(2 * time.Minute)
			for attempt := 0; attempt < 2; attempt++ {
				plan := NewPlan(policy, info, metadata).(*InternalLazyRoutingPlan)
				plan.InternalNow = future
				target, ok := InternalNextTarget(plan)
				if !ok || (target.HostID == slow) != (attempt == 0) {
					t.Fatal("abandoned probe did not expire/reserve once", attempt, target, ok)
				}
			}
			// A completed probe restarts its retry interval even before the decision
			// updater publishes again; sharing a policy shares the reservation state.
			policy.Observe(AttemptEvent{Target: model.Target{HostID: slow}, Started: future, Latency: 10 * time.Millisecond})
			plan := NewPlan(policy, info, metadata).(*InternalLazyRoutingPlan)
			plan.InternalNow = future.Add(20 * time.Millisecond)
			if target, ok := InternalNextTarget(plan); !ok || target.HostID == slow {
				t.Fatal("completed probe did not restart exclusion", target, ok)
			}
		})
	})

	t.Run("Plan", func(t *testing.T) {
		t.Run("DeprioritizesAndRetries", func(t *testing.T) {
			policy, _ := NewRoutingPolicy(RoutingOptions{Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: time.Hour}})
			p := policy.(*InternalRoutingPolicy)
			m := v2TestMetadata()
			for i, h := range SnapshotData(m).InternalHosts {
				latency := time.Millisecond
				if i == 0 {
					latency = time.Second
				}
				p.Observe(AttemptEvent{Target: model.Target{HostID: h.ID}, Started: time.Now(), Latency: latency})
			}
			_ = NewPlan(p, model.RoutingInfo{}, m)
			ids := v2PlanIDs(NewPlan(p, model.RoutingInfo{}, m))
			if ids[len(ids)-1] != 1 {
				t.Fatal("slow host was not deferred", ids)
			}
		})
	})
}
