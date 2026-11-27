// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"bytes"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

func TestRoutingPolicy(t *testing.T) {
	t.Run("Plan", func(t *testing.T) {
		t.Run("OrderAndIndependence", func(t *testing.T) {
			m := v2TestMetadata()
			policy, err := NewRoutingPolicy(routing.Options{Datacenter: "a", Rack: "r1", AllowRemote: true, NoLeaderAware: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := policy.Validate(m); err != nil {
				t.Fatal(err)
			}
			info := routing.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Set: true, Value: 1}, Kind: cql.ConditionalStatement}
			a, b := NewPlan(policy, info, m), NewPlan(policy, info, m)
			want := []byte{1, 2, 3, 4}
			if got := v2PlanIDs(a); !bytes.Equal(got, want) {
				t.Fatalf("plan=%v want %v", got, want)
			}
			if got := v2PlanIDs(b); !bytes.Equal(got, want) {
				t.Fatalf("second plan=%v", got)
			}
			for i := 0; i < 20; i++ {
				info.Kind = cql.OrdinaryStatement
				info.Consistency = cql.Serial
				if got := v2PlanIDs(NewPlan(policy, info, m)); !bytes.Equal(got, want) {
					t.Fatal("SERIAL order changed", got)
				}
			}
			SnapshotData(m).InternalHosts[0].Enabled = false
			if got := v2PlanIDs(NewPlan(policy, info, m)); bytes.Contains(got, []byte{1}) {
				t.Fatal("disabled host in plan")
			}
			bad, _ := NewRoutingPolicy(routing.Options{Datacenter: "missing", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true})
			if err := bad.Validate(m); err == nil {
				t.Fatal("missing DC validated")
			}
		})

		t.Run("TabletMissAndStrictShard", func(t *testing.T) {
			m := v2TestMetadata()
			SnapshotData(m).InternalTablets[InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}] = []metadata.TokenRange{{Start: 10, End: 20, Replicas: []metadata.Target{{HostID: SnapshotData(m).InternalHosts[1].HostID, ShardID: cql.Option[uint32]{Value: 2, Set: true}}}}}
			if got, err := m.Replicas("ks", "table", 30); !errors.Is(err, metadata.ErrPlacementUnknown) || len(got) != 0 {
				t.Fatal("tablet miss fell back to vnode", got, err)
			}
			single := SingleTargetRouting(metadata.Target{HostID: SnapshotData(m).InternalHosts[0].HostID, ShardID: cql.Option[uint32]{Value: 9, Set: true}})
			if err := single.Validate(m); err == nil {
				t.Fatal("invalid strict shard accepted")
			}
			if _, ok := nextTarget(NewPlan(single, routing.Info{}, m)); ok {
				t.Fatal("strict shard fell back")
			}
			single = SingleTargetRouting(metadata.Target{HostID: SnapshotData(m).InternalHosts[0].HostID, ShardID: cql.Option[uint32]{Value: 9, Set: true}, AnyShardFallback: true})
			target, ok := nextTarget(NewPlan(single, routing.Info{}, m))
			if !ok || target.ShardID.Set {
				t.Fatal("hint fallback failed", target, ok)
			}
		})

		t.Run("PinnedTargets", func(t *testing.T) {
			m := v2TestMetadata()
			// A host pin overrides locality and availability selection, but never admits
			// an unknown or administratively disabled host and never falls back.
			SnapshotData(m).InternalHosts[1].Enabled = false
			SnapshotData(m).InternalHosts[2].Up = false
			SnapshotData(m).InternalHosts[2].Connected = false
			policy := codecTestMust(NewRoutingPolicy(routing.Options{Datacenter: "a", FixedReplicaOrder: true}))
			tests := []struct {
				host  byte
				shard cql.Option[uint32]
				valid bool
			}{
				{1, cql.Option[uint32]{}, true},
				{3, cql.Option[uint32]{Value: 0, Set: true}, true},
				{1, cql.Option[uint32]{Value: 3, Set: true}, true},
				{2, cql.Option[uint32]{}, false},
				{99, cql.Option[uint32]{Value: 1, Set: true}, false},
			}
			plans := make([]QueryPlan, len(tests))
			info := routing.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Set: true, Value: 1}, Consistency: cql.LocalQuorum}
			for i, test := range tests {
				info.HostID = cql.Option[cql.UUID]{Value: cql.UUID{15: test.host}, Set: true}
				info.ShardID = test.shard
				plans[i] = NewPlan(policy, info, m)
			}
			// Consume in reverse after all plans have been created, including different
			// shards on the same host. Each plan must retain its own exact destination.
			for i := len(plans) - 1; i >= 0; i-- {
				test := tests[i]
				target, ok := nextTarget(plans[i])
				want := metadata.Target{HostID: cql.UUID{15: test.host}, ShardID: test.shard}
				if ok != test.valid || ok && target != want {
					t.Fatalf("host %d shard %+v: got %+v,%t; want %+v,%t", test.host, test.shard, target, ok, want, test.valid)
				}
				if target, ok := nextTarget(plans[i]); ok {
					t.Fatalf("pinned host %d fell back to %+v", test.host, target)
				}
			}
		})
	})

	t.Run("Pick", func(t *testing.T) {
		t.Run("FirstRoute", func(t *testing.T) {
			m := v2TestMetadata()
			info := routing.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cql.Quorum, Kind: cql.OrdinaryStatement}
			t.Run("rack replica first", func(t *testing.T) {
				p := codecTestMust(NewRoutingPolicy(routing.Options{Datacenter: "a", Rack: "r1", AllowRemote: true, NoLeaderAware: true})).(*InternalRoutingPolicy)
				var storage InternalLazyRoutingPlan
				for range 200 {
					hosts := planHosts(p.InternalPlanInto(info, m, &storage))
					if len(hosts) != 4 || hosts[0] != 1 || hosts[1] != 2 || hosts[2] != 3 || hosts[3] != 4 {
						t.Fatalf("plan %v, want [1 2 3 4]", hosts)
					}
				}
			})
			t.Run("uniform in datacenter", func(t *testing.T) {
				p := codecTestMust(NewRoutingPolicy(routing.Options{Datacenter: "a", NoLeaderAware: true})).(*InternalRoutingPolicy)
				var storage InternalLazyRoutingPlan
				var first [5]int
				const plans = 4000
				for range plans {
					hosts := planHosts(p.InternalPlanInto(info, m, &storage))
					if len(hosts) != 3 || hosts[0]+hosts[1] != 3 || hosts[0] == hosts[1] || hosts[2] != 4 {
						t.Fatalf("plan %v, want replicas 1 and 2 in either order, then 4", hosts)
					}
					first[hosts[0]]++
				}
				if first[1] < plans*4/10 || first[2] < plans*4/10 {
					t.Fatalf("first replicas %v are not uniform", first)
				}
			})
			t.Run("down replica is skipped", func(t *testing.T) {
				m := v2TestMetadata()
				SnapshotData(m).InternalHosts[0].Up = false
				p := codecTestMust(NewRoutingPolicy(routing.Options{Datacenter: "a", Rack: "r1", NoLeaderAware: true})).(*InternalRoutingPolicy)
				var storage InternalLazyRoutingPlan
				for range 200 {
					hosts := planHosts(p.InternalPlanInto(info, m, &storage))
					if len(hosts) != 3 || hosts[0] != 2 || hosts[1] != 4 || hosts[2] != 1 {
						t.Fatalf("plan %v, want [2 4 1]", hosts)
					}
				}
			})
		})

		t.Run("ThenFallback", func(t *testing.T) {
			m := v2TestMetadata()
			hosts := m.HostRoutes()
			t.Run("pick first, fallback on demand", func(t *testing.T) {
				p := &pickFallbackPolicy{pick: cql.Option[Route]{Value: hosts[1], Set: true}, fallback: []Route{hosts[0], hosts[1], hosts[2], hosts[1]}}
				plan := NewPlan(p, routing.Info{}, m)
				if r, ok := plan.Next(); !ok || r != hosts[1] || p.fallbacks != 0 {
					t.Fatalf("first route %v %v after %d fallbacks, want the pick and no fallback", r.HostID(), ok, p.fallbacks)
				}
				var got []Route
				for r, ok := plan.Next(); ok; r, ok = plan.Next() {
					got = append(got, r)
				}
				if p.fallbacks != 1 || len(got) != 2 || got[0] != hosts[0] || got[1] != hosts[2] {
					t.Fatalf("fallback routes %v after %d fallbacks, want hosts 0 and 2 without the pick", got, p.fallbacks)
				}
			})
			t.Run("no pick", func(t *testing.T) {
				p := &pickFallbackPolicy{fallback: []Route{hosts[2], hosts[0]}}
				plan := NewPlan(p, routing.Info{}, m)
				a, _ := plan.Next()
				b, _ := plan.Next()
				if _, ok := plan.Next(); ok || a != hosts[2] || b != hosts[0] || p.fallbacks != 1 {
					t.Fatalf("plan %v %v, want the fallback routes in order", a.HostID(), b.HostID())
				}
			})
			t.Run("shard of the picked host", func(t *testing.T) {
				p := &pickFallbackPolicy{pick: cql.Option[Route]{Value: hosts[0].WithShard(1), Set: true}, fallback: []Route{hosts[0].WithShard(1), hosts[0]}}
				plan := NewPlan(p, routing.Info{}, m)
				plan.Next()
				if r, ok := plan.Next(); !ok || r != hosts[0] {
					t.Fatalf("second route %v shard %d, want any shard of the picked host", r.HostID(), r.shard)
				}
			})
		})

		t.Run("BuiltinFallback", func(t *testing.T) {
			m := v2TestMetadata()
			info := routing.Info{Keyspace: "ks", Table: "table", Token: cql.Option[int64]{Value: 1, Set: true}, Consistency: cql.Quorum, Kind: cql.OrdinaryStatement}
			p := codecTestMust(NewRoutingPolicy(routing.Options{Datacenter: "a", Rack: "r1", NoLeaderAware: true}))
			picked := 0
			for range 200 {
				if r, ok := p.Pick(info, m); ok {
					picked++
					if r.HostID()[15] != 1 {
						t.Fatalf("picked host %v, want the local-rack replica 1", r.HostID())
					}
				}
			}
			if picked == 0 {
				t.Fatal("Pick never routed to the local-rack replica")
			}
			if allocs := testing.AllocsPerRun(100, func() { p.Pick(info, m) }); allocs != 0 {
				t.Fatalf("Pick allocates %v times", allocs)
			}
			if hosts := planHosts(p.Fallback(info, m)); len(hosts) != 3 || hosts[0] != 1 || hosts[1] != 2 || hosts[2] != 4 {
				t.Fatalf("fallback %v, want [1 2 4]", hosts)
			}
			conditional := info
			conditional.Kind = cql.ConditionalStatement
			if _, ok := p.Pick(conditional, m); ok {
				t.Fatal("Pick routed a conditional request, which keeps replica order")
			}
		})
	})
}
