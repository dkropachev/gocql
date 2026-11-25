// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing_test

import (
	"slices"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/tablets"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// The options fixture has five hosts:
//
//	1 a/r1   2 a/r2   3 b/r3   4 a/r1   5 b/r4
//
// Table ks.t is placed by vnodes: every token's replicas are 3, 2 and 1.
// Table ks.tt is placed by tablets: (0, 100] on 4 shard 2 and 5 shard 1.

func optionsHost(id byte, dc, rack string) metadata.HostSnapshot {
	return metadata.HostSnapshot{HostID: cql.UUID{15: id}, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}

func optionsTarget(id byte, shard uint32) metadata.Target {
	return metadata.Target{HostID: cql.UUID{15: id}, ShardID: cql.Some(shard)}
}

// tabletHint is a server's tablet hint for ks.tt.
func tabletHint(t *testing.T, first, last int64, replicas ...metadata.Target) tablets.TabletInfo {
	t.Helper()
	rs := make([]tablets.ReplicaInfo, len(replicas))
	for i, r := range replicas {
		rs[i] = tablets.NewReplicaInfo(tablets.HostUUID(r.HostID), int(r.ShardID.Value))
	}
	h, err := tablets.NewTabletInfo("ks", "tt", first, last, rs)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

var tabletTable = implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "tt"}

// optionsSnapshot is the fixture, with change applied to its hosts first.
func optionsSnapshot(t *testing.T, change func(hosts []metadata.HostSnapshot)) *implRouting.MetadataSnapshot {
	hosts := []metadata.HostSnapshot{optionsHost(1, "a", "r1"), optionsHost(2, "a", "r2"), optionsHost(3, "b", "r3"), optionsHost(4, "a", "r1"), optionsHost(5, "b", "r4")}
	if change != nil {
		change(hosts)
	}
	ranges, _ := implRouting.InternalUpdateTabletRange(nil, tabletHint(t, 0, 100, optionsTarget(4, 2), optionsTarget(5, 1)))
	state := implRouting.SnapshotState{
		InternalHosts:     hosts,
		InternalHostIndex: implRouting.InternalBuildRoutingHostIndex(hosts),
		InternalKeyspaces: map[string]*metadata.Keyspace{"ks": {Name: "ks", Tables: map[string]*metadata.Table{"t": {Name: "t"}, "tt": {Name: "tt"}}}},
		InternalRanges:    map[string][]metadata.TokenRange{"ks": {{Start: 0, End: 0, Replicas: []metadata.Target{{HostID: cql.UUID{15: 3}}, {HostID: cql.UUID{15: 2}}, {HostID: cql.UUID{15: 1}}}}}},
		InternalTablets:   map[implRouting.InternalMetadataTable][]metadata.TokenRange{},
	}
	state.InternalPlacements = state.InternalPlacements.Put(tabletTable, implRouting.InternalIndexTabletPlacement(implRouting.InternalTabletPlacement{InternalRanges: ranges}))
	return implRouting.NewMetadataSnapshot(state)
}

// planRoutes lists the routes of a plan of policy, as host and shard, the
// shard -1 for any shard.
func planRoutes(policy routing.Policy, info routing.Info, m *implRouting.MetadataSnapshot) [][2]int {
	var routes [][2]int
	plan := implRouting.PlanFor(policy, info, m.Public())
	for r, ok := plan.Next(); ok; r, ok = plan.Next() {
		routes = append(routes, routePair(r))
	}
	return routes
}

// routePair is r's host and shard, the shard -1 for any shard.
func routePair(r metadata.Route) [2]int {
	shard := -1
	if s := r.ShardID(); s.Set {
		shard = int(s.Value)
	}
	return [2]int{int(r.HostID()[15]), shard}
}

// policyHosts is the hosts a plan of policy routes to, in order, without the
// route to any shard of a host that follows a route to one of its shards.
func policyHosts(policy routing.Policy, info routing.Info, m *implRouting.MetadataSnapshot) []byte {
	var hosts []byte
	plan := implRouting.NewPlan(implRouting.EngineOf(policy), info, m)
	for target, ok := implRouting.InternalNextTarget(plan); ok; target, ok = implRouting.InternalNextTarget(plan) {
		hosts = append(hosts, target.HostID[15])
	}
	return hosts
}

// inTiers reports whether hosts are the hosts of each tier in turn, in any
// order within a tier.
func inTiers(hosts []byte, tiers [][]byte) bool {
	for _, tier := range tiers {
		if len(hosts) < len(tier) {
			return false
		}
		got, want := slices.Clone(hosts[:len(tier)]), slices.Clone(tier)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			return false
		}
		hosts = hosts[len(tier):]
	}
	return len(hosts) == 0
}

// withoutHost returns tiers without host, dropping tiers left empty.
func withoutHost(tiers [][]byte, host byte) [][]byte {
	var out [][]byte
	for _, tier := range tiers {
		if tier = slices.DeleteFunc(slices.Clone(tier), func(h byte) bool { return h == host }); len(tier) > 0 {
			out = append(out, tier)
		}
	}
	return out
}

// TestPolicyOptions pins the order of the plans NewPolicy's options produce:
// for each option set, the hosts of each tier come before those of the next.
// Each case also runs with host 1 filtered out by the host filter, which
// drops it from the plan and changes nothing else, and with host 1 measured
// slow by latency-aware routing, which moves it after every other host.
func TestPolicyOptions(t *testing.T) {
	vnode := routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](1), Kind: cql.OrdinaryStatement, Consistency: cql.LocalQuorum}
	tablet := vnode
	tablet.Table, tablet.Token = "tt", cql.Some[int64](50)
	outside := tablet
	outside.Token = cql.Some[int64](200)
	noToken := vnode
	noToken.Token = cql.Option[int64]{}

	for _, test := range []struct {
		name    string
		options routing.Options
		info    routing.Info
		tiers   [][]byte
	}{
		{"none", routing.Options{NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 2, 3, 4, 5}}},
		{"token aware", routing.Options{FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 2, 3}, {4, 5}}},
		{"token aware without token", routing.Options{FixedReplicaOrder: true, NoLeaderAware: true}, noToken, [][]byte{{1, 2, 3, 4, 5}}},
		{"datacenter", routing.Options{Datacenter: "a", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 2, 4}}},
		{"datacenter token aware", routing.Options{Datacenter: "a", FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 2}, {4}}},
		{"rack", routing.Options{Datacenter: "a", Rack: "r1", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 4}, {2}}},
		{"rack token aware", routing.Options{Datacenter: "a", Rack: "r1", FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1}, {2}, {4}}},
		{"remote", routing.Options{Datacenter: "a", AllowRemote: true, NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 2, 4}, {3, 5}}},
		// A remote replica comes before a local host that is no replica.
		{"remote token aware", routing.Options{Datacenter: "a", AllowRemote: true, FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1, 2}, {3}, {4}, {5}}},
		{"rack remote token aware", routing.Options{Datacenter: "a", Rack: "r1", AllowRemote: true, FixedReplicaOrder: true, NoLeaderAware: true}, vnode, [][]byte{{1}, {2}, {3}, {4}, {5}}},
		// Tablets place ks.tt, not the vnode ring, whose replicas are 1, 2 and 3.
		{"tablet", routing.Options{Datacenter: "a", AllowRemote: true, FixedReplicaOrder: true, NoLeaderAware: true}, tablet, [][]byte{{4}, {5}, {1, 2}, {3}}},
		{"tablet rack", routing.Options{Datacenter: "a", Rack: "r1", FixedReplicaOrder: true, NoLeaderAware: true}, tablet, [][]byte{{4}, {1}, {2}}},
		{"tablet not token aware", routing.Options{Datacenter: "a", AllowRemote: true, NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, tablet, [][]byte{{1, 2, 4}, {3, 5}}},
		// A token no known tablet holds has no replicas: it does not fall
		// back to the vnode ring.
		{"tablet unknown", routing.Options{Datacenter: "a", AllowRemote: true, FixedReplicaOrder: true, NoLeaderAware: true}, outside, [][]byte{{1, 2, 4}, {3, 5}}},
	} {
		for _, variant := range []struct {
			name  string
			hosts func([]metadata.HostSnapshot)
			slow  bool
			tiers [][]byte
		}{
			{"", nil, false, test.tiers},
			{"/filtered", func(h []metadata.HostSnapshot) { h[0].Enabled = false }, false, withoutHost(test.tiers, 1)},
			{"/slow", nil, true, append(withoutHost(test.tiers, 1), []byte{1})},
		} {
			for _, shuffle := range []bool{false, true} {
				name := test.name + variant.name
				if shuffle {
					name += "/shuffled"
				}
				t.Run(name, func(t *testing.T) {
					options := test.options
					options.FixedReplicaOrder = !shuffle
					if variant.slow {
						options.Latency = routing.LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: time.Hour, UpdateInterval: time.Hour}
					}
					policy, err := routing.NewPolicy(options)
					if err != nil {
						t.Fatal(err)
					}
					m := optionsSnapshot(t, variant.hosts)
					if err := policy.Validate(m.Public()); err != nil {
						t.Fatal(err)
					}
					if variant.slow {
						// Host 1 is measured first; the faster host 2 then
						// publishes the decision that host 1 is slow.
						start := time.Now()
						policy.Learn(routing.AttemptEvent{Target: metadata.Target{HostID: cql.UUID{15: 1}}, Started: start, Latency: time.Second})
						policy.Learn(routing.AttemptEvent{Target: metadata.Target{HostID: cql.UUID{15: 2}}, Started: start, Latency: time.Millisecond})
					}
					for range 20 {
						if hosts := policyHosts(policy, test.info, m); !inTiers(hosts, variant.tiers) {
							t.Fatalf("plan %v, want tiers %v", hosts, variant.tiers)
						}
					}
				})
			}
		}
	}

	t.Run("ReplicaShards", func(t *testing.T) {
		// A tablet replica's route goes to the tablet's shard, then to any
		// shard of the host; a vnode replica's to the shard owning the token.
		policy, _ := routing.NewPolicy(routing.Options{Datacenter: "a", AllowRemote: true, FixedReplicaOrder: true, NoLeaderAware: true})
		m := optionsSnapshot(t, nil)
		routes := planRoutes(policy, tablet, m)
		if len(routes) < 4 || routes[0] != [2]int{4, 2} || routes[1] != [2]int{4, -1} || routes[2] != [2]int{5, 1} || routes[3] != [2]int{5, -1} {
			t.Fatalf("tablet routes %v, want 4 shard 2, 4, 5 shard 1, 5", routes)
		}
		if first := planRoutes(policy, vnode, m)[0]; first[1] < 0 || first[1] >= 4 {
			t.Fatalf("vnode replica route %v, want a shard of the replica", first)
		}
		// Hosts that are no replicas take any shard.
		for _, r := range routes[4:] {
			if r[1] != -1 {
				t.Fatalf("fallback route %v names a shard", r)
			}
		}
	})

	t.Run("Validate", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			options routing.Options
			hosts   func([]metadata.HostSnapshot)
			valid   bool
		}{
			{"no datacenter", routing.Options{NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, nil, true},
			{"local datacenter", routing.Options{Datacenter: "b", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, nil, true},
			{"unknown datacenter", routing.Options{Datacenter: "x", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, nil, false},
			{"unknown rack", routing.Options{Datacenter: "a", Rack: "r3", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, nil, false},
			{"datacenter filtered out", routing.Options{Datacenter: "b", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, func(h []metadata.HostSnapshot) { h[2].Enabled, h[4].Enabled = false, false }, false},
			{"rack filtered out", routing.Options{Datacenter: "a", Rack: "r2", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, func(h []metadata.HostSnapshot) { h[1].Enabled = false }, false},
			// Hosts that are down still count: they may come back.
			{"datacenter down", routing.Options{Datacenter: "b", NoTokenAware: true, FixedReplicaOrder: true, NoLeaderAware: true}, func(h []metadata.HostSnapshot) { h[2].Up, h[4].Up = false, false }, true},
		} {
			t.Run(test.name, func(t *testing.T) {
				policy, err := routing.NewPolicy(test.options)
				if err != nil {
					t.Fatal(err)
				}
				if err := policy.Validate(optionsSnapshot(t, test.hosts).Public()); (err == nil) != test.valid {
					t.Fatalf("Validate = %v, want valid %v", err, test.valid)
				}
			})
		}
		if _, err := routing.NewPolicy(routing.Options{Rack: "r1"}); err == nil {
			t.Fatal("a rack without a datacenter was accepted")
		}
	})

	t.Run("Unreachable", func(t *testing.T) {
		// Hosts that are down or not connected come after every reachable
		// host of the plan's tiers, replicas among them.
		policy, _ := routing.NewPolicy(routing.Options{Datacenter: "a", Rack: "r1", AllowRemote: true, FixedReplicaOrder: true, NoLeaderAware: true})
		m := optionsSnapshot(t, func(h []metadata.HostSnapshot) { h[0].Connected, h[1].Up = false, false })
		if hosts := policyHosts(policy, vnode, m); !inTiers(hosts, [][]byte{{3}, {4}, {5}, {1, 2}}) {
			t.Fatalf("plan %v, want reachable 3, 4, 5, then 1 and 2", hosts)
		}
	})
}

// TestSingleTarget pins SingleTarget: every request goes to its host, to its
// shard first when it names one, and, unless the shard is strict, to any
// shard of the host after. It ignores the request's token and locality, but
// not the host filter.
func TestSingleTarget(t *testing.T) {
	info := routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](1), Kind: cql.OrdinaryStatement}
	shard := func(id byte, shard uint32, strict bool) metadata.Target {
		target := optionsTarget(id, shard)
		target.StrictShard = strict
		return target
	}
	for _, test := range []struct {
		name   string
		target metadata.Target
		hosts  func([]metadata.HostSnapshot)
		valid  bool
		routes [][2]int
	}{
		{"host", metadata.Target{HostID: cql.UUID{15: 3}}, nil, true, [][2]int{{3, -1}}},
		{"shard", shard(3, 1, false), nil, true, [][2]int{{3, 1}, {3, -1}}},
		{"strict shard", shard(3, 1, true), nil, true, [][2]int{{3, 1}}},
		{"shard out of range", shard(3, 9, false), nil, true, [][2]int{{3, -1}}},
		{"strict shard out of range", shard(3, 9, true), nil, false, nil},
		{"strict without shard", metadata.Target{HostID: cql.UUID{15: 3}, StrictShard: true}, nil, false, [][2]int{{3, -1}}},
		{"down host", shard(3, 1, true), func(h []metadata.HostSnapshot) { h[2].Up, h[2].Connected = false, false }, true, [][2]int{{3, 1}}},
		{"filtered host", shard(3, 1, false), func(h []metadata.HostSnapshot) { h[2].Enabled = false }, false, nil},
		{"unknown host", shard(9, 1, false), nil, false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := routing.SingleTarget(test.target)
			m := optionsSnapshot(t, test.hosts)
			if err := policy.Validate(m.Public()); (err == nil) != test.valid {
				t.Fatalf("Validate = %v, want valid %v", err, test.valid)
			}
			if routes := planRoutes(policy, info, m); !slices.Equal(routes, test.routes) {
				t.Fatalf("routes %v, want %v", routes, test.routes)
			}
		})
	}
}

// TestTabletMigration follows ks.tt through tablet hints, as responses
// carry them when a request reached a node that no longer holds its tablet:
// each hint replaces every tablet it overlaps, and later plans route by it,
// while plans of the snapshot before it keep the replicas they had.
func TestTabletMigration(t *testing.T) {
	policy, _ := routing.NewPolicy(routing.Options{FixedReplicaOrder: true, NoLeaderAware: true})
	m := optionsSnapshot(t, nil)
	for _, step := range []struct {
		name string
		hint tablets.TabletInfo
		// first is, for each token, the route a plan tries first, or none
		// (host 0) when no tablet holds the token.
		first map[int64][2]int
	}{
		{"migrated", tabletHint(t, 0, 100, optionsTarget(2, 3)), map[int64][2]int{50: {2, 3}, 100: {2, 3}, 150: {}}},
		{"split", tabletHint(t, 0, 50, optionsTarget(1, 0)), map[int64][2]int{25: {1, 0}, 50: {1, 0}, 75: {}}},
		{"adjacent", tabletHint(t, 50, 100, optionsTarget(3, 1)), map[int64][2]int{25: {1, 0}, 75: {3, 1}}},
		{"merged", tabletHint(t, 0, 100, optionsTarget(5, 2)), map[int64][2]int{25: {5, 2}, 75: {5, 2}}},
		{"partial overlap", tabletHint(t, 80, 120, optionsTarget(4, 0)), map[int64][2]int{25: {}, 90: {4, 0}, 120: {4, 0}}},
	} {
		t.Run(step.name, func(t *testing.T) {
			before := implRouting.SnapshotData(m).InternalPlacements.Get(tabletTable).InternalValue
			placement, changed := implRouting.InternalUpdateTabletPlacement(before, step.hint)
			if !changed {
				t.Fatal("hint changed nothing")
			}
			if _, again := implRouting.InternalUpdateTabletPlacement(placement, step.hint); again {
				t.Fatal("repeated hint changed the placement again")
			}
			next := implRouting.InternalClonePlacementSnapshot(m)
			implRouting.SnapshotData(next).InternalPlacements = implRouting.SnapshotData(next).InternalPlacements.Put(tabletTable, placement)
			at50 := routing.Info{Keyspace: "ks", Table: "tt", Token: cql.Some[int64](50), Kind: cql.OrdinaryStatement}
			old := m
			oldFirst := planRoutes(policy, at50, old)[0]
			m = next
			for token, want := range step.first {
				info := routing.Info{Keyspace: "ks", Table: "tt", Token: cql.Some(token), Kind: cql.OrdinaryStatement}
				replicas, err := next.Replicas("ks", "tt", token)
				if err != nil {
					t.Fatal(err)
				}
				if want == ([2]int{}) {
					if len(replicas) != 0 {
						t.Fatalf("token %d: replicas %v, want none", token, replicas)
					}
					continue
				}
				if len(replicas) != 1 || replicas[0] != optionsTarget(byte(want[0]), uint32(want[1])) {
					t.Fatalf("token %d: replicas %v, want host %d shard %d", token, replicas, want[0], want[1])
				}
				if routes := planRoutes(policy, info, next); len(routes) == 0 || routes[0] != want {
					t.Fatalf("token %d: routes %v, want %v first", token, routes, want)
				}
			}
			if again := planRoutes(policy, at50, old)[0]; again != oldFirst {
				t.Fatalf("the hint changed the earlier snapshot's first route: %v, was %v", again, oldFirst)
			}
		})
	}
}

// TestReplicaRoutes pins routing.ReplicaRoutes, which custom policies build
// plans from: a token's replicas, to the tablet's shard where tablets place
// the table and to the token's shard on each 4-shard host for vnodes, and no
// routes without a token or known replicas.
func TestReplicaRoutes(t *testing.T) {
	m := optionsSnapshot(t, nil)
	for _, test := range []struct {
		name   string
		info   routing.Info
		routes [][2]int
	}{
		{"vnode", routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](1)}, [][2]int{{3, 2}, {2, 2}, {1, 2}}},
		{"tablet", routing.Info{Keyspace: "ks", Table: "tt", Token: cql.Some[int64](50)}, [][2]int{{4, 2}, {5, 1}}},
		{"unknown tablet", routing.Info{Keyspace: "ks", Table: "tt", Token: cql.Some[int64](200)}, nil},
		{"no token", routing.Info{Keyspace: "ks", Table: "t"}, nil},
		{"no table", routing.Info{Keyspace: "ks", Token: cql.Some[int64](1)}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			var routes [][2]int
			for _, r := range routing.ReplicaRoutes(m.Public(), test.info) {
				routes = append(routes, routePair(r))
			}
			if !slices.Equal(routes, test.routes) {
				t.Fatalf("routes %v, want %v", routes, test.routes)
			}
		})
	}
}
