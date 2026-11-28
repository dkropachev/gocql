// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routingtest_test

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
	"github.com/scylladb/gocql/v2/routing/routingtest"
)

func host(id byte, dc, rack string) metadata.HostSnapshot {
	return metadata.HostSnapshot{HostID: cql.UUID{15: id}, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true}
}

// cluster has hosts 1 and 2 in dc a, 3 and 4 in dc b, and one token range
// whose replicas are 3, 1, 4, 2.
func cluster() routingtest.Cluster {
	hosts := []metadata.HostSnapshot{host(1, "a", "r1"), host(2, "a", "r2"), host(3, "b", "r1"), host(4, "b", "r2")}
	var replicas []metadata.Target
	for _, i := range []int{2, 0, 3, 1} {
		replicas = append(replicas, metadata.Target{HostID: hosts[i].HostID})
	}
	return routingtest.Cluster{
		Hosts:       hosts,
		Keyspaces:   map[string]*metadata.Keyspace{"ks": {Name: "ks", Tables: map[string]*metadata.Table{"t": {Keyspace: "ks", Name: "t"}}}},
		TokenRanges: map[string][]metadata.TokenRange{"ks": {{Start: 0, End: 0, Replicas: replicas}}},
		Generation:  7,
	}
}

var info = routingtest.Request("ks", "t", 42)

// planHosts follows the plan of policy and returns the last byte of each
// host it routes to, once each.
func planHosts(t *testing.T, policy routing.Policy, m *metadata.Snapshot) []byte {
	t.Helper()
	var out []byte
	if r, ok := policy.Pick(info, m); ok {
		out = append(out, r.HostID()[15])
	}
	plan := policy.Fallback(info, m)
	for {
		r, ok := plan.Next()
		if !ok {
			return out
		}
		if id := r.HostID()[15]; !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
}

// mustSnapshot returns routingtest.NewSnapshot(c), failing t on an error.
func mustSnapshot(t *testing.T, c routingtest.Cluster) *metadata.Snapshot {
	t.Helper()
	m, err := routingtest.NewSnapshot(c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewSnapshot(t *testing.T) {
	m := mustSnapshot(t, cluster())
	if m.Generation() != 7 || len(m.Hosts()) != 4 {
		t.Fatalf("generation %d, %d hosts", m.Generation(), len(m.Hosts()))
	}
	replicas, err := m.Replicas("ks", "t", 42)
	if err != nil || len(replicas) != 4 || replicas[0].HostID[15] != 3 {
		t.Fatalf("Replicas = %v, %v", replicas, err)
	}
	if routes := routing.ReplicaRoutes(m, info); len(routes) != 4 {
		t.Fatalf("ReplicaRoutes = %v", routes)
	}
	if routes := routing.HostRoutes(m); len(routes) != 4 {
		t.Fatalf("HostRoutes = %v", routes)
	}
	if _, ok := mustSnapshot(t, routingtest.Cluster{}).Keyspace("ks"); ok {
		t.Fatal("the zero Cluster has a keyspace")
	}

	c := cluster()
	c.Tablets = map[routingtest.Table][]metadata.TokenRange{{Keyspace: "ks", Name: "t"}: {{Start: 100, End: 200, Replicas: []metadata.Target{{HostID: c.Hosts[1].HostID}}}}}
	m = mustSnapshot(t, c)
	if replicas, err := m.Replicas("ks", "t", 150); err != nil || len(replicas) != 1 || replicas[0].HostID[15] != 2 {
		t.Fatalf("tablet Replicas = %v, %v", replicas, err)
	}
	if replicas, err := m.Replicas("ks", "t", 42); !errors.Is(err, metadata.ErrPlacementUnknown) || len(replicas) != 0 {
		t.Fatalf("Replicas outside the tablets = %v, %v", replicas, err)
	}
}

// TestLocalDatacenter checks that the built-in policy takes the session's
// locality when its options name no datacenter of their own.
func TestLocalDatacenter(t *testing.T) {
	newPolicy := func(o routing.Options) routing.Policy {
		p := routing.NewPolicy(o)
		return p
	}
	global := mustSnapshot(t, cluster())
	c := cluster()
	c.LocalDatacenter = "a"
	local := mustSnapshot(t, c)
	c.LocalRack = "r2"
	rack := mustSnapshot(t, c)

	for range 20 {
		if got := planHosts(t, newPolicy(routing.Options{}), global); len(got) != 4 {
			t.Fatalf("without a local datacenter: %v", got)
		}
		if got := planHosts(t, newPolicy(routing.Options{}), local); len(got) != 2 || slices.Contains(got, 3) || slices.Contains(got, 4) {
			t.Fatalf("local datacenter a: %v", got)
		}
		if got := planHosts(t, newPolicy(routing.Options{}), rack); !slices.Equal(got, []byte{2, 1}) {
			t.Fatalf("local rack r2: %v", got)
		}
		if got := planHosts(t, newPolicy(routing.Options{AllowRemote: true}), local); len(got) != 4 || got[0] > 2 || got[1] > 2 {
			t.Fatalf("local datacenter a, remote allowed: %v", got)
		}
		if got := planHosts(t, newPolicy(routing.Options{Datacenter: "b"}), rack); len(got) != 2 || slices.Contains(got, 1) || slices.Contains(got, 2) {
			t.Fatalf("own datacenter b: %v", got)
		}
	}

	c.LocalDatacenter, c.LocalRack = "x", ""
	missing := mustSnapshot(t, c)
	if err := newPolicy(routing.Options{}).(routing.Validator).Validate(missing); err == nil {
		t.Fatal("a local datacenter without hosts validated")
	}
	if err := newPolicy(routing.Options{Datacenter: "a"}).(routing.Validator).Validate(missing); err != nil {
		t.Fatalf("the policy's own datacenter: %v", err)
	}
}

// wrapped overrides only Pick; Validate and Learn come from the embedded
// *routing.Builtin.
type wrapped struct{ *routing.Builtin }

func (w wrapped) Pick(routing.Info, *metadata.Snapshot) (metadata.Route, bool) {
	return metadata.Route{}, false
}

// TestEmbeddedBuiltin checks that a policy embedding *routing.Builtin keeps
// the built-in Validate and Learn, and that the zero Builtin routes as the
// default policy.
func TestEmbeddedBuiltin(t *testing.T) {
	builtin := routing.NewPolicy(routing.Options{})
	var policy routing.Policy = wrapped{builtin}
	validator, ok := policy.(routing.Validator)
	if !ok {
		t.Fatal("the wrapper lost Validate")
	}
	observer, ok := policy.(routing.Learner)
	if !ok {
		t.Fatal("the wrapper lost Learn")
	}
	observer.Learn(routing.AttemptEvent{})

	c := cluster()
	c.LocalDatacenter = "x"
	if err := validator.Validate(mustSnapshot(t, c)); err == nil {
		t.Fatal("the wrapper did not run the built-in validation")
	}
	if err := validator.Validate(mustSnapshot(t, cluster())); err != nil {
		t.Fatal(err)
	}
	if got := planHosts(t, policy, mustSnapshot(t, cluster())); len(got) != 4 {
		t.Fatalf("the wrapper's plan: %v", got)
	}
	if got := planHosts(t, &routing.Builtin{}, mustSnapshot(t, cluster())); len(got) != 4 {
		t.Fatalf("the zero Builtin's plan: %v", got)
	}
	if err := routing.SingleTarget(metadata.Target{HostID: cql.UUID{15: 9}}).Validate(mustSnapshot(t, cluster())); err == nil {
		t.Fatal("SingleTarget validated a host the snapshot does not hold")
	}
}

// TestVnodeReplicaShards checks that vnode replica routes carry the shard
// that owns the token, from the hosts' sharding, and that Route.ShardOf
// works without connections.
func TestVnodeReplicaShards(t *testing.T) {
	c := cluster()
	for i := range c.Hosts {
		c.Hosts[i].Shards = 8
		c.Hosts[i].ShardingIgnoreMSB = 12
	}
	m := mustSnapshot(t, c)
	const token = int64(-4611686018427387904)
	want := routing.HostRoutes(m)[0].ShardOf(token)
	if !want.Set || want.Value >= 8 {
		t.Fatalf("ShardOf without connections = %v", want)
	}
	routes := routing.ReplicaRoutes(m, routingtest.Request("ks", "t", token))
	if len(routes) != 4 {
		t.Fatalf("ReplicaRoutes = %v", routes)
	}
	for _, r := range routes {
		if r.ShardID() != want {
			t.Fatalf("vnode replica route to shard %v, want %v", r.ShardID(), want)
		}
	}
	// Without sharding, as on Cassandra, routes go to any shard.
	for _, r := range routing.ReplicaRoutes(mustSnapshot(t, cluster()), info) {
		if r.ShardID().Set {
			t.Fatalf("a host without sharding got shard %v", r.ShardID())
		}
		if r.ShardOf(42).Set {
			t.Fatal("ShardOf on a host without sharding is set")
		}
	}
}

// TestReplicaRoutesWithoutTableSchema checks that a vnode keyspace whose
// tables are not loaded, as under metadata.ModeKeyspaces, still routes by the
// keyspace's ring.
func TestReplicaRoutesWithoutTableSchema(t *testing.T) {
	c := cluster()
	c.Keyspaces = map[string]*metadata.Keyspace{"ks": {Name: "ks"}}
	c.TokenRanges = map[string][]metadata.TokenRange{"ks": {{Replicas: []metadata.Target{{HostID: c.Hosts[2].HostID}}}}}
	m := mustSnapshot(t, c)
	if routes := routing.ReplicaRoutes(m, info); len(routes) != 1 || routes[0].HostID()[15] != 3 {
		t.Fatalf("ReplicaRoutes without table schema = %v", routes)
	}
	policy := routing.NewPolicy(routing.Options{})
	for range 10 {
		if got := planHosts(t, policy, m); len(got) == 0 || got[0] != 3 {
			t.Fatalf("built-in plan without table schema: %v", got)
		}
	}
	c.Keyspaces["ks"].TabletBased = true
	if routes := routing.ReplicaRoutes(mustSnapshot(t, c), info); len(routes) != 0 {
		t.Fatalf("a tablet keyspace routed a table it has no placement for: %v", routes)
	}
}

// TestRouteHelpers checks RouteTo, AppendReplicaRoutes, FilterPlan, Routes,
// Route.Datacenter and Route.Rack, on a snapshot built with Host and Tables.
func TestRouteHelpers(t *testing.T) {
	hosts := []metadata.HostSnapshot{
		routingtest.Host(cql.UUID{15: 1}, "a", "r1"),
		routingtest.Host(cql.UUID{15: 2}, "b", "r2"),
	}
	m := mustSnapshot(t, routingtest.Cluster{
		Hosts:  hosts,
		Tables: []routingtest.Table{{Keyspace: "ks", Name: "t"}},
		TokenRanges: map[string][]metadata.TokenRange{"ks": {{Replicas: []metadata.Target{
			{HostID: hosts[1].HostID}, {HostID: hosts[0].HostID},
		}}}},
	})
	if _, ok := m.Table("ks", "t"); !ok {
		t.Fatal("Tables added no table")
	}

	r, ok := routing.RouteTo(m, metadata.Target{HostID: hosts[1].HostID, ShardID: cql.Some[uint32](3)})
	if !ok || r.HostID() != hosts[1].HostID || r.ShardID() != cql.Some[uint32](3) || r.Datacenter() != "b" || r.Rack() != "r2" {
		t.Fatalf("RouteTo = %v %v %v %q %q", ok, r.HostID(), r.ShardID(), r.Datacenter(), r.Rack())
	}
	if r, ok := routing.RouteTo(m, metadata.Target{HostID: hosts[0].HostID}); !ok || r.ShardID().Set {
		t.Fatalf("RouteTo without a shard = %v, %v", r.ShardID(), ok)
	}
	if _, ok := routing.RouteTo(m, metadata.Target{HostID: cql.UUID{15: 9}}); ok {
		t.Fatal("RouteTo found an unknown host")
	}
	if (metadata.Route{}).Datacenter() != "" || (metadata.Route{}).Rack() != "" {
		t.Fatal("the zero Route has a locality")
	}

	buf := make([]metadata.Route, 1, 8)
	routes := routing.AppendReplicaRoutes(buf, m, info)
	if len(routes) != 3 || &routes[0] != &buf[0] || routes[1].HostID() != hosts[1].HostID || routes[2].HostID() != hosts[0].HostID {
		t.Fatalf("AppendReplicaRoutes = %v", routes)
	}
	if got := routing.AppendReplicaRoutes(buf[:0], m, routing.Info{}); len(got) != 0 {
		t.Fatalf("AppendReplicaRoutes without a token = %v", got)
	}

	plan := routing.FilterPlan(routing.PlanOf(routing.HostRoutes(m)...), func(r metadata.Route) bool { return r.Datacenter() == "a" })
	var ids []byte
	for r := range routing.Routes(plan) {
		ids = append(ids, r.HostID()[15])
	}
	if !slices.Equal(ids, []byte{1}) {
		t.Fatalf("filtered plan routes to %v", ids)
	}
	// Routes stops consuming the plan when the loop breaks.
	all := routing.PlanOf(routing.HostRoutes(m)...)
	for range routing.Routes(all) {
		break
	}
	if r, ok := all.Next(); !ok || r.HostID() != hosts[1].HostID {
		t.Fatalf("after a break the plan continues with %v, %v", r.HostID(), ok)
	}
}

// TestNewSnapshotValidates checks that NewSnapshot rejects a cluster no
// session could see.
func TestNewSnapshotValidates(t *testing.T) {
	for name, edit := range map[string]func(c *routingtest.Cluster){
		"duplicate host": func(c *routingtest.Cluster) { c.Hosts = append(c.Hosts, c.Hosts[0]) },
		"unknown replica": func(c *routingtest.Cluster) {
			c.TokenRanges["ks"][0].Replicas = append(c.TokenRanges["ks"][0].Replicas, metadata.Target{HostID: cql.UUID{15: 9}})
		},
		"duplicate token": func(c *routingtest.Cluster) {
			c.TokenRanges["ks"] = append(c.TokenRanges["ks"], c.TokenRanges["ks"][0])
		},
		"empty tablet": func(c *routingtest.Cluster) {
			c.Tablets = map[routingtest.Table][]metadata.TokenRange{{Keyspace: "ks", Name: "t"}: {{Start: 5, End: 5}}}
		},
		"overlapping tablets": func(c *routingtest.Cluster) {
			c.Tablets = map[routingtest.Table][]metadata.TokenRange{{Keyspace: "ks", Name: "t"}: {{Start: 0, End: 10}, {Start: 5, End: 20}}}
		},
		"tablets of no table": func(c *routingtest.Cluster) {
			c.Tablets = map[routingtest.Table][]metadata.TokenRange{{Keyspace: "ks", Name: "missing"}: {{Start: 0, End: 10}}}
		},
	} {
		c := cluster()
		edit(&c)
		if _, err := routingtest.NewSnapshot(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestCombinedPolicies checks Filter, Prefer and Concat, and that they
// forward Validate and Learn.
func TestCombinedPolicies(t *testing.T) {
	c := cluster()
	c.LocalDatacenter = "a"
	m := mustSnapshot(t, c)
	builtin := routing.NewPolicy(routing.Options{AllowRemote: true})
	notOne := func(r metadata.Route) bool { return r.HostID()[15] != 1 }
	for range 20 {
		if got := planHosts(t, routing.Filter(builtin, notOne), m); len(got) != 3 || slices.Contains(got, 1) {
			t.Fatalf("Filter: %v", got)
		}
		inB := func(r metadata.Route) bool { return r.Datacenter() == "b" }
		if got := planHosts(t, routing.Prefer(builtin, inB), m); len(got) != 4 || got[0] < 3 || got[1] < 3 {
			t.Fatalf("Prefer: %v", got)
		}
	}
	first := routing.SingleTarget(metadata.Target{HostID: cql.UUID{15: 4}})
	got := planHosts(t, routing.Concat(first, builtin), m)
	if len(got) != 4 || got[0] != 4 {
		t.Fatalf("Concat: %v", got)
	}
	if got := planHosts(t, routing.Concat(), m); len(got) != 0 {
		t.Fatalf("Concat of nothing: %v", got)
	}

	learned := 0
	learner := &learnerPolicy{Builtin: builtin, learn: func() { learned++ }}
	for _, p := range []routing.Policy{routing.Filter(learner, notOne), routing.Prefer(learner, notOne), routing.Concat(first, learner)} {
		p.(routing.Learner).Learn(routing.AttemptEvent{})
		if err := p.(routing.Validator).Validate(m); err != nil {
			t.Fatal(err)
		}
	}
	if learned != 3 {
		t.Fatalf("Learn reached the wrapped policy %d times, want 3", learned)
	}
	bad := routing.SingleTarget(metadata.Target{HostID: cql.UUID{15: 9}})
	if err := routing.Filter(bad, notOne).(routing.Validator).Validate(m); err == nil {
		t.Fatal("Filter did not forward Validate")
	}
	if err := routing.Concat(builtin, bad).(routing.Validator).Validate(m); err == nil {
		t.Fatal("Concat did not validate every policy")
	}
}

type learnerPolicy struct {
	*routing.Builtin
	learn func()
}

func (p *learnerPolicy) Learn(routing.AttemptEvent) { p.learn() }

// TestRouteEnabledAndRouteTo checks Route.Enabled, that HostRoutes includes
// hosts the filter rejects, and that RouteTo honors AnyShardFallback for a
// shard the host does not have.
func TestRouteEnabledAndRouteTo(t *testing.T) {
	c := cluster()
	c.Hosts[1].Enabled = false
	c.Hosts[2].Shards = 4
	m := mustSnapshot(t, c)
	routes := routing.HostRoutes(m)
	if len(routes) != 4 || !routes[0].Enabled() || routes[1].Enabled() {
		t.Fatalf("HostRoutes = %v", routes)
	}
	if (metadata.Route{}).Enabled() {
		t.Fatal("the zero Route is enabled")
	}
	host3 := c.Hosts[2].HostID
	for _, test := range []struct {
		target metadata.Target
		want   cql.Option[uint32]
	}{
		{metadata.Target{HostID: host3, ShardID: cql.Some[uint32](2)}, cql.Some[uint32](2)},
		{metadata.Target{HostID: host3, ShardID: cql.Some[uint32](7)}, cql.Some[uint32](7)},
		{metadata.Target{HostID: host3, ShardID: cql.Some[uint32](2), AnyShardFallback: true}, cql.Some[uint32](2)},
		{metadata.Target{HostID: host3, ShardID: cql.Some[uint32](7), AnyShardFallback: true}, cql.Option[uint32]{}},
		{metadata.Target{HostID: c.Hosts[0].HostID, ShardID: cql.Some[uint32](0), AnyShardFallback: true}, cql.Option[uint32]{}},
	} {
		if r, ok := routing.RouteTo(m, test.target); !ok || r.ShardID() != test.want {
			t.Errorf("RouteTo(%+v) = %v, %v; want shard %v", test.target, r.ShardID(), ok, test.want)
		}
	}
}

// TestSnapshotTopologyLookups checks Snapshot.Tables, HostInfo,
// PartitionReplicas, the shards Replicas fills in and TokenRanges'
// completeness.
func TestSnapshotTopologyLookups(t *testing.T) {
	c := cluster()
	c.Hosts[2].Shards = 4
	id := &metadata.Column{Keyspace: "ks", Table: "t", Name: "id", Type: "int", Kind: metadata.ColumnKindPartitionKey}
	c.Keyspaces["ks"].Tables["t"] = &metadata.Table{Keyspace: "ks", Name: "t", Columns: map[string]*metadata.Column{"id": id}, PartitionKey: []*metadata.Column{id}}
	c.Keyspaces["ks"].Tables["b"] = &metadata.Table{Keyspace: "ks", Name: "b"}
	m := mustSnapshot(t, c)
	if got := m.Tables("ks"); !slices.Equal(got, []string{"b", "t"}) {
		t.Fatalf("Tables = %v", got)
	}
	if m.Tables("missing") != nil {
		t.Fatal("Tables of a missing keyspace")
	}
	if h, ok := m.HostInfo(c.Hosts[0].HostID); !ok || h.Tokens != nil || h.Datacenter != "a" {
		t.Fatalf("HostInfo = %+v, %v", h, ok)
	}
	hosts, err := m.PartitionReplicas("ks", "t", cql.IntValue(1))
	if err != nil || len(hosts) != 4 || hosts[0].HostID[15] != 3 {
		t.Fatalf("PartitionReplicas = %v, %v", hosts, err)
	}
	token, _ := m.Token("ks", "t", cql.IntValue(1))
	replicas, err := m.Replicas("ks", "t", token)
	if err != nil || !replicas[0].ShardID.Set || replicas[1].ShardID.Set {
		t.Fatalf("Replicas = %v, %v; want the shard of the sharded host 3 only", replicas, err)
	}
	if replicas[0].ShardID != routing.ReplicaRoutes(m, routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some(token)})[0].ShardID() {
		t.Fatal("Replicas and ReplicaRoutes disagree on the shard")
	}
	// A table the snapshot does not hold is placed by its vnode keyspace's
	// ring, as routing places it.
	if replicas, err := m.Replicas("ks", "unloaded", token); err != nil || len(replicas) != 4 {
		t.Fatalf("Replicas of a table not loaded = %v, %v", replicas, err)
	}
	if _, complete, err := m.TokenRanges("ks", "t"); err != nil || !complete {
		t.Fatalf("token ring complete %v, %v", complete, err)
	}
	c.Tablets = map[routingtest.Table][]metadata.TokenRange{{Keyspace: "ks", Name: "t"}: {{Start: math.MinInt64, End: 0}}}
	m = mustSnapshot(t, c)
	if _, complete, err := m.TokenRanges("ks", "t"); err != nil || complete {
		t.Fatalf("half the tablets complete %v, %v", complete, err)
	}
	c.Tablets[routingtest.Table{Keyspace: "ks", Name: "t"}] = append(c.Tablets[routingtest.Table{Keyspace: "ks", Name: "t"}], metadata.TokenRange{Start: 0, End: math.MaxInt64})
	m = mustSnapshot(t, c)
	if _, complete, err := m.TokenRanges("ks", "t"); err != nil || !complete {
		t.Fatalf("every tablet complete %v, %v", complete, err)
	}
}
