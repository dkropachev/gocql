// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routingtest_test

import (
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

var info = routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](42), Kind: cql.OrdinaryStatement}

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

func TestNewSnapshot(t *testing.T) {
	m := routingtest.NewSnapshot(cluster())
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
	if _, ok := routingtest.NewSnapshot(routingtest.Cluster{}).Keyspace("ks"); ok {
		t.Fatal("the zero Cluster has a keyspace")
	}

	c := cluster()
	c.Tablets = map[routingtest.Table][]metadata.TokenRange{{Keyspace: "ks", Name: "t"}: {{Start: 100, End: 200, Replicas: []metadata.Target{{HostID: c.Hosts[1].HostID}}}}}
	m = routingtest.NewSnapshot(c)
	if replicas, err := m.Replicas("ks", "t", 150); err != nil || len(replicas) != 1 || replicas[0].HostID[15] != 2 {
		t.Fatalf("tablet Replicas = %v, %v", replicas, err)
	}
	if replicas, err := m.Replicas("ks", "t", 42); err != nil || len(replicas) != 0 {
		t.Fatalf("Replicas outside the tablets = %v, %v", replicas, err)
	}
}

// TestLocalDatacenter checks that the built-in policy takes the session's
// locality when its options name no datacenter of their own.
func TestLocalDatacenter(t *testing.T) {
	newPolicy := func(o routing.Options) routing.Policy {
		p, err := routing.NewPolicy(o)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	global := routingtest.NewSnapshot(cluster())
	c := cluster()
	c.LocalDatacenter = "a"
	local := routingtest.NewSnapshot(c)
	c.LocalRack = "r2"
	rack := routingtest.NewSnapshot(c)

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
	missing := routingtest.NewSnapshot(c)
	if err := newPolicy(routing.Options{}).(routing.Validator).Validate(missing); err == nil {
		t.Fatal("a local datacenter without hosts validated")
	}
	if err := newPolicy(routing.Options{Datacenter: "a"}).(routing.Validator).Validate(missing); err != nil {
		t.Fatalf("the policy's own datacenter: %v", err)
	}
}

// wrapped overrides only Pick; Validate and Observe come from the embedded
// *routing.Builtin.
type wrapped struct{ *routing.Builtin }

func (w wrapped) Pick(routing.Info, *metadata.Snapshot) (metadata.Route, bool) {
	return metadata.Route{}, false
}

// TestEmbeddedBuiltin checks that a policy embedding *routing.Builtin keeps
// the built-in Validate and Observe, and that the zero Builtin routes as the
// default policy.
func TestEmbeddedBuiltin(t *testing.T) {
	builtin, err := routing.NewPolicy(routing.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var policy routing.Policy = wrapped{builtin}
	validator, ok := policy.(routing.Validator)
	if !ok {
		t.Fatal("the wrapper lost Validate")
	}
	observer, ok := policy.(routing.AttemptObserver)
	if !ok {
		t.Fatal("the wrapper lost Observe")
	}
	observer.Observe(routing.AttemptEvent{})

	c := cluster()
	c.LocalDatacenter = "x"
	if err := validator.Validate(routingtest.NewSnapshot(c)); err == nil {
		t.Fatal("the wrapper did not run the built-in validation")
	}
	if err := validator.Validate(routingtest.NewSnapshot(cluster())); err != nil {
		t.Fatal(err)
	}
	if got := planHosts(t, policy, routingtest.NewSnapshot(cluster())); len(got) != 4 {
		t.Fatalf("the wrapper's plan: %v", got)
	}
	if got := planHosts(t, &routing.Builtin{}, routingtest.NewSnapshot(cluster())); len(got) != 4 {
		t.Fatalf("the zero Builtin's plan: %v", got)
	}
	if err := routing.SingleTarget(metadata.Target{HostID: cql.UUID{15: 9}}).Validate(routingtest.NewSnapshot(cluster())); err == nil {
		t.Fatal("SingleTarget validated a host the snapshot does not hold")
	}
}
