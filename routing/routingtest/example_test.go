// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routingtest_test

import (
	"fmt"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
	"github.com/scylladb/gocql/v2/routing/routingtest"
)

// avoid wraps a policy and never routes to one host.
type avoid struct {
	routing.Policy
	host cql.UUID
}

func (a avoid) Pick(info routing.Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := a.Policy.Pick(info, m)
	return r, ok && r.HostID() != a.host
}

func (a avoid) Fallback(info routing.Info, m *metadata.Snapshot) routing.Plan {
	var routes []metadata.Route
	plan := a.Policy.Fallback(info, m)
	for r, ok := plan.Next(); ok; r, ok = plan.Next() {
		if r.HostID() != a.host {
			routes = append(routes, r)
		}
	}
	return routing.PlanOf(routes...)
}

func ExampleNewSnapshot() {
	hosts := []metadata.HostSnapshot{
		{HostID: cql.UUID{15: 1}, Datacenter: "dc1", Enabled: true, Up: true, Connected: true},
		{HostID: cql.UUID{15: 2}, Datacenter: "dc1", Enabled: true, Up: true, Connected: true},
		{HostID: cql.UUID{15: 3}, Datacenter: "dc2", Enabled: true, Up: true, Connected: true},
	}
	m := routingtest.NewSnapshot(routingtest.Cluster{
		Hosts:     hosts,
		Keyspaces: map[string]*metadata.Keyspace{"ks": {Name: "ks", Tables: map[string]*metadata.Table{"t": {Keyspace: "ks", Name: "t"}}}},
		TokenRanges: map[string][]metadata.TokenRange{"ks": {{Replicas: []metadata.Target{
			{HostID: hosts[0].HostID}, {HostID: hosts[1].HostID}, {HostID: hosts[2].HostID},
		}}}},
		LocalDatacenter: "dc1",
	})
	builtin, err := routing.NewPolicy(routing.Options{})
	if err != nil {
		panic(err)
	}
	policy := avoid{Policy: builtin, host: hosts[0].HostID}
	info := routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](42), Kind: cql.OrdinaryStatement}
	plan := policy.Fallback(info, m)
	for r, ok := plan.Next(); ok; r, ok = plan.Next() {
		fmt.Println(r.HostID()[15], r.ShardID().Set)
	}
	// Output:
	// 2 false
}
