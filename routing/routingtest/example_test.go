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

// avoid wraps the built-in policy and never routes to one host. Embedding
// *routing.Builtin keeps its Validate and Learn.
type avoid struct {
	*routing.Builtin
	host cql.UUID
}

func (a avoid) Pick(info routing.Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := a.Builtin.Pick(info, m)
	return r, ok && r.HostID() != a.host
}

func (a avoid) Fallback(info routing.Info, m *metadata.Snapshot) routing.Plan {
	return routing.FilterPlan(a.Builtin.Fallback(info, m), func(r metadata.Route) bool { return r.HostID() != a.host })
}

func ExampleNewSnapshot() {
	hosts := []metadata.HostSnapshot{
		routingtest.Host(cql.UUID{15: 1}, "dc1", ""),
		routingtest.Host(cql.UUID{15: 2}, "dc1", ""),
		routingtest.Host(cql.UUID{15: 3}, "dc2", ""),
	}
	m := routingtest.NewSnapshot(routingtest.Cluster{
		Hosts:  hosts,
		Tables: []routingtest.Table{{Keyspace: "ks", Name: "t"}},
		TokenRanges: map[string][]metadata.TokenRange{"ks": {{Replicas: []metadata.Target{
			{HostID: hosts[0].HostID}, {HostID: hosts[1].HostID}, {HostID: hosts[2].HostID},
		}}}},
		LocalDatacenter: "dc1",
	})
	builtin, err := routing.NewPolicy(routing.Options{})
	if err != nil {
		panic(err)
	}
	policy := avoid{Builtin: builtin, host: hosts[0].HostID}
	info := routing.Info{Keyspace: "ks", Table: "t", Token: cql.Some[int64](42), Kind: cql.OrdinaryStatement}
	for r := range routing.Routes(policy.Fallback(info, m)) {
		fmt.Println(r.HostID()[15], r.Datacenter(), r.ShardID().Set)
	}
	// Output:
	// 2 dc1 false
}
