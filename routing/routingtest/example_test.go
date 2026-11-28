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

func ExampleNewSnapshot() {
	hosts := []metadata.HostSnapshot{
		routingtest.Host(cql.UUID{15: 1}, "dc1", ""),
		routingtest.Host(cql.UUID{15: 2}, "dc1", ""),
		routingtest.Host(cql.UUID{15: 3}, "dc2", ""),
	}
	m, err := routingtest.NewSnapshot(routingtest.Cluster{
		Hosts:  hosts,
		Tables: []routingtest.Table{{Keyspace: "ks", Name: "t"}},
		TokenRanges: map[string][]metadata.TokenRange{"ks": {{Replicas: []metadata.Target{
			{HostID: hosts[0].HostID}, {HostID: hosts[1].HostID}, {HostID: hosts[2].HostID},
		}}}},
		LocalDatacenter: "dc1",
	})
	if err != nil {
		panic(err)
	}
	builtin := routing.NewPolicy(routing.Options{})
	// The policy under test never routes to host 1. Filter keeps the
	// built-in policy's validation and latency tracking.
	policy := routing.Filter(builtin, func(r metadata.Route) bool { return r.HostID() != hosts[0].HostID })
	// A routing.Info's Kind defaults to cql.UnknownStatement, which routes
	// as a conditional request would; Request sets OrdinaryStatement, for
	// the usual plan.
	info := routingtest.Request("ks", "t", 42)
	for r := range routing.Routes(policy.Fallback(info, m)) {
		fmt.Println(r.HostID()[15], r.Datacenter(), r.ShardID().Set)
	}
	// Output:
	// 2 dc1 false
}
