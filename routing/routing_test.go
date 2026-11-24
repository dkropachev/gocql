// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing_test

import (
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// TestWithoutSession runs where no session is linked: this package's test
// binary leaves out the engine that implements the built-in policies.
func TestWithoutSession(t *testing.T) {
	p, err := routing.NewPolicy(routing.Options{FixedReplicaOrder: true, NoLeaderAware: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.(routing.Validator).Validate(nil); err == nil {
		t.Error("Validate succeeded without a session")
	}
	if _, ok := p.Pick(routing.Info{}, nil); ok {
		t.Error("Pick found a route without a session")
	}
	if _, ok := p.Fallback(routing.Info{}, nil).Next(); ok {
		t.Error("Fallback found a route without a session")
	}
	p.(routing.AttemptObserver).Observe(routing.AttemptEvent{})
	if r := routing.HostRoutes(nil); r != nil {
		t.Errorf("HostRoutes = %v", r)
	}
	if r := routing.ReplicaRoutes(nil, routing.Info{}); r != nil {
		t.Errorf("ReplicaRoutes = %v", r)
	}
}

func TestNewPolicyValidates(t *testing.T) {
	if _, err := routing.NewPolicy(routing.Options{Rack: "r1"}); err == nil {
		t.Error("NewPolicy accepted a rack without a datacenter")
	}
	if _, err := routing.NewPolicy(routing.Options{Datacenter: "dc", Latency: routing.LatencyOptions{Enabled: true, ExclusionThreshold: 0.5}}); err == nil {
		t.Error("NewPolicy accepted an exclusion threshold below 1")
	}
	var zero metadata.Route
	if zero.Usable() || zero.HostID() != zero.Target().HostID || zero.ShardOf(1).Set || zero.ShardID().Set {
		t.Error("the zero Route has a host")
	}
}

func TestPlanOf(t *testing.T) {
	if _, ok := routing.PlanOf().Next(); ok {
		t.Fatal("an empty plan returned a route")
	}
	a := metadata.Route{}.WithShardID(cql.Option[uint32]{})
	plan := routing.PlanOf(a, a, a)
	for i := range 3 {
		if _, ok := plan.Next(); !ok {
			t.Fatalf("route %d missing", i)
		}
	}
	if _, ok := plan.Next(); ok {
		t.Fatal("the plan returned a fourth route")
	}
}
