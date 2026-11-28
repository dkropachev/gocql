// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing_test

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

func TestAttemptEventCQLTrace(t *testing.T) {
	if _, ok := (routing.AttemptEvent{}).CQLTrace(); ok {
		t.Error("an attempt without a trace session has a reference")
	}
	id, host := cql.RandomUUID(), cql.RandomUUID()
	e := routing.AttemptEvent{CQLTraceID: cql.Some(id), Target: metadata.Target{HostID: host}, Page: 2, Attempt: 3, Speculative: true, Err: errors.New("unavailable")}
	ref, ok := e.CQLTrace()
	if want := (cql.CQLTraceRef{ID: id, HostID: host, Page: 2, Attempt: 3, Failed: true, Speculative: true}); !ok || ref != want {
		t.Errorf("CQLTrace = %+v, %v, want %+v", ref, ok, want)
	}
}

// TestWithoutSession runs where no session is linked: this package's test
// binary leaves out the engine that implements the built-in policies.
func TestWithoutSession(t *testing.T) {
	p := routing.NewPolicy(routing.Options{FixedReplicaOrder: true, NoLeaderAware: true})
	if err := p.Validate(nil); err == nil {
		t.Error("Validate succeeded without a session")
	}
	if _, ok := p.Pick(routing.Info{}, nil); ok {
		t.Error("Pick found a route without a session")
	}
	if _, ok := p.Fallback(routing.Info{}, nil).Next(); ok {
		t.Error("Fallback found a route without a session")
	}
	p.Learn(routing.AttemptEvent{})
	if r := routing.HostRoutes(nil); r != nil {
		t.Errorf("HostRoutes = %v", r)
	}
	if r := routing.ReplicaRoutes(nil, routing.Info{}); r != nil {
		t.Errorf("ReplicaRoutes = %v", r)
	}
	if _, ok := routing.RouteTo(nil, metadata.Target{HostID: cql.UUID{1}}); ok {
		t.Error("RouteTo found a route without a session")
	}
}

func TestZeroRoute(t *testing.T) {
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
