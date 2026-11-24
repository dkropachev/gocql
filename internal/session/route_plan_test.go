// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// invalidPolicy fails validation and routes nowhere.
type invalidPolicy struct{}

func (invalidPolicy) Validate(*metadata2.Snapshot) error { return errors.New("invalid") }
func (invalidPolicy) Pick(routing2.Info, *metadata2.Snapshot) (metadata2.Route, bool) {
	return metadata2.Route{}, false
}
func (invalidPolicy) Fallback(routing2.Info, *metadata2.Snapshot) routing2.Plan {
	return routing2.PlanOf()
}

// TestRoutePlanPinned checks that a pinned request routes to its host
// without validating or consulting the profile's policy.
func TestRoutePlanPinned(t *testing.T) {
	m := v2TestMetadata()
	host := implRouting.SnapshotData(m).InternalHosts[1].HostID
	builtin := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{Datacenter: "missing"}))
	for _, policy := range []implRouting.RoutingPolicy{implRouting.EngineOf(invalidPolicy{}), builtin} {
		if _, err := routePlan(policy, routing2.Info{}, m, new(implRouting.InternalLazyRoutingPlan), nil); err == nil {
			t.Fatalf("%T: an unpinned request ignored validation", policy)
		}
		plan, err := routePlan(policy, routing2.Info{HostID: cql.Some(host)}, m, new(implRouting.InternalLazyRoutingPlan), nil)
		if err != nil {
			t.Fatalf("%T: %v", policy, err)
		}
		if got := v2PlanIDs(plan); len(got) != 1 || got[0] != host[15] {
			t.Fatalf("%T: pinned plan %v", policy, got)
		}
	}
}

// TestPublishedLocality checks that the snapshots a session publishes carry
// its local datacenter, which a built-in policy without one of its own
// takes.
func TestPublishedLocality(t *testing.T) {
	s := testSession(t)
	policy := codecTestMust(implRouting.NewRoutingPolicy(routing2.Options{}))
	if err := policy.Validate(s.Metadata()); err != nil {
		t.Fatal(err)
	}
	peripheral := s.Peripheral()
	peripheral.InternalMu.Lock()
	config := peripheral.InternalConfig
	peripheral.InternalMu.Unlock()
	config.LocalDatacenter = "missing"
	s.ConfigureV2Peripheral(config)
	if !s.PublishV2Metadata() {
		t.Fatal("publication failed")
	}
	if err := policy.Validate(s.Metadata()); err == nil {
		t.Fatal("a local datacenter without hosts validated")
	}
}
