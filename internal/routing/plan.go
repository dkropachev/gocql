// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/routing"
)

// NewPlan returns the plan a request follows under policy: the route
// policy.Pick gives, then the routes of policy.Fallback except that one.
// Fallback is called only when the plan needs a second route, or a first
// one when Pick returns false. A request pinned to a host follows
// PinnedPlan, whatever the policy.
func NewPlan(policy RoutingPolicy, info routing.Info, m *MetadataSnapshot) QueryPlan {
	if p, ok := policy.(*InternalRoutingPolicy); ok {
		// Built-in routing picks and falls back within one cursor.
		return p.InternalPlanInto(info, m, new(InternalLazyRoutingPlan))
	}
	if info.HostID.Set {
		return PinnedPlan(info, m, new(InternalLazyRoutingPlan))
	}
	if p, ok := policy.(*singleTargetRouting); ok {
		// A single target has nothing cheaper to pick than its plan.
		return p.Fallback(info, m)
	}
	return &pickPlan{policy: policy, info: info, metadata: m}
}

// pickPlan is the plan of a policy: its pick, then its fallback.
type pickPlan struct {
	policy   RoutingPolicy
	fallback QueryPlan
	metadata *MetadataSnapshot
	picked   Route
	info     routing.Info
	started  bool
	hasPick  bool
}

func (p *pickPlan) Next() (Route, bool) {
	if !p.started {
		p.started = true
		if r, ok := p.policy.Pick(p.info, p.metadata); ok {
			p.picked, p.hasPick = r, true
			return r, true
		}
	}
	if p.fallback == nil {
		p.fallback = p.policy.Fallback(p.info, p.metadata)
		if p.fallback == nil {
			return Route{}, false
		}
	}
	for {
		r, ok := p.fallback.Next()
		if !ok {
			return Route{}, false
		}
		if p.hasPick && r.shard == p.picked.shard && r.HostID() == p.picked.HostID() {
			continue
		}
		return r, true
	}
}

// PinnedPlan initializes plan as the plan of a request pinned to the host
// info.HostID, and to the shard info.ShardID when set: that one route, when
// the host is known and enabled, and no other. info must name a host.
func PinnedPlan(info routing.Info, m *MetadataSnapshot, plan *InternalLazyRoutingPlan) QueryPlan {
	if m == nil {
		return &InternalTargetPlan{}
	}
	// A pinned plan follows no policy: it is never skipped for latency and
	// never traverses replicas or leaders. Only the captured destination
	// is kept, in the leader slot.
	*plan = InternalLazyRoutingPlan{metadata: m, index: m.routingIndex(), pinned: true}
	plan.init(info)
	plan.leader = cql.Option[Target]{Value: Target{HostID: info.HostID.Value}, Set: true}
	return plan
}
