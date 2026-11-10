// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import "github.com/scylladb/gocql/v2/internal/model"

// NewPlan returns the plan a request follows under policy: the route
// policy.Pick gives, then the routes of policy.Fallback except that one.
// Fallback is called only when the plan needs a second route, or a first
// one when Pick returns false.
func NewPlan(policy RoutingPolicy, info model.RoutingInfo, m *MetadataSnapshot) QueryPlan {
	switch p := policy.(type) {
	case *InternalRoutingPolicy:
		// Built-in routing picks and falls back within one cursor.
		return p.InternalPlanInto(info, m, new(InternalLazyRoutingPlan))
	case *singleTargetRouting:
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
	info     model.RoutingInfo
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
		if p.hasPick && r.Shard == p.picked.Shard && r.HostID() == p.picked.HostID() {
			continue
		}
		return r, true
	}
}
