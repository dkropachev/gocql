// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"unsafe"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// The bridge between the session's routing and packages routing and
// metadata: the engine works on Route, RoutingPolicy and QueryPlan, an
// application on metadata.Route, routing.Policy and routing.Plan.
//
// unsafe: metadata.Route's unsafe.Pointer mirrors Route's *pool.HostRoute,
// so the conversions in this file (PublicRoute, EngineRoute, PublicRoutes)
// keep the host visible to the GC. The RouteHost and RouteShardOf links
// receive only pointers that came from a Route.

// PublicRoute returns r as a metadata.Route; the two share a layout, which
// TestMirrorLayouts checks.
func PublicRoute(r Route) metadata.Route { return *(*metadata.Route)(unsafe.Pointer(&r)) }

// EngineRoute returns r as a Route.
func EngineRoute(r metadata.Route) Route { return *(*Route)(unsafe.Pointer(&r)) }

// PublicRoutes returns routes as metadata.Routes, sharing their storage.
func PublicRoutes(routes []Route) []metadata.Route {
	return unsafe.Slice((*metadata.Route)(unsafe.Pointer(unsafe.SliceData(routes))), len(routes))
}

// EngineOf returns the engine policy of p: the session's own for a policy
// routing.NewPolicy or routing.SingleTarget made, an adapter for an
// application's.
func EngineOf(p routing.Policy) RoutingPolicy {
	if p == nil {
		return nil
	}
	if impl, ok := builtinPolicy(p); ok {
		if v, ok := impl.(publicPolicy); ok {
			return v.p
		}
	}
	if v, ok := p.(publicPolicy); ok {
		return v.p
	}
	return applicationPolicy{p}
}

// PlanFor returns the plan a request described by info follows under the
// application's policy p, given m: Pick's route first, then Fallback's. It
// replaces the public routing.NewPlan for tests that inspect a policy's plan.
func PlanFor(p routing.Policy, info routing.Info, m *metadata.Snapshot) routing.Plan {
	return publicPlan{NewPlan(EngineOf(p), info, SnapshotOf(m))}
}

// InternalBuiltin returns the session's default policy behind p, if p is one.
func InternalBuiltin(p routing.Policy) (*InternalRoutingPolicy, bool) {
	b, ok := EngineOf(p).(*InternalRoutingPolicy)
	return b, ok
}

var builtinPolicy = link.Get[func(routing.Policy) (routing.Policy, bool)](link.BuiltinPolicy)

func init() {
	link.Set(link.BuildPolicy, func(single bool, options routing.Options, target metadata.Target) routing.Policy {
		if single {
			return publicPolicy{SingleTargetRouting(target)}
		}
		p, err := NewRoutingPolicy(options)
		if err != nil {
			// routing.NewPolicy validated the options already.
			panic(err)
		}
		return publicPolicy{p}
	})
	link.Set(link.HostRoutes, func(m *metadata.Snapshot) []metadata.Route {
		return PublicRoutes(SnapshotOf(m).HostRoutes())
	})
	link.Set(link.ReplicaRoutes, func(m *metadata.Snapshot, info routing.Info) []metadata.Route {
		return PublicRoutes(SnapshotOf(m).ReplicaRoutes(info))
	})
	link.Set(link.RouteHost, func(host unsafe.Pointer) (cql.UUID, bool) {
		h := (*pool.HostRoute)(host)
		return h.ID(), h.Usable()
	})
	link.Set(link.RouteShardOf, func(host unsafe.Pointer, token int64) int32 {
		return (*pool.HostRoute)(host).ShardOf(token)
	})
}

// publicPolicy is an engine policy seen as a routing.Policy.
type publicPolicy struct{ p RoutingPolicy }

func (v publicPolicy) Validate(m *metadata.Snapshot) error { return v.p.Validate(SnapshotOf(m)) }

func (v publicPolicy) Pick(info routing.Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := v.p.Pick(info, SnapshotOf(m))
	return PublicRoute(r), ok
}

func (v publicPolicy) Fallback(info routing.Info, m *metadata.Snapshot) routing.Plan {
	return publicPlan{v.p.Fallback(info, SnapshotOf(m))}
}

func (v publicPolicy) Observe(e routing.AttemptEvent) { v.p.Observe(e) }

// applicationPolicy is an application's routing.Policy seen as an engine
// policy.
type applicationPolicy struct{ p routing.Policy }

func (a applicationPolicy) Validate(m *MetadataSnapshot) error { return a.p.Validate(m.Public()) }

func (a applicationPolicy) Pick(info routing.Info, m *MetadataSnapshot) (Route, bool) {
	r, ok := a.p.Pick(info, m.Public())
	return EngineRoute(r), ok
}

func (a applicationPolicy) Fallback(info routing.Info, m *MetadataSnapshot) QueryPlan {
	plan := a.p.Fallback(info, m.Public())
	if v, ok := plan.(publicPlan); ok {
		return v.p
	}
	if plan == nil {
		return nil
	}
	return applicationPlan{plan}
}

func (a applicationPolicy) Observe(e routing.AttemptEvent) { a.p.Observe(e) }

// publicPlan is an engine plan seen as a routing.Plan.
type publicPlan struct{ p QueryPlan }

func (v publicPlan) Next() (metadata.Route, bool) {
	if v.p == nil {
		return metadata.Route{}, false
	}
	r, ok := v.p.Next()
	return PublicRoute(r), ok
}

// applicationPlan is an application's routing.Plan seen as an engine plan.
type applicationPlan struct{ p routing.Plan }

func (a applicationPlan) Next() (Route, bool) {
	r, ok := a.p.Next()
	return EngineRoute(r), ok
}

// PublicPolicy returns p as a routing.Policy.
func PublicPolicy(p RoutingPolicy) routing.Policy {
	switch v := p.(type) {
	case nil:
		return nil
	case applicationPolicy:
		return v.p
	}
	return publicPolicy{p}
}
