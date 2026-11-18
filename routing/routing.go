// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package routing chooses the hosts and shards a request is sent to.
//
// A Policy picks the routes of each request from a metadata.Snapshot.
// NewPolicy builds the default token- and tablet-aware policy and
// SingleTarget one that always routes to a single host; both are implemented
// by the session. An application can implement Policy itself, often by
// wrapping a built-in one. In a program that links no session, such as the
// tests of a policy library, the built-in policies find no route and
// Validate fails, and HostRoutes and ReplicaRoutes return no routes.
package routing

import (
	"errors"
	"sync"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

// Plan returns the routes a request tries, in order, until one of them has a
// connection.
type Plan interface{ Next() (metadata.Route, bool) }

// Policy chooses the routes of a request in two steps, optimized for the
// happy path where the first route serves it:
//
//   - Pick returns the first route, cheaply: without building a plan and
//     without allocating. It returns false when it has no cheap answer, and
//     the request then starts with Fallback's routes.
//   - Fallback returns every route the request may take, in order. The
//     driver calls it only when the picked route fails or Pick returned
//     false, and skips the picked route if Fallback returns it again.
//
// Validate checks the policy against a snapshot before the session uses it,
// and Observe receives the outcome of every attempt. The session combines
// Pick and Fallback into the plan a request follows. Methods may be called
// concurrently.
type Policy interface {
	Validate(*metadata.Snapshot) error
	Pick(Info, *metadata.Snapshot) (metadata.Route, bool)
	Fallback(Info, *metadata.Snapshot) Plan
	Observe(AttemptEvent)
}

// NewPolicy builds the default token- and tablet-aware policy with options.
// It reports invalid options at once; the session builds the policy itself
// when it first uses it.
func NewPolicy(options Options) (Policy, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	return &builtinPolicy{options: options}, nil
}

// SingleTarget routes every request to target. When target.StrictShard is
// set, a request goes to target's shard only.
func SingleTarget(target metadata.Target) Policy {
	return &builtinPolicy{single: true, target: target}
}

// HostRoutes returns a route to every host of snapshot, to any shard.
func HostRoutes(snapshot *metadata.Snapshot) []metadata.Route {
	hostRoutes, ok := link.Lookup[func(*metadata.Snapshot) []metadata.Route](link.HostRoutes)
	if !ok {
		return nil
	}
	return hostRoutes(snapshot)
}

// ReplicaRoutes returns routes to the replicas of the request info describes:
// to its token's replicas, or to every host when info has no token.
func ReplicaRoutes(snapshot *metadata.Snapshot, info Info) []metadata.Route {
	replicaRoutes, ok := link.Lookup[func(*metadata.Snapshot, Info) []metadata.Route](link.ReplicaRoutes)
	if !ok {
		return nil
	}
	return replicaRoutes(snapshot, info)
}

// PlanOf returns a plan of routes, in order. The plan keeps the slice
// without copying it, so the caller must not modify it afterwards. A plan is followed by
// one request, so it is not safe for concurrent use.
func PlanOf(routes ...metadata.Route) Plan { return &slicePlan{routes: routes} }

type slicePlan struct{ routes []metadata.Route }

func (p *slicePlan) Next() (metadata.Route, bool) {
	if len(p.routes) == 0 {
		return metadata.Route{}, false
	}
	r := p.routes[0]
	p.routes = p.routes[1:]
	return r, true
}

// emptyPlan is the plan of a program without a session: it has no routes.
type emptyPlan struct{}

func (emptyPlan) Next() (metadata.Route, bool) { return metadata.Route{}, false }

// builtinPolicy is a policy the session implements. It builds the session's
// policy on first use, so a policy made during package initialization works
// whatever order packages initialize in.
type builtinPolicy struct {
	impl    Policy
	options Options
	target  metadata.Target
	once    sync.Once
	single  bool
}

func init() {
	link.Set(link.BuiltinPolicy, func(p Policy) (Policy, bool) {
		b, ok := p.(*builtinPolicy)
		if !ok {
			return nil, false
		}
		return b.policy(), true
	})
}

// policy returns the session's policy, or nil in a program without a
// session, whose package internal/routing fills link.BuildPolicy.
func (b *builtinPolicy) policy() Policy {
	build, ok := link.Lookup[func(single bool, options Options, target metadata.Target) Policy](link.BuildPolicy)
	if !ok {
		return nil
	}
	b.once.Do(func() { b.impl = build(b.single, b.options, b.target) })
	return b.impl
}

// errNoSession is what a built-in policy reports in a program without a
// session.
var errNoSession = &cql.ValidationError{Field: "routing", Err: errors.New("the built-in policies need a session")}

func (b *builtinPolicy) Validate(m *metadata.Snapshot) error {
	p := b.policy()
	if p == nil {
		return errNoSession
	}
	return p.Validate(m)
}

func (b *builtinPolicy) Pick(info Info, m *metadata.Snapshot) (metadata.Route, bool) {
	p := b.policy()
	if p == nil {
		return metadata.Route{}, false
	}
	return p.Pick(info, m)
}

func (b *builtinPolicy) Fallback(info Info, m *metadata.Snapshot) Plan {
	p := b.policy()
	if p == nil {
		return emptyPlan{}
	}
	return p.Fallback(info, m)
}

func (b *builtinPolicy) Observe(e AttemptEvent) {
	if p := b.policy(); p != nil {
		p.Observe(e)
	}
}
