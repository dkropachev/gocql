// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package routing chooses the hosts and shards a request is sent to.
//
// A Policy picks the routes of each request from a metadata.Snapshot.
// NewPolicy builds the default token- and tablet-aware policy and
// SingleTarget one that always routes to a single host; both are implemented
// by the session. An application can implement Policy itself, often by
// wrapping a built-in one.
package routing

import (
	"sync"

	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

// AnyShard is a Route's shard when any connection of its host will do.
const AnyShard = metadata.AnyShard

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
// and Observe receives the outcome of every attempt. NewPlan combines Pick
// and Fallback into the plan a request follows. Methods may be called
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

// SingleTarget routes every request to target. With strictShard, or when
// target.StrictShard is set, a request goes to target's shard only.
func SingleTarget(target metadata.Target, strictShard bool) Policy {
	return &builtinPolicy{single: true, target: target, strict: strictShard}
}

// NewPlan returns the routes policy would try for a request described by
// info, given snapshot.
func NewPlan(policy Policy, info Info, snapshot *metadata.Snapshot) Plan {
	return link.Get[func(Policy, Info, *metadata.Snapshot) Plan](link.NewPlan)(policy, info, snapshot)
}

// HostRoutes returns a route to every host of snapshot, to any shard.
func HostRoutes(snapshot *metadata.Snapshot) []metadata.Route {
	return link.Get[func(*metadata.Snapshot) []metadata.Route](link.HostRoutes)(snapshot)
}

// ReplicaRoutes returns routes to the replicas of the request info describes:
// to its token's replicas, or to every host when info has no token.
func ReplicaRoutes(snapshot *metadata.Snapshot, info Info) []metadata.Route {
	return link.Get[func(*metadata.Snapshot, Info) []metadata.Route](link.ReplicaRoutes)(snapshot, info)
}

// builtinPolicy is a policy the session implements. It builds the session's
// policy on first use, so a policy made during package initialization works
// whatever order packages initialize in.
type builtinPolicy struct {
	impl    Policy
	options Options
	target  metadata.Target
	once    sync.Once
	single  bool
	strict  bool
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

func (b *builtinPolicy) policy() Policy {
	b.once.Do(func() {
		build := link.Get[func(single bool, options Options, target metadata.Target, strict bool) Policy](link.BuildPolicy)
		b.impl = build(b.single, b.options, b.target, b.strict)
	})
	return b.impl
}

func (b *builtinPolicy) Validate(m *metadata.Snapshot) error { return b.policy().Validate(m) }

func (b *builtinPolicy) Pick(info Info, m *metadata.Snapshot) (metadata.Route, bool) {
	return b.policy().Pick(info, m)
}

func (b *builtinPolicy) Fallback(info Info, m *metadata.Snapshot) Plan {
	return b.policy().Fallback(info, m)
}

func (b *builtinPolicy) Observe(e AttemptEvent) { b.policy().Observe(e) }
