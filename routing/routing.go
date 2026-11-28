// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package routing chooses the hosts and shards a request is sent to.
//
// A Policy picks the routes of each request from a metadata.Snapshot.
// NewPolicy builds the default token- and tablet-aware policy and
// SingleTarget one that always routes to a single host; both are implemented
// by the session. An application can implement Policy itself, often by
// wrapping a built-in one, and test it on snapshots from package
// routing/routingtest. HostRoutes, ReplicaRoutes, AppendReplicaRoutes and
// RouteTo give a policy routes from a snapshot, and FilterPlan and Routes
// work on plans. In a program that links no session, such as the tests of a
// policy library that do not import routingtest, the built-in policies find
// no route and fail validation, and those functions return no routes.
package routing

import (
	"errors"
	"iter"
	"sync"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/metadata"
)

// Plan returns the routes a request tries, in order, until one of them has a
// connection.
type Plan interface {
	// Next returns the next route, and false when the plan has no more.
	Next() (metadata.Route, bool)
}

// Policy chooses the routes of a request in two steps, optimized for the
// happy path where the first route serves it: Pick returns the first route
// cheaply, and Fallback every route the request may take. The session
// combines them into the plan a request follows. A request pinned to a host
// (execopt.Options.HostID) goes there without consulting the policy. Methods
// may be called concurrently.
//
// A Policy may also implement Validator and Learner.
type Policy interface {
	// Pick returns the first route, cheaply: without building a plan and
	// without allocating. It returns false when it has no cheap answer, and
	// the request then starts with Fallback's routes.
	Pick(Info, *metadata.Snapshot) (metadata.Route, bool)
	// Fallback returns every route the request may take, in order. The
	// driver calls it only when the picked route fails or Pick returned
	// false, and skips the picked route if Fallback returns it again.
	Fallback(Info, *metadata.Snapshot) Plan
}

// Validator is implemented by a Policy that checks itself against the
// cluster. The session calls Validate once per snapshot before it routes a
// request with the policy, and again for the next snapshot; while Validate
// fails, so do the requests the policy would route. Connect fails when it
// fails for the default profile's policy.
type Validator interface {
	// Validate checks the policy against a snapshot.
	Validate(*metadata.Snapshot) error
}

// Learner is implemented by a Policy that learns from attempts, such as one
// that tracks latency. The session calls Learn with the outcome of every
// attempt of the requests of the profiles that use the policy.
type Learner interface {
	// Learn receives the outcome of an attempt.
	Learn(AttemptEvent)
}

// NewPolicy builds the default token- and tablet-aware policy with options;
// the session builds the policy itself when it first uses it. Invalid
// options are reported, as a cql.ValidationError naming the field, by
// config.ProfileBuilder.Build and config.ConfigBuilder.Build for a profile
// that uses the policy, and by its Validate, which also checks that the
// local datacenter and rack have eligible hosts.
func NewPolicy(options Options) *Builtin {
	return &Builtin{options: options}
}

// SingleTarget routes every request to target: to target's shard only when
// it names one, unless target.AnyShardFallback lets a request use another
// shard of the host. Its Validate checks that target is an eligible host. A
// request pinned with execopt.Options.HostID goes to that host instead, without
// consulting the policy, and with execopt.Options.AnyShardFallback, not
// target's, deciding whether it may leave its shard.
func SingleTarget(target metadata.Target) *Builtin {
	return &Builtin{single: true, target: target}
}

// HostRoutes returns a route to every host of snapshot, to any shard, in
// the order of metadata.Snapshot.Hosts. It includes hosts the session's host
// filter rejects (metadata.Route.Enabled) and hosts that are down or have no
// connection (metadata.Route.Usable); a request skips a route to a host the
// filter rejects.
func HostRoutes(snapshot *metadata.Snapshot) []metadata.Route {
	hostRoutes, ok := link.Lookup[func(*metadata.Snapshot) []metadata.Route](link.HostRoutes)
	if !ok {
		return nil
	}
	return hostRoutes(snapshot)
}

// ReplicaRoutes returns routes to the replicas of the token of the request
// info describes, each to the replica's shard: the one tablet placement
// names, or for a vnode replica the shard that owns the token on the host,
// from metadata.HostSnapshot.Shards. A route goes to any shard when the
// host's sharding is not known. It returns none when info has no token,
// keyspace or table, or when no replica of the token is known; HostRoutes
// then gives every host. Like HostRoutes, it includes replicas the host
// filter rejects and replicas that are down. It reads the snapshot's own
// placement, as metadata.Snapshot.Replicas does. AppendReplicaRoutes does the
// same without allocating, for a policy's Pick.
func ReplicaRoutes(snapshot *metadata.Snapshot, info Info) []metadata.Route {
	return AppendReplicaRoutes(nil, snapshot, info)
}

// AppendReplicaRoutes appends the routes ReplicaRoutes returns to dst and
// returns the extended slice, so that a policy can reuse a buffer across
// requests. Pick and Fallback may run concurrently, so a buffer must not be
// shared between calls in flight: take one from a sync.Pool, for example.
func AppendReplicaRoutes(dst []metadata.Route, snapshot *metadata.Snapshot, info Info) []metadata.Route {
	replicaRoutes, ok := link.Lookup[func([]metadata.Route, *metadata.Snapshot, Info) []metadata.Route](link.ReplicaRoutes)
	if !ok {
		return dst
	}
	return replicaRoutes(dst, snapshot, info)
}

// RouteTo returns the route to target: to its shard when ShardID is set,
// else to any shard of its host. With AnyShardFallback, a shard the host
// does not have, by its HostSnapshot.Shards in snapshot, or any shard of a
// host whose sharding is not known, routes to any shard of the host, as
// SingleTarget does. A route names one shard, so it cannot itself fall back
// from a shard that has no connection: a plan that should, as SingleTarget's
// does, follows the route with RouteTo of the target without its ShardID.
// RouteTo returns false when snapshot has no host with target.HostID, which a
// policy pinning requests should treat as a route it cannot take.
func RouteTo(snapshot *metadata.Snapshot, target metadata.Target) (metadata.Route, bool) {
	routeTo, ok := link.Lookup[func(*metadata.Snapshot, cql.UUID) (metadata.Route, bool)](link.RouteTo)
	if !ok {
		return metadata.Route{}, false
	}
	r, ok := routeTo(snapshot, target.HostID)
	if !ok {
		return metadata.Route{}, false
	}
	shard := target.ShardID
	if target.AnyShardFallback && shard.Set {
		if h, ok := snapshot.HostInfo(target.HostID); !ok || h.Shards == 0 || shard.Value >= h.Shards {
			shard = cql.Option[uint32]{}
		}
	}
	return r.WithShardID(shard), true
}

// Routes returns the routes plan has left, in order, as an iterator that
// consumes the plan.
func Routes(plan Plan) iter.Seq[metadata.Route] {
	return func(yield func(metadata.Route) bool) {
		for r, ok := plan.Next(); ok; r, ok = plan.Next() {
			if !yield(r) {
				return
			}
		}
	}
}

// FilterPlan returns a plan of the routes of plan that keep reports true for,
// in order. It reads plan lazily, one route per Next, so a policy can drop
// hosts from a built-in plan without building the whole plan.
func FilterPlan(plan Plan, keep func(metadata.Route) bool) Plan {
	return &filterPlan{plan: plan, keep: keep}
}

type filterPlan struct {
	plan Plan
	keep func(metadata.Route) bool
}

func (p *filterPlan) Next() (metadata.Route, bool) {
	for {
		r, ok := p.plan.Next()
		if !ok || p.keep(r) {
			return r, ok
		}
	}
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

// Builtin is a policy the session implements, from NewPolicy or
// SingleTarget. It implements Policy, Validator and Learner, so a
// policy that embeds a *Builtin to override Pick or Fallback keeps the
// built-in validation and latency tracking without forwarding them.
//
// The zero Builtin is the default policy, as NewPolicy(Options{}) returns. A
// Builtin builds the session's policy on first use, so one made during package
// initialization works whatever order packages initialize in. It must not be
// copied after first use; share a *Builtin instead.
type Builtin struct {
	impl    Policy
	options Options
	target  metadata.Target
	once    sync.Once
	single  bool
}

var (
	_ Policy    = (*Builtin)(nil)
	_ Validator = (*Builtin)(nil)
	_ Learner   = (*Builtin)(nil)
)

func init() {
	link.Set(link.BuiltinPolicy, func(p Policy) (Policy, bool) {
		b, ok := p.(*Builtin)
		if !ok {
			return nil, false
		}
		return b.policy(), true
	})
}

// policy returns the session's policy, or nil in a program without a
// session, whose package internal/routing fills link.BuildPolicy.
func (b *Builtin) policy() Policy {
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

// Validate checks the policy against a snapshot: that the local datacenter
// and rack, or the single target, have eligible hosts. In a program without a
// session it reports a cql.ValidationError.
func (b *Builtin) Validate(m *metadata.Snapshot) error {
	p := b.policy()
	if p == nil {
		return errNoSession
	}
	return p.(Validator).Validate(m)
}

// Pick returns the first route of the built-in plan, without building it.
func (b *Builtin) Pick(info Info, m *metadata.Snapshot) (metadata.Route, bool) {
	p := b.policy()
	if p == nil {
		return metadata.Route{}, false
	}
	return p.Pick(info, m)
}

// Fallback returns every route of the built-in plan, in order.
func (b *Builtin) Fallback(info Info, m *metadata.Snapshot) Plan {
	p := b.policy()
	if p == nil {
		return emptyPlan{}
	}
	return p.Fallback(info, m)
}

// Learn feeds an attempt's outcome to latency-aware routing.
func (b *Builtin) Learn(e AttemptEvent) {
	if p := b.policy(); p != nil {
		p.(Learner).Learn(e)
	}
}
