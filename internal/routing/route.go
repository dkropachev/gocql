// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"math"

	pool "github.com/scylladb/gocql/v2/internal/pool"
)

// AnyShard is a Route's shard when any connection of its host will do.
const AnyShard = pool.AnyShard

// Route is a destination a routing plan resolved fully: a host and a shard.
// A request takes a connection for Shard of the route's host and does not
// move to another shard or host on its own; moving on is the plan's next
// Route. Policies take routes from a MetadataSnapshot (HostRoutes, ReplicaRoutes).
type Route struct {
	host  *pool.HostRoute
	Shard int32
}

// InternalNewRoute returns a route to shard of host.
func InternalNewRoute(host *pool.HostRoute, shard int32) Route {
	return Route{host: host, Shard: shard}
}

// InternalRouteOf returns a route to t's host and shard that belongs to no
// session; a request resolves its host through its session's pools.
func InternalRouteOf(t Target) Route {
	shard := AnyShard
	if t.ShardID.Set && t.ShardID.Value <= math.MaxInt32 {
		shard = int32(t.ShardID.Value)
	}
	return Route{host: pool.NewHostRoute(t.HostID), Shard: shard}
}

// InternalHost returns the route's host handle.
func (r Route) InternalHost() *pool.HostRoute { return r.host }

// HostID returns the ID of the route's host.
func (r Route) HostID() UUID {
	if r.host == nil {
		return UUID{}
	}
	return r.host.ID()
}

// Target describes the route.
func (r Route) Target() Target {
	t := Target{HostID: r.HostID()}
	if r.Shard >= 0 {
		t.ShardID = Option[uint32]{Value: uint32(r.Shard), Set: true}
	}
	return t
}

// WithShard returns the route to shard of the same host.
func (r Route) WithShard(shard int32) Route { return Route{host: r.host, Shard: shard} }

// ShardOf returns the shard owning token on the route's host, or AnyShard
// when its sharding is not known.
func (r Route) ShardOf(token int64) int32 {
	if r.host == nil {
		return AnyShard
	}
	return r.host.ShardOf(token)
}

// Usable reports whether the route's host is up and has connections.
func (r Route) Usable() bool { return r.host != nil && r.host.Usable() }

// shardOf returns the shard owning token on a host of shards shards that
// ignores msbIgnore most significant token bits.
func shardOf(token int64, shards uint32, msbIgnore uint8) int32 {
	z := uint64(token+math.MinInt64) << msbIgnore
	sum := ((z & 0xffffffff) * uint64(shards) >> 32) + (z>>32)*uint64(shards)
	return int32(sum >> 32)
}

// HostRoutes returns a route to every host of the snapshot, for any shard.
func (m *MetadataSnapshot) HostRoutes() []Route {
	if m == nil {
		return nil
	}
	x := m.routingIndex()
	out := make([]Route, len(x.routes))
	for i, h := range x.routes {
		out[i] = Route{host: h, Shard: AnyShard}
	}
	return out
}

// ReplicaRoutes returns a route to each replica of info's token, to its
// shard when placement names one and to any shard otherwise.
func (m *MetadataSnapshot) ReplicaRoutes(info RoutingInfo) []Route {
	if m == nil || !info.Token.Set || info.Keyspace == "" || info.Table == "" {
		return nil
	}
	lookup := InternalLookupTablet(m, info)
	replicas, _ := m.lookupReplicas(&lookup, info.Keyspace, info.Table, info.Token.Value)
	x := m.routingIndex()
	out := make([]Route, 0, len(replicas))
	for _, t := range replicas {
		i, ok := x.byID[t.HostID]
		if !ok {
			continue
		}
		r := Route{host: x.routes[i], Shard: AnyShard}
		if t.ShardID.Set && t.ShardID.Value <= math.MaxInt32 {
			r.Shard = int32(t.ShardID.Value)
		}
		out = append(out, r)
	}
	return out
}

// InternalNextTarget returns the target of plan's next route, leaving out
// the route to any shard of the same host that a built-in plan returns
// after a route to a shard. The target is strict when no such route follows.
func InternalNextTarget(plan QueryPlan) (Target, bool) {
	r, ok := plan.Next()
	if !ok {
		return Target{}, false
	}
	t := r.Target()
	strict := true
	switch p := plan.(type) {
	case *InternalLazyRoutingPlan:
		if p.fallback.host != nil {
			p.fallback, strict = Route{}, false
		}
	case *InternalTargetPlan:
		if p.fallback {
			p.fallback, strict = false, false
		}
	}
	t.StrictShard = t.ShardID.Set && strict
	return t, true
}
