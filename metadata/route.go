// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"math"
	"unsafe"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

// anyShard is a Route's shard when any connection of its host will do. It
// matches internal/routing.AnyShard, which a test checks.
const anyShard int32 = -1

// Route is a destination a routing plan resolved fully: a host and,
// optionally, one of its shards. A request takes a connection for the
// route's shard of its host, or any connection of the host when the route
// has no shard, and does not move to another shard or host on its own;
// moving on is the plan's next Route. Policies take routes from a Snapshot
// through routing.HostRoutes, whose routes have no shard, and
// routing.ReplicaRoutes, whose routes have a shard when tablet placement
// names one, and narrow them with WithShard and ShardOf. The zero Route has
// no host.
type Route struct {
	// host is the session's handle of the host. The engine reads it
	// through the mirror in internal/routing.
	host  unsafe.Pointer
	shard int32
}

// HostID returns the ID of the route's host.
func (r Route) HostID() cql.UUID {
	if r.host == nil {
		return cql.UUID{}
	}
	id, _ := link.Get[func(unsafe.Pointer) (cql.UUID, bool)](link.RouteHost)(r.host)
	return id
}

// Usable reports whether the route's host is up and has connections.
func (r Route) Usable() bool {
	if r.host == nil {
		return false
	}
	_, usable := link.Get[func(unsafe.Pointer) (cql.UUID, bool)](link.RouteHost)(r.host)
	return usable
}

// Shard returns the route's shard, unset when any connection of its host
// will do or the route has no host.
func (r Route) Shard() cql.Option[uint32] {
	if r.host == nil {
		return cql.Option[uint32]{}
	}
	return shardOption(r.shard)
}

// ShardOf returns the shard owning token on the route's host, unset when its
// sharding is not known.
func (r Route) ShardOf(token int64) cql.Option[uint32] {
	if r.host == nil {
		return cql.Option[uint32]{}
	}
	return shardOption(link.Get[func(unsafe.Pointer, int64) int32](link.RouteShardOf)(r.host, token))
}

// WithShard returns the route to shard of the same host. An unset shard, or
// one above math.MaxInt32, which no server has, routes to any connection of
// the host. ShardOf's result is a valid argument.
func (r Route) WithShard(shard cql.Option[uint32]) Route {
	s := anyShard
	if shard.Set && shard.Value <= math.MaxInt32 {
		s = int32(shard.Value)
	}
	return Route{host: r.host, shard: s}
}

// Target describes the route.
func (r Route) Target() Target {
	return Target{HostID: r.HostID(), ShardID: r.Shard()}
}

func shardOption(shard int32) cql.Option[uint32] {
	if shard < 0 {
		return cql.Option[uint32]{}
	}
	return cql.Option[uint32]{Value: uint32(shard), Set: true}
}
