// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"unsafe"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

// AnyShard is a Route's shard when any connection of its host will do.
const AnyShard int32 = -1

// Route is a destination a routing plan resolved fully: a host and a shard.
// A request takes a connection for Shard of the route's host and does not
// move to another shard or host on its own; moving on is the plan's next
// Route. Policies take routes from a Snapshot through routing.HostRoutes
// and routing.ReplicaRoutes. The zero Route has no host.
type Route struct {
	// host is the session's handle of the host. The engine reads it
	// through the mirror in internal/routing.
	host  unsafe.Pointer
	Shard int32
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

// ShardOf returns the shard owning token on the route's host, or AnyShard
// when its sharding is not known.
func (r Route) ShardOf(token int64) int32 {
	if r.host == nil {
		return AnyShard
	}
	return link.Get[func(unsafe.Pointer, int64) int32](link.RouteShardOf)(r.host, token)
}

// Target describes the route.
func (r Route) Target() Target {
	t := Target{HostID: r.HostID()}
	if r.Shard >= 0 {
		t.ShardID = cql.Option[uint32]{Value: uint32(r.Shard), Set: true}
	}
	return t
}

// WithShard returns the route to shard of the same host.
func (r Route) WithShard(shard int32) Route { return Route{host: r.host, Shard: shard} }
