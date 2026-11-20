// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// HostSnapshot is what a session knows about a host: its address, place in
// the cluster, tokens, sharding and state.
//
// The driver fills it; its zero value is empty.
type HostSnapshot struct {
	// Address is the address and port the driver connects to, or the zero
	// AddrPort for a host reached over a Unix socket.
	Address netip.AddrPort
	// Datacenter and Rack are the host's location, as it advertises it.
	Datacenter, Rack string
	// Tokens lists the host's vnode tokens.
	Tokens []int64
	// Shards is the host's ScyllaDB shard count, or zero when it is not known
	// or the host is not sharded.
	Shards uint32
	// ShardingIgnoreMSB is how many most significant token bits the host
	// ignores when mapping a token to a shard.
	ShardingIgnoreMSB uint8
	// ID is the host's host ID.
	ID cql.UUID
	// Enabled reports that the session's host filter accepts the host,
	// Connected that its pool has at least one connection, and Up that the
	// driver considers the host up.
	Enabled, Connected, Up bool
}
