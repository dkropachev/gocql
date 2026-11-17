// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// HostSnapshot is what a session knows about a host: its address, place in
// the cluster, tokens, sharding and state.
type HostSnapshot struct {
	Address          netip.AddrPort
	Datacenter, Rack string
	Tokens           []int64
	Shards           uint32
	// ShardingIgnoreMSB is how many most significant token bits the host
	// ignores when mapping a token to a shard.
	ShardingIgnoreMSB      uint8
	ID                     cql.UUID
	Enabled, Connected, Up bool
}
