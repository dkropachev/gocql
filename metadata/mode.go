// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import "fmt"

// Mode selects how much schema a session loads. Topology (hosts, tokens and
// tablets) loads in every mode. The zero Mode is ModeNone. Later releases may
// add values, so a switch over them needs a default case.
type Mode uint8

// The metadata modes.
const (
	// ModeNone loads no schema. Without keyspace replication the session
	// knows no replicas, so routing is neither token- nor tablet-aware: the
	// built-in policy routes to hosts by locality only, and
	// Snapshot.Replicas, Snapshot.Token and routing.ReplicaRoutes find
	// nothing.
	ModeNone Mode = iota
	// ModeKeyspaces loads keyspaces (replication, durable writes and
	// ScyllaDB keyspace options) without their tables, views, types,
	// functions or aggregates. A vnode keyspace's tables route by the
	// keyspace's token ring, to the replica and shard that own the token.
	// A tablet keyspace's tables are not routed to their replicas, since
	// tablet placement is kept per table, and Snapshot.Replicas,
	// Snapshot.Token and Snapshot.TokenRanges need the table's schema.
	ModeKeyspaces
	// ModeFull loads keyspaces with everything in them, which token- and
	// tablet-aware routing use fully. It is the default.
	ModeFull
)

// String returns the metadata mode name, or Mode(n) for a value it does not
// define.
func (m Mode) String() string {
	switch m {
	case ModeNone:
		return "none"
	case ModeKeyspaces:
		return "keyspaces"
	case ModeFull:
		return "full"
	default:
		return fmt.Sprintf("Mode(%d)", uint8(m))
	}
}
