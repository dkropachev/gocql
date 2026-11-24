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
	// ModeNone loads no schema.
	ModeNone Mode = iota
	// ModeKeyspaces loads keyspaces (replication, durable writes and
	// ScyllaDB keyspace options) without their tables, views, types,
	// functions or aggregates.
	ModeKeyspaces
	// ModeFull loads keyspaces with everything in them. It is the default.
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
