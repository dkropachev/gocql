// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import "fmt"

// Mode selects how much schema a session loads. Topology (hosts, tokens and
// tablets) loads in every mode.
type Mode uint8

// The metadata modes.
const (
	// SchemaNone loads no schema.
	SchemaNone Mode = iota
	// SchemaKeyspaces loads keyspaces (replication, durable writes and
	// ScyllaDB keyspace options) without their tables, views, types,
	// functions or aggregates.
	SchemaKeyspaces
	// SchemaFull loads keyspaces with everything in them. It is the default.
	SchemaFull
)

// String returns the metadata mode name, or Mode(n) for a value it does not
// define.
func (m Mode) String() string {
	switch m {
	case SchemaNone:
		return "none"
	case SchemaKeyspaces:
		return "keyspaces"
	case SchemaFull:
		return "full"
	default:
		return fmt.Sprintf("Mode(%d)", uint8(m))
	}
}
