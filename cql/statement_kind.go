// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "fmt"

// StatementKind classifies a statement as ordinary or conditional (a
// lightweight transaction). The zero StatementKind is UnknownStatement, which
// the driver resolves when the statement is prepared or, unprepared, bound;
// see UnknownStatement. Later releases may add values, so a switch over them
// needs a default case.
type StatementKind uint8

// Statement kinds.
const (
	// UnknownStatement is a statement the application did not classify. The
	// driver infers its kind from the statement: it is conditional when its
	// Rows is a CASRows decoder, or when its CQL is an INSERT, UPDATE,
	// DELETE or BATCH with an IF outside string literals, quoted identifiers
	// and comments; otherwise it is ordinary, a SELECT included. ScyllaDB
	// also reports whether a prepared statement is conditional, which makes
	// it conditional.
	UnknownStatement StatementKind = iota

	// OrdinaryStatement is a statement known not to be conditional. One the
	// driver would infer to be conditional, by its CASRows decoder or an IF
	// in its CQL, is rejected with a ValidationError before it is sent.
	OrdinaryStatement

	// ConditionalStatement is a conditional (lightweight transaction) statement,
	// which returns an [applied] row. The driver sends it no client
	// timestamp, tries replicas in a fixed order without shuffling, keeps a
	// strict shard pin, and never speculates it. Only an INSERT, UPDATE,
	// DELETE or BATCH can be one: any other statement, a SELECT included,
	// declared ConditionalStatement or decoded with CASRows is rejected with a
	// ValidationError before it is sent.
	ConditionalStatement
)

// String returns the statement kind name, or StatementKind(n) for a value it
// does not define.
func (k StatementKind) String() string {
	switch k {
	case UnknownStatement:
		return "unknown"
	case OrdinaryStatement:
		return "ordinary"
	case ConditionalStatement:
		return "conditional"
	default:
		return fmt.Sprintf("StatementKind(%d)", uint8(k))
	}
}
