// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "fmt"

// StatementKind is an explicit classification; the driver never parses CQL to
// infer it. The zero StatementKind is UnknownStatement. Later releases may add
// values, so a switch over them needs a default case.
type StatementKind uint8

// Statement kinds.
const (
	// UnknownStatement is a statement the application did not classify. The
	// driver treats it as possibly conditional: it sends no client
	// timestamp, tries replicas in a fixed order without shuffling, keeps a
	// strict shard pin, and never speculates. ScyllaDB reports whether a
	// prepared statement is conditional, which replaces UnknownStatement;
	// Cassandra does not. A statement whose row type is EmptyRow reads no
	// rows, so it executes as an OrdinaryStatement, prepared or not; as a
	// batch member it stays unclassified.
	UnknownStatement StatementKind = iota

	// OrdinaryStatement is a statement known not to be conditional.
	OrdinaryStatement

	// ConditionalStatement is a conditional (lightweight transaction) statement,
	// which returns an [applied] row.
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
