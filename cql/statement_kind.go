// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "fmt"

// StatementKind is an explicit classification; the driver never parses CQL to infer it.
type StatementKind uint8

const (
	UnknownStatement StatementKind = iota
	OrdinaryStatement
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
