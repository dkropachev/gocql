// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "fmt"

// ExecutionStatus reports what a request may have done on the cluster.
// Compare a status with == or a switch, never with < or >: the numeric order
// of the values is not their severity, which is NotExecuted, then
// MaybeExecuted, then PartiallyExecuted, then Executed. The zero
// ExecutionStatus is MaybeExecuted, the safe assumption. Later releases may
// add values, so a switch over them needs a default case.
type ExecutionStatus uint8

const (
	// MaybeExecuted means the request reached a server and may or may not
	// have been applied.
	MaybeExecuted ExecutionStatus = iota
	// NotExecuted guarantees the request had no effect: it never reached a
	// server, or the server rejected it before applying anything.
	NotExecuted
	// PartiallyExecuted means the request was applied at least in part: a
	// write that some replicas acknowledged before it timed out or failed,
	// or an unlogged or counter batch that may have applied some of its
	// statements. Treat it as executed for work that is not idempotent: do
	// not send it again without reading back what it changed.
	PartiallyExecuted
	// Executed means the request succeeded: ExecutionStatusOf reports it
	// for a nil error.
	Executed
)

// String returns the status name, or ExecutionStatus(n) for a value it does
// not define.
func (e ExecutionStatus) String() string {
	switch e {
	case NotExecuted:
		return "not executed"
	case MaybeExecuted:
		return "maybe executed"
	case PartiallyExecuted:
		return "partially executed"
	case Executed:
		return "executed"
	default:
		return fmt.Sprintf("ExecutionStatus(%d)", uint8(e))
	}
}
