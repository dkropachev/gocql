// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

// ExecutionStatus reports what a failed request may have done on the
// cluster. The zero value is MaybeExecuted, the conservative answer.
type ExecutionStatus uint8

const (
	// MaybeExecuted means the outcome is unknown: the request reached a
	// server and may or may not have been applied.
	MaybeExecuted ExecutionStatus = iota
	// NotExecuted guarantees the request had no effect: it never reached a
	// server, or the server rejected it before applying anything.
	NotExecuted
	// PartiallyExecuted means the request was applied at least in part: a
	// write reached some replicas without the requested consistency, or a
	// schema change succeeded but its follow-up work failed.
	PartiallyExecuted
)

func (e ExecutionStatus) String() string {
	switch e {
	case NotExecuted:
		return "not executed"
	case MaybeExecuted:
		return "maybe executed"
	case PartiallyExecuted:
		return "partially executed"
	default:
		return "unknown execution status"
	}
}

// severity orders statuses: NotExecuted < MaybeExecuted < PartiallyExecuted.
func (e ExecutionStatus) severity() int {
	switch e {
	case NotExecuted:
		return 0
	case PartiallyExecuted:
		return 2
	default:
		return 1
	}
}

// Worse returns the more severe of e and o.
func (e ExecutionStatus) Worse(o ExecutionStatus) ExecutionStatus {
	if o.severity() > e.severity() {
		return o
	}
	return e
}
