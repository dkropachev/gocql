// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
)

type executionStatusReporter interface {
	error
	ExecutionStatus() ExecutionStatus
}

// statusWalk is the state of one ExecutionStatusOf walk over an error tree.
type statusWalk struct {
	reporter    executionStatusReporter
	server      ServerError
	notExecuted bool
}

// visit inspects one error of the tree, not what it wraps, and reports
// whether the walk must go on.
func (w *statusWalk) visit(err error) bool {
	if reporter, ok := err.(executionStatusReporter); ok { //nolint:errorlint // one node of a tree walkErrors walks
		w.reporter = reporter // the first one decides
		return false
	}
	if w.server == nil {
		w.server, _ = err.(ServerError) //nolint:errorlint // one node of a tree walkErrors walks
	}
	if !w.notExecuted {
		w.notExecuted = unsent(err)
	}
	return true
}

// unsent reports whether err proves that a request was never sent. Only
// conditions raised before anything is sent do: validation and codec errors
// also arise from decoding a response, after the server executed the
// request.
func unsent(err error) bool {
	if _, ok := err.(*SchemaChangedError); ok { //nolint:errorlint // one node of a tree walkErrors walks
		return true
	}
	switch err {
	case ErrNoConnections, ErrSessionClosed, ErrSessionNotReady, ErrBusy, ErrConditionalExec, //nolint:errorlint // one node of a tree walkErrors walks
		context.Canceled, context.DeadlineExceeded:
		return true
	}
	return false
}

// ExecutionStatusOf reports what the request that failed with err may have
// done on the cluster, across all of its attempts. It returns NotExecuted for
// a nil error and MaybeExecuted for an error the driver cannot attribute.
func ExecutionStatusOf(err error) ExecutionStatus {
	if err == nil {
		return NotExecuted
	}
	var w statusWalk
	walkErrors(err, nil, &w)
	switch {
	case w.reporter != nil:
		return w.reporter.ExecutionStatus()
	case w.server != nil:
		return serverErrorExecutionStatus(w.server)
	case w.notExecuted:
		return NotExecuted
	default:
		return MaybeExecuted
	}
}

// AttemptFailure records one failed attempt of a request; see
// ExecutionError.Attempts.
//
// The driver fills it; its zero value is empty.
type AttemptFailure struct {
	Err error
	// Address is the host:port the attempt was sent to.
	Address string
	HostID  cql.UUID
	// Number is the 1-based attempt number.
	Number int
	// Shard is the shard of the connection, when the host is shard-aware.
	Shard       cql.Option[uint32]
	Consistency cql.Consistency
	Status      ExecutionStatus
	// CQLTraceID is the CQL trace session the server recorded for the
	// attempt, when it ran with CQL tracing and the server answered with an
	// error. Read it with package cqltrace.
	CQLTraceID cql.Option[cql.UUID]
}

func (a AttemptFailure) String() string {
	var b strings.Builder
	a.writeTarget(&b)
	fmt.Fprintf(&b, ": %v", a.Err)
	return b.String()
}

// writeTarget writes the attempt's number and where it ran: "#2 host:port",
// with "/shardN" on a shard-aware host.
func (a AttemptFailure) writeTarget(b *strings.Builder) {
	target := a.Address
	if target == "" {
		target = a.HostID.String()
	}
	fmt.Fprintf(b, "#%d %s", a.Number, target)
	if a.Shard.Set {
		fmt.Fprintf(b, "/shard%d", a.Shard.Value)
	}
}

// maxRenderedAttempts bounds how many attempts an error message lists.
const maxRenderedAttempts = 3

// ExecutionError is returned when a request fails after all its attempts.
//
// The driver returns it filled in; its zero value is an error without detail.
type ExecutionError struct {
	// Err is the reason the request stopped: the last attempt's error, the
	// caller's cancellation, or a failure before any attempt.
	Err error
	// Attempts lists the failed attempts, speculative attempts included, in
	// the order the request received their failures. When the caller's
	// context ended the request, the attempts it stopped waiting for come
	// last, in the order they started, with the context's error and
	// MaybeExecuted. Error names the last attempt's host with Err.
	Attempts []AttemptFailure
	Result   cql.Result
	status   ExecutionStatus
}

// NewExecutionError returns the failure of a request whose attempts reached
// status.
func newExecutionError(err error, result cql.Result, status ExecutionStatus, attempts []AttemptFailure) *ExecutionError {
	return &ExecutionError{Err: err, Result: result, status: status, Attempts: attempts}
}

func (e *ExecutionError) Error() string {
	var b strings.Builder
	b.WriteString("gocql: request: ")
	attempts := e.Attempts
	if n := len(attempts); n > 0 && errors.Is(e.Err, attempts[n-1].Err) {
		// Err is the last attempt's failure: name where it ran.
		attempts[n-1].writeTarget(&b)
		b.WriteString(": ")
		attempts = attempts[:n-1]
	}
	if e.Err != nil {
		b.WriteString(e.Err.Error())
	} else {
		b.WriteString("unknown failure") // a zero ExecutionError
	}
	// A single attempt's error already names its own status.
	if ExecutionStatusOf(e.Err) != e.status {
		b.WriteString(executionSuffix(e.status))
	}
	if len(attempts) == 0 {
		return b.String()
	}
	b.WriteString("; earlier attempts:")
	if skipped := len(attempts) - maxRenderedAttempts; skipped > 0 {
		fmt.Fprintf(&b, " (%d not shown)", skipped)
		attempts = attempts[skipped:]
	}
	for i, a := range attempts {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteByte(' ')
		b.WriteString(a.String())
	}
	return b.String()
}

// Unwrap returns Err followed by the error of every attempt.
func (e *ExecutionError) Unwrap() []error {
	out := make([]error, 0, 1+len(e.Attempts))
	out = append(out, e.Err)
	for _, a := range e.Attempts {
		out = append(out, a.Err)
	}
	return out
}

// ExecutionStatus reports the most severe status among all attempts.
func (e *ExecutionError) ExecutionStatus() ExecutionStatus { return e.status }

// IsCASWriteUnknown reports whether err says that a conditional (LWT) write
// timed out without knowing whether it applied. Cassandra reports this as
// CAS_WRITE_UNKNOWN on protocol 5 and as a write timeout of write type CAS on
// protocol 4; ScyllaDB reports the write timeout. ExecutionStatusOf reports
// MaybeExecuted for all of them. Read the row back before retrying a write
// that is not idempotent.
func IsCASWriteUnknown(err error) bool {
	if _, ok := errors.AsType[*CASWriteUnknownError](err); ok {
		return true
	}
	timeout, ok := errors.AsType[*WriteTimeoutError](err)
	return ok && timeout.WriteType == "CAS"
}
