// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

type executionStatusReporter interface {
	error
	ExecutionStatus() cqltype.ExecutionStatus
}

// statusWalk is the state of one ExecutionStatusOf walk over an error tree.
type statusWalk struct {
	reporter    executionStatusReporter
	server      protocol.ServerError
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
		w.server, _ = err.(protocol.ServerError) //nolint:errorlint // one node of a tree walkErrors walks
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
// done on the cluster. The driver wraps every failure that follows a sent
// attempt in a QueryError or ExecutionError carrying that verdict, so a bare
// cancellation means the request was never sent. Any other error the driver
// cannot attribute reports MaybeExecuted.
//
// It walks err's tree once: the first QueryError or ExecutionError decides,
// then the first server error, then any error proving the request unsent.
func ExecutionStatusOf(err error) cqltype.ExecutionStatus {
	if err == nil {
		return cqltype.NotExecuted
	}
	var w statusWalk
	walkErrors(err, nil, &w)
	switch {
	case w.reporter != nil:
		return w.reporter.ExecutionStatus()
	case w.server != nil:
		return protocol.ServerErrorExecutionStatus(w.server)
	case w.notExecuted:
		return cqltype.NotExecuted
	default:
		return cqltype.MaybeExecuted
	}
}

// AttemptFailure records one failed attempt of a request.
type AttemptFailure struct {
	Err error
	// Address is the host:port the attempt was sent to.
	Address string
	HostID  cqltype.UUID
	// Number is the 1-based attempt number.
	Number int
	// Shard is the shard of the connection, when the host is shard-aware.
	Shard       cqltype.Option[uint32]
	Consistency cqltype.Consistency
	Status      cqltype.ExecutionStatus
	// CQLTraceID is the CQL trace session the server recorded for the
	// attempt, when it ran with CQL tracing and the server answered with an
	// error. Read it with package cqltrace.
	CQLTraceID cqltype.Option[cqltype.UUID]
}

func (a AttemptFailure) String() string {
	target := a.Address
	if target == "" {
		target = a.HostID.String()
	}
	if a.Shard.Set {
		target += fmt.Sprintf("/shard%d", a.Shard.Value)
	}
	return fmt.Sprintf("#%d %s: %v", a.Number, target, a.Err)
}

// maxRenderedAttempts bounds how many attempts an error message lists.
const maxRenderedAttempts = 3

// ExecutionError is returned when a request fails. ExecutionStatus reports
// what the request may have done on the cluster across all of its attempts.
// errors.Is and errors.As search Err and the error of every attempt.
type ExecutionError struct {
	// Err is the reason the request stopped: the last attempt's error, the
	// caller's cancellation, or a failure before any attempt.
	Err error
	// Attempts lists the failed attempts in the order they finished.
	Attempts []AttemptFailure
	Result   model.Result
	status   cqltype.ExecutionStatus
}

// NewExecutionError returns the failure of a request whose attempts reached
// status.
func NewExecutionError(err error, result model.Result, status cqltype.ExecutionStatus, attempts []AttemptFailure) *ExecutionError {
	return &ExecutionError{Err: err, Result: result, status: status, Attempts: attempts}
}

func (e *ExecutionError) Error() string {
	var b strings.Builder
	b.WriteString("gocql: request: ")
	b.WriteString(e.Err.Error())
	// A single attempt's error already names its own status.
	if ExecutionStatusOf(e.Err) != e.status {
		b.WriteString(ExecutionSuffix(e.status))
	}
	attempts := e.Attempts
	if n := len(attempts); n > 0 && errors.Is(e.Err, attempts[n-1].Err) {
		attempts = attempts[:n-1] // already rendered as Err
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
func (e *ExecutionError) ExecutionStatus() cqltype.ExecutionStatus { return e.status }

// IsCASWriteUnknown reports whether err says that a conditional write timed
// out with an unknown outcome: CAS_WRITE_UNKNOWN on Cassandra over protocol
// 5, or a write timeout of write type CAS on ScyllaDB and on Cassandra over
// protocol 4. The write may or may not have applied.
func IsCASWriteUnknown(err error) bool {
	if _, ok := errors.AsType[*protocol.CASWriteUnknownError](err); ok {
		return true
	}
	timeout, ok := errors.AsType[*protocol.WriteTimeoutError](err)
	return ok && timeout.WriteType == "CAS"
}
