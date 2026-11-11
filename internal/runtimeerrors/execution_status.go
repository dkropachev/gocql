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

// ExecutionStatusOf reports what the request that failed with err may have
// done on the cluster. The driver wraps every failure that follows a sent
// attempt in a QueryError or ExecutionError carrying that verdict, so a bare
// cancellation means the request was never sent. Any other error the driver
// cannot attribute reports MaybeExecuted.
func ExecutionStatusOf(err error) cqltype.ExecutionStatus {
	if err == nil {
		return cqltype.NotExecuted
	}
	if reporter, ok := errors.AsType[executionStatusReporter](err); ok {
		return reporter.ExecutionStatus()
	}
	if server, ok := errors.AsType[protocol.RequestError](err); ok {
		return protocol.ServerErrorExecutionStatus(server)
	}
	// Only conditions raised before anything is sent prove the request had
	// no effect. Validation and codec errors also arise from decoding a
	// response, after the server executed the request.
	if hasType[*SchemaChangedError](err) ||
		errors.Is(err, ErrNoConnections) || errors.Is(err, ErrNoConnectionsInPool) ||
		errors.Is(err, ErrNoPool) || errors.Is(err, ErrHostDown) || errors.Is(err, ErrSessionClosed) ||
		errors.Is(err, ErrSessionNotReady) || errors.Is(err, ErrQueryArgLength) || errors.Is(err, ErrBusy) || errors.Is(err, ErrConditionalExec) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return cqltype.NotExecuted
	}
	return cqltype.MaybeExecuted
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
	if _, ok := errors.AsType[*protocol.RequestErrCASWriteUnknown](err); ok {
		return true
	}
	timeout, ok := errors.AsType[*protocol.RequestErrWriteTimeout](err)
	return ok && timeout.WriteType == "CAS"
}
