// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
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
	case ErrNoConnections, ErrBusy, ErrConditionalExec, //nolint:errorlint // one node of a tree walkErrors walks
		ErrNoStreams, ErrNoConnectionsStarted, ErrUseStmt, ErrPagingComplete,
		context.Canceled, context.DeadlineExceeded, ErrRequestTimeout:
		return true
	}
	return false
}

// ExecutionStatusOf reports what the request that failed with err may have
// done on the cluster, across all of its attempts. It returns Executed for a
// nil error, the outcome of a request that succeeded, and MaybeExecuted for
// an error the driver cannot attribute.
// A call that refuses a request before sending it, for example for an
// invalid argument, returns an ExecutionError reporting NotExecuted.
func ExecutionStatusOf(err error) ExecutionStatus {
	if err == nil {
		return Executed
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
	// Err is the attempt's error.
	Err error
	// Address is the host:port the attempt was sent to, or the socket path
	// of a Unix socket connection, which is why it is text.
	Address string
	// Attempt is the 1-based attempt number.
	Attempt int
	// Target is the host the attempt was sent to and, when the host is
	// shard-aware, the shard of the connection, as in
	// routing.AttemptEvent.Target.
	Target metadata.Target
	// Consistency is the consistency level the attempt used.
	Consistency cql.Consistency
	// CQLTraceID is the CQL trace session the server recorded for the
	// attempt, when it ran with CQL tracing and the server answered with an
	// error. Read it with package cqltrace.
	CQLTraceID cql.Option[cql.UUID]
	// ExecutionStatus reports what the attempt may have done on the cluster.
	ExecutionStatus ExecutionStatus
}

// String returns the attempt's number, its host, its shard on a shard-aware
// host, and its error.
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
		target = a.Target.HostID.String()
	}
	fmt.Fprintf(b, "#%d %s", a.Attempt, target)
	if a.Target.ShardID.Set {
		fmt.Fprintf(b, "/shard%d", a.Target.ShardID.Value)
	}
}

// maxRenderedAttempts bounds how many attempts an error message lists.
const maxRenderedAttempts = 3

// ExecutionError is returned when a request fails after all its attempts.
// It wraps Err only: errors.Is and errors.As see why the request stopped,
// not what an earlier attempt failed with. Read Attempts for those.
//
// The driver returns it filled in; NewExecutionError builds one for tests.
// Its zero value is an error without detail.
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
	// Statement is the CQL text of the request's statement, or of a batch's
	// first statement, when the session was configured with
	// config.ConfigBuilder.WithErrorStatementText; it is empty otherwise.
	// Text over that setting's limit is cut to it and "..." is appended.
	// It never holds bound values, but it does hold literals written into
	// the CQL text.
	Statement string
	// Result holds what the responses carried before the request failed:
	// warnings, CQL trace IDs and custom payload.
	Result cql.Result
	// BatchSize is the number of statements of a batch whose first
	// statement Statement holds, and zero otherwise.
	BatchSize int
	// Idempotent reports that the request was idempotent as the driver
	// resolved it: execopt.Options.Idempotent when set, else the statement's
	// or the profile's. Retryable reads it.
	Idempotent bool
	// StatementKind classifies the request's statement, or a batch's
	// statements together, as retry.Attempt.StatementKind does:
	// cql.ConditionalStatement when it is a lightweight transaction. A
	// SERIAL or LOCAL_SERIAL read reports its own kind, OrdinaryStatement,
	// though the driver routes it as a conditional statement.
	// ConditionalUnknown reads it.
	StatementKind cql.StatementKind
	status        ExecutionStatus
}

// NewExecutionError returns the failure of a request that stopped with err
// after attempts, whose ExecutionStatus reports status. Tests of code that
// handles the driver's errors, such as a retry.Policy or a caller's
// fallback, can use it; set Statement, Result, BatchSize, Idempotent and
// StatementKind directly.
func NewExecutionError(err error, status ExecutionStatus, attempts ...AttemptFailure) *ExecutionError {
	return &ExecutionError{Err: err, status: status, Attempts: attempts}
}

// Error returns Err, prefixed with the last attempt's target when Err is that
// attempt's failure, followed by the execution status when Err does not
// already report it, the statement when Statement is set, and at most three
// earlier attempts.
func (e *ExecutionError) Error() string {
	var b strings.Builder
	b.WriteString("gocql: request: ")
	attempts := e.Attempts
	if n := len(attempts); n > 0 && e.Err != nil && errors.Is(e.Err, attempts[n-1].Err) {
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
	e.writeStatement(&b)
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

// writeStatement writes the statement the request ran, quoted, when the
// session reports it: `; statement "..."`, or for a batch
// `; batch of N statements, first "..."`.
func (e *ExecutionError) writeStatement(b *strings.Builder) {
	if e.Statement == "" {
		return
	}
	if e.BatchSize > 0 {
		fmt.Fprintf(b, "; batch of %d statements, first %q", e.BatchSize, e.Statement)
		return
	}
	fmt.Fprintf(b, "; statement %q", e.Statement)
}

// Unwrap returns Err. An earlier attempt's error is in Attempts only, so
// errors.Is and errors.As agree with KindOf, which classifies Err.
func (e *ExecutionError) Unwrap() error { return e.Err }

// ExecutionStatus reports the most severe status among all attempts.
func (e *ExecutionError) ExecutionStatus() ExecutionStatus { return e.status }

// ConditionalUnknown reports whether err is the failure of a conditional
// request (ExecutionError.StatementKind is cql.ConditionalStatement) that may
// have applied: its ExecutionStatus is not NotExecuted. The outcome of such
// an LWT is unknown whatever the cause, a server's CAS timeout
// (CASWriteUnknownError on Cassandra's protocol 5, a WriteTimeoutError of
// WriteTypeCAS otherwise), the driver's timeout, a lost connection or the
// caller's cancellation; read the row back, with a SERIAL read, before
// sending it again. A SERIAL or LOCAL_SERIAL read is OrdinaryStatement and
// reports false. An error that holds no ExecutionError reports false. To
// tell which of those causes ended the request, use AttemptAs: the server's
// CAS timeout may be an earlier attempt's.
func ConditionalUnknown(err error) bool {
	request, ok := errors.AsType[*ExecutionError](err)
	return ok && request.StatementKind == cql.ConditionalStatement && request.status != NotExecuted
}
