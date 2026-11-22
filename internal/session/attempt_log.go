// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
)

type AttemptFailure = cqlerr.AttemptFailure

// attemptLog collects the failed attempts of one request. Its zero value is
// empty and allocates nothing until an attempt fails; a nil *attemptLog reads
// as empty, so a hot loop can allocate one on its first failed attempt.
type attemptLog struct {
	failures []AttemptFailure
	worst    cqlerr.ExecutionStatus
	raised   bool
}

// worseStatus returns the more severe of a and b, ordering NotExecuted <
// MaybeExecuted < PartiallyExecuted. An undefined status counts as
// MaybeExecuted, the safe assumption.
func worseStatus(a, b cqlerr.ExecutionStatus) cqlerr.ExecutionStatus {
	if statusSeverity(b) > statusSeverity(a) {
		return b
	}
	return a
}

func statusSeverity(s cqlerr.ExecutionStatus) int {
	switch s {
	case cqlerr.NotExecuted:
		return 0
	case cqlerr.PartiallyExecuted:
		return 2
	default:
		return 1
	}
}

// fail records a failed attempt and returns its execution status; partial
// reports a batch a server may apply in part (see partialBatch). It stays out
// of line so the request loops keep their happy-path frame small.
//
//go:noinline
func (l *attemptLog) fail(err error, conn *transport.Conn, number int, consistency cql.Consistency, trace cql.Option[cql.UUID], partial bool) cqlerr.ExecutionStatus {
	status := attemptStatus(err, partial)
	l.record(AttemptFailure{Err: err, Number: number, Consistency: consistency, Status: status, CQLTraceID: trace}, conn)
	return status
}

// attemptStatus returns what an attempt that failed with err may have done;
// partial reports an unlogged or counter batch. Unavailable and Overloaded
// report a request the server rejected, but Cassandra checks such a batch one
// partition at a time and can reject it after it sent the earlier
// partitions' writes, so the batch may have been applied in part. The driver
// assumes so on every server: it does not rely on the order in which a
// server checks a batch's partitions. A logged batch is checked whole before
// its batch log is written.
func attemptStatus(err error, partial bool) cqlerr.ExecutionStatus {
	status := cqlerr.ExecutionStatusOf(err)
	if status != cqlerr.NotExecuted || !partial {
		return status
	}
	if server, ok := errors.AsType[cqlerr.ServerError](err); ok {
		if code := server.Code(); code == cqlerr.CodeUnavailable || code == cqlerr.CodeOverloaded {
			return cqlerr.MaybeExecuted
		}
	}
	return status
}

// record adds failure, filling in where conn sent it.
func (l *attemptLog) record(failure AttemptFailure, conn *transport.Conn) {
	if conn != nil {
		failure.Address = conn.Address()
		failure.HostID = conn.InternalHostID()
		if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
			failure.ShardID = cql.Option[uint32]{Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard), Set: true}
		}
	}
	l.failures = append(l.failures, failure)
	l.raise(failure.Status)
}

// responseTraceID returns the CQL trace session a possibly nil response names.
func responseTraceID(response *transport.InternalResponsePage) cql.Option[cql.UUID] {
	if response == nil {
		return cql.Option[cql.UUID]{}
	}
	return response.CQLTraceID()
}

// raise records that the request reached at least status.
func (l *attemptLog) raise(status cqlerr.ExecutionStatus) {
	if !l.raised {
		l.worst, l.raised = status, true
		return
	}
	l.worst = worseStatus(l.worst, status)
}

func (l *attemptLog) status() cqlerr.ExecutionStatus {
	if l == nil || !l.raised {
		return cqlerr.NotExecuted
	}
	return l.worst
}

func (l *attemptLog) empty() bool { return l == nil || !l.raised }

// error returns the request failure err with every recorded attempt.
func (l *attemptLog) error(err error, result cql.Result) *ExecutionError {
	if l == nil {
		return runtimeerrors.NewExecutionError(err, result, cqlerr.NotExecuted, nil)
	}
	return runtimeerrors.NewExecutionError(err, result, l.status(), l.failures)
}

// merge folds a later execution's failure, such as a hand-off to the general
// path, into this log's earlier attempts.
func (l *attemptLog) merge(err error, result cql.Result) error {
	if l.empty() {
		return err
	}
	later, ok := errors.AsType[*ExecutionError](err)
	if !ok {
		return l.error(err, result)
	}
	failures := append(l.failures[:len(l.failures):len(l.failures)], later.Attempts...)
	status := worseStatus(l.status(), later.ExecutionStatus())
	merged := later.Result
	merged.Attempts += len(l.failures)
	return runtimeerrors.NewExecutionError(later.Err, merged, status, failures)
}

// inflightAttempt is an attempt whose outcome its execution has not received.
type inflightAttempt struct {
	conn        *transport.Conn
	number      int
	consistency cql.Consistency
}

// removeInflight drops attempt number from attempts.
func removeInflight(attempts []inflightAttempt, number int) []inflightAttempt {
	for i, a := range attempts {
		if a.number == number {
			return append(attempts[:i], attempts[i+1:]...)
		}
	}
	return attempts
}

// abandon records an attempt its execution stopped waiting for because err,
// the execution's context error, ended it. The attempt may have been sent,
// so it may have been applied.
func (l *attemptLog) abandon(err error, conn *transport.Conn, number int, consistency cql.Consistency) {
	l.record(AttemptFailure{Err: err, Number: number, Consistency: consistency, Status: cqlerr.MaybeExecuted}, conn)
}
