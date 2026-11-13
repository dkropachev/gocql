// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
)

type AttemptFailure = runtimeerrors.AttemptFailure

// attemptLog collects the failed attempts of one request. Its zero value is
// empty and allocates nothing until an attempt fails; a nil *attemptLog reads
// as empty, so a hot loop can allocate one on its first failed attempt.
type attemptLog struct {
	failures []AttemptFailure
	worst    cqltype.ExecutionStatus
	raised   bool
}

// fail records a failed attempt and returns its execution status. It stays
// out of line so the request loops keep their happy-path frame small.
//
//go:noinline
func (l *attemptLog) fail(err error, conn *transport.Conn, number int, consistency cqltype.Consistency, trace cqltype.Option[cqltype.UUID]) cqltype.ExecutionStatus {
	status := runtimeerrors.ExecutionStatusOf(err)
	failure := AttemptFailure{Err: err, Number: number, Consistency: consistency, Status: status, CQLTraceID: trace}
	if conn != nil {
		failure.Address = conn.Address()
		failure.HostID = conn.InternalHostID()
		if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
			failure.Shard = cqltype.Option[uint32]{Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard), Set: true}
		}
	}
	l.failures = append(l.failures, failure)
	l.raise(status)
	return status
}

// responseTraceID returns the CQL trace session a possibly nil response names.
func responseTraceID(response *transport.InternalResponsePage) cqltype.Option[cqltype.UUID] {
	if response == nil {
		return cqltype.Option[cqltype.UUID]{}
	}
	return response.CQLTraceID()
}

// raise records that the request reached at least status.
func (l *attemptLog) raise(status cqltype.ExecutionStatus) {
	if !l.raised {
		l.worst, l.raised = status, true
		return
	}
	l.worst = l.worst.Worse(status)
}

func (l *attemptLog) status() cqltype.ExecutionStatus {
	if l == nil || !l.raised {
		return cqltype.NotExecuted
	}
	return l.worst
}

func (l *attemptLog) empty() bool { return l == nil || !l.raised }

// error returns the request failure err with every recorded attempt.
func (l *attemptLog) error(err error, result model.Result) *ExecutionError {
	if l == nil {
		return runtimeerrors.NewExecutionError(err, result, cqltype.NotExecuted, nil)
	}
	return runtimeerrors.NewExecutionError(err, result, l.status(), l.failures)
}

// merge folds a later execution's failure, such as a hand-off to the general
// path, into this log's earlier attempts.
func (l *attemptLog) merge(err error, result model.Result) error {
	if l.empty() {
		return err
	}
	later, ok := errors.AsType[*ExecutionError](err)
	if !ok {
		return l.error(err, result)
	}
	failures := append(l.failures[:len(l.failures):len(l.failures)], later.Attempts...)
	status := l.status().Worse(later.ExecutionStatus())
	merged := later.Result
	merged.Attempts += len(l.failures)
	return runtimeerrors.NewExecutionError(later.Err, merged, status, failures)
}
