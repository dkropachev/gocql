// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package runtimeerrors builds the cqlerr errors whose state is unexported:
// the engine's attempt and execution errors. It reaches them through
// internal/link: a constructor slot for the unexported attempt error, and a
// layout mirror of ExecutionError that TestMirrorLayouts checks.
package runtimeerrors

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/link"
)

// QueryErrorState is what the engine records about a failed attempt.
type QueryErrorState struct {
	InternalErr                 error
	InternalTimeout             time.Duration
	InternalInFlight            int
	InternalPotentiallyExecuted bool
}

// executionErrorRep mirrors cqlerr.ExecutionError.
type executionErrorRep struct {
	Err           error
	Attempts      []cqlerr.AttemptFailure
	Statement     string
	Result        cql.Result
	BatchSize     int
	Idempotent    bool
	StatementKind cql.StatementKind
	status        cqlerr.ExecutionStatus
}

var (
	serverErrorMessage = link.Get[func(cqlerr.ServerError, string, []string)](link.ServerErrorMessage)
	newAttemptError    = link.Get[func(error, time.Duration, int, bool) error](link.NewAttemptError)
)

// NewQueryError returns the error of a failed attempt. It reports
// cqlerr.NotExecuted unless state says the request may have been sent.
func NewQueryError(state QueryErrorState) error {
	return newAttemptError(state.InternalErr, state.InternalTimeout, state.InternalInFlight, state.InternalPotentiallyExecuted)
}

// NewExecutionError returns the error of a request that stopped with err
// after attempts, with status describing what it may have done.
func NewExecutionError(err error, result cql.Result, status cqlerr.ExecutionStatus, attempts []cqlerr.AttemptFailure) *cqlerr.ExecutionError {
	e := new(cqlerr.ExecutionError)
	*link.Mirror[executionErrorRep](e) = executionErrorRep{Err: err, Attempts: attempts, Result: result, status: status}
	return e
}

// WithMessage sets err's server message, for tests that build server errors.
func WithMessage[E cqlerr.ServerError](err E, message string) E {
	serverErrorMessage(err, message, nil)
	return err
}
