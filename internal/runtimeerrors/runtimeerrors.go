// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package runtimeerrors builds the cqlerr errors whose state is unexported:
// the engine's attempt and execution errors. It reaches their fields through
// layout mirrors; TestMirrorLayouts checks them.
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

// queryErrorRep mirrors cqlerr.QueryError.
type queryErrorRep struct{ state QueryErrorState }

// executionErrorRep mirrors cqlerr.ExecutionError.
type executionErrorRep struct {
	Err       error
	Attempts  []cqlerr.AttemptFailure
	Statement string
	Result    cql.Result
	BatchSize int
	status    cqlerr.ExecutionStatus
}

var serverErrorMessage = link.Get[func(cqlerr.ServerError, string, []string)](link.ServerErrorMessage)

// NewQueryError returns the error of a failed attempt.
func NewQueryError(state QueryErrorState) *cqlerr.QueryError {
	e := new(cqlerr.QueryError)
	link.Mirror[queryErrorRep](e).state = state
	return e
}

// QueryErrorData returns the state of e.
func QueryErrorData(e *cqlerr.QueryError) *QueryErrorState {
	return &link.Mirror[queryErrorRep](e).state
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
