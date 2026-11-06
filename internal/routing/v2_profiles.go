// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"context"
	"errors"

	model "github.com/scylladb/gocql/v2/internal/model"

	"time"

	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

type Target = model.Target

// QueryPlan returns the routes a request tries, in order, until one of
// them has a connection.
type QueryPlan interface{ Next() (Route, bool) }

// RoutingPolicy chooses the routes of a request in two steps, optimized for
// the happy path where the first route serves it:
//
//   - Pick returns the first route, cheaply: without building a plan and
//     without allocating. It returns false when it has no cheap answer, and
//     the request then starts with Fallback's routes.
//   - Fallback returns every route the request may take, in order. The
//     driver calls it only when the picked route fails or Pick returned
//     false, and skips the picked route if Fallback returns it again.
//
// Observe receives the outcome of every attempt. NewPlan combines Pick and
// Fallback into the plan a request follows.
type RoutingPolicy interface {
	Validate(*MetadataSnapshot) error
	Pick(RoutingInfo, *MetadataSnapshot) (Route, bool)
	Fallback(RoutingInfo, *MetadataSnapshot) QueryPlan
	Observe(AttemptEvent)
}
type RetryDecision struct {
	Action      RetryAction
	Consistency Option[Consistency]
	Delay       time.Duration
}
type RetryAction uint8

const (
	StopRetry RetryAction = iota
	RetrySame
	RetryNext
	IgnoreWriteError
)

type AttemptError struct {
	Err          error
	Attempt      int
	Idempotent   bool
	Consistency  Consistency
	Conditional  bool
	BytesWritten bool
}

type AttemptEvent struct {
	Started                time.Time
	Err                    error
	RequestID              uint64
	Attempt                int
	Latency                time.Duration
	Target                 Target
	Cancelled, Speculative bool
}

// SafeRetryPolicy retries bounded transient failures only when replay is safe.
type SafeRetryPolicy struct{ MaxAttempts int }

func (p SafeRetryPolicy) Decide(a AttemptError) RetryDecision {
	limit := p.MaxAttempts
	if limit <= 0 {
		limit = 3
	}
	if a.Attempt >= limit || errors.Is(a.Err, context.Canceled) || errors.Is(a.Err, context.DeadlineExceeded) {
		return RetryDecision{}
	}
	var unavailable *RequestErrUnavailable
	if errors.As(a.Err, &unavailable) {
		return RetryDecision{Action: RetryNext}
	}
	var read *RequestErrReadTimeout
	if errors.As(a.Err, &read) && read.Received >= read.BlockFor && read.DataPresent == 0 {
		return RetryDecision{Action: RetrySame}
	}
	if !a.Idempotent {
		var query *runtimeerrors.QueryError
		if !a.BytesWritten && errors.As(a.Err, &query) && !query.PotentiallyExecuted() {
			return RetryDecision{Action: RetryNext}
		}
		return RetryDecision{}
	}
	var server RequestError
	if errors.As(a.Err, &server) {
		switch server.GetCode() {
		case ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeServer:
			return RetryDecision{Action: RetryNext}
		default:
			return RetryDecision{}
		}
	}
	var schema *runtimeerrors.SchemaChangedError
	var validation *ValidationError
	var protocol *ProtocolError
	if errors.As(a.Err, &schema) || errors.As(a.Err, &validation) || errors.As(a.Err, &protocol) {
		return RetryDecision{}
	}
	return RetryDecision{Action: RetryNext}
}
