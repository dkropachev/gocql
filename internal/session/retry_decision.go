// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/retry"
)

// decideRetry asks request's retry policy what to do after the attempt
// number that failed with err on conn; first is when the page's first
// attempt started. Without a policy it stops. It stays out of line so the
// request loops keep their happy-path frame small.
//
//go:noinline
func decideRetry(ctx context.Context, request *InternalExecutionRequest, conn *transport.Conn, err error, number int, consistency cql.Consistency, status cqlerr.ExecutionStatus, first time.Time) retry.Decision {
	policy := request.options.InternalProfile.Retry
	if policy == nil {
		return retry.Decision{}
	}
	attempt := retry.Attempt{
		Context:         ctx,
		Err:             err,
		Attempt:         number,
		Idempotent:      request.isIdempotent(),
		Consistency:     consistency,
		StatementKind:   request.statementKind(),
		Batch:           request.batchKind(),
		ExecutionStatus: status,
		Keyspace:        request.routing.Keyspace,
		Table:           request.routing.Table,
	}
	if conn != nil {
		attempt.Target = attemptTarget(request, conn)
	}
	if !first.IsZero() {
		attempt.Elapsed = time.Since(first)
	}
	return policy.Decide(attempt)
}

// checkDecision reports whether d, a decision other than Stop, is one the
// driver can carry out. An invalid decision fails the request with a
// cql.ValidationError that wraps the attempt's error, attemptErr, so that
// errors.Is and errors.As still find why the attempt failed.
func checkDecision(d retry.Decision, attemptErr error) error {
	if d.Action != retry.Same && d.Action != retry.Next {
		return cqltype.TypeValidation("retry", "unknown retry action %v after: %w", d.Action, attemptErr)
	}
	if d.Consistency.Set && !settings.InternalValidConsistency(d.Consistency.Value) {
		return cqltype.TypeValidation("retry consistency", "invalid consistency %v after: %w", d.Consistency.Value, attemptErr)
	}
	if d.Delay < 0 {
		return cqltype.TypeValidation("retry delay", "negative delay %v after: %w", d.Delay, attemptErr)
	}
	return nil
}
