// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry_test

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/retry"
)

// TestAttemptRetryable checks that Attempt.Retryable reads the attempt's
// own idempotence and statement kind, which cqlerr.Retryable cannot see in
// Attempt.Err: an idempotent request whose attempt timed out is retryable,
// one that may have applied an LWT is not.
func TestAttemptRetryable(t *testing.T) {
	t.Parallel()
	timeout := &cqlerr.WriteTimeoutError{WriteType: cqlerr.WriteTypeSimple}
	for _, tc := range []struct {
		name    string
		attempt retry.Attempt
		want    bool
	}{
		{"zero", retry.Attempt{}, false},
		{"unavailable", retry.Attempt{Err: &cqlerr.UnavailableError{}, ExecutionStatus: cqlerr.NotExecuted}, true},
		{"timeout idempotent", retry.Attempt{Err: timeout, ExecutionStatus: cqlerr.MaybeExecuted, Idempotent: true, StatementKind: cql.OrdinaryStatement}, true},
		{"timeout not idempotent", retry.Attempt{Err: timeout, ExecutionStatus: cqlerr.MaybeExecuted, StatementKind: cql.OrdinaryStatement}, false},
		{"partial idempotent", retry.Attempt{Err: timeout, ExecutionStatus: cqlerr.PartiallyExecuted, Idempotent: true, StatementKind: cql.OrdinaryStatement}, true},
		{"conditional idempotent", retry.Attempt{Err: timeout, ExecutionStatus: cqlerr.MaybeExecuted, Idempotent: true, StatementKind: cql.ConditionalStatement}, false},
		{"unknown kind idempotent", retry.Attempt{Err: timeout, ExecutionStatus: cqlerr.MaybeExecuted, Idempotent: true}, false},
		{"conditional not executed", retry.Attempt{Err: &cqlerr.UnavailableError{}, ExecutionStatus: cqlerr.NotExecuted, StatementKind: cql.ConditionalStatement}, true},
		{"driver timeout idempotent", retry.Attempt{Err: cqlerr.ErrRequestTimeout, ExecutionStatus: cqlerr.MaybeExecuted, Idempotent: true, StatementKind: cql.OrdinaryStatement}, true},
		{"caller deadline", retry.Attempt{Err: context.DeadlineExceeded, ExecutionStatus: cqlerr.NotExecuted, Idempotent: true}, false},
		{"invalid", retry.Attempt{Err: cqlerr.NewOtherServerError(cqlerr.CodeSyntax, ""), ExecutionStatus: cqlerr.NotExecuted}, false},
	} {
		if got := tc.attempt.Retryable(); got != tc.want {
			t.Errorf("%s: Retryable() = %t, want %t", tc.name, got, tc.want)
		}
		// cqlerr.Retryable agrees on the error alone only when the attempt
		// did not execute: Err holds no ExecutionError.
		if tc.attempt.Err != nil && tc.attempt.ExecutionStatus == cqlerr.NotExecuted {
			if got := cqlerr.Retryable(tc.attempt.Err); got != tc.want {
				t.Errorf("%s: cqlerr.Retryable(Err) = %t, want %t", tc.name, got, tc.want)
			}
		}
	}
}
