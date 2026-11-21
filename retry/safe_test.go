// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/retry"
)

// attempt builds the Attempt the driver passes for err: the error as an
// attempt on one connection reports it, with the status the driver derives.
func attempt(err error, sent bool, idempotent bool) retry.Attempt {
	err = runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: sent})
	return retry.Attempt{Err: err, Number: 1, Idempotent: idempotent, ExecutionStatus: cqlerr.ExecutionStatusOf(err)}
}

// TestSafe pins retry.Safe's decision for every class of failure, for
// idempotent and non-idempotent requests. The rule it enforces: a request
// that is not idempotent is replayed only when the failure proves nothing was
// applied.
func TestSafe(t *testing.T) {
	type want struct{ idempotent, nonIdempotent retry.Action }
	for _, tc := range []struct {
		name string
		err  error
		sent bool
		want want
	}{
		// Rejected before execution: another host may serve it.
		{"Unavailable", &cqlerr.UnavailableError{Required: 2, Alive: 1}, true, want{retry.Next, retry.Next}},
		{"Overloaded", cqlerr.NewOtherServerError(cqlerr.CodeOverloaded, ""), true, want{retry.Next, retry.Next}},
		{"Bootstrapping", cqlerr.NewOtherServerError(cqlerr.CodeBootstrapping, ""), true, want{retry.Next, retry.Next}},
		// A read that had its replicas but no data is retried in place.
		{"ReadTimeoutEnoughReplicas", &cqlerr.ReadTimeoutError{Received: 2, BlockFor: 2}, true, want{retry.Same, retry.Same}},
		{"ReadTimeoutTooFewReplicas", &cqlerr.ReadTimeoutError{Received: 1, BlockFor: 2}, true, want{retry.Stop, retry.Stop}},
		{"ReadTimeoutDataPresent", &cqlerr.ReadTimeoutError{Received: 2, BlockFor: 2, DataPresent: true}, true, want{retry.Stop, retry.Stop}},
		// A coordinator failure: only an idempotent request moves on.
		{"ServerError", cqlerr.NewOtherServerError(cqlerr.CodeServer, ""), true, want{retry.Next, retry.Stop}},
		// Failures after which the request may have been applied, or that a
		// replay cannot fix.
		{"WriteTimeout", &cqlerr.WriteTimeoutError{WriteType: cqlerr.WriteTypeSimple}, true, want{retry.Stop, retry.Stop}},
		{"WriteTimeoutCAS", &cqlerr.WriteTimeoutError{WriteType: cqlerr.WriteTypeCAS}, true, want{retry.Stop, retry.Stop}},
		{"CASWriteUnknown", &cqlerr.CASWriteUnknownError{}, true, want{retry.Stop, retry.Stop}},
		{"WriteFailure", &cqlerr.WriteFailureError{WriteType: cqlerr.WriteTypeSimple}, true, want{retry.Stop, retry.Stop}},
		{"ReadFailure", &cqlerr.ReadFailureError{}, true, want{retry.Stop, retry.Stop}},
		{"Truncate", cqlerr.NewOtherServerError(cqlerr.CodeTruncate, ""), true, want{retry.Stop, retry.Stop}},
		{"RateLimitedByCoordinator", &cqlerr.RateLimitReachedError{RejectedByCoordinator: true}, true, want{retry.Stop, retry.Stop}},
		{"RateLimitedByReplica", &cqlerr.RateLimitReachedError{}, true, want{retry.Stop, retry.Stop}},
		{"Syntax", cqlerr.NewOtherServerError(cqlerr.CodeSyntax, ""), true, want{retry.Stop, retry.Stop}},
		{"Invalid", cqlerr.NewOtherServerError(cqlerr.CodeInvalid, ""), true, want{retry.Stop, retry.Stop}},
		{"Unauthorized", cqlerr.NewOtherServerError(cqlerr.CodeUnauthorized, ""), true, want{retry.Stop, retry.Stop}},
		{"ServerProtocol", cqlerr.NewOtherServerError(cqlerr.CodeProtocol, ""), true, want{retry.Stop, retry.Stop}},
		{"FunctionFailure", &cqlerr.FunctionFailureError{}, true, want{retry.Stop, retry.Stop}},
		{"AlreadyExists", &cqlerr.AlreadyExistsError{}, true, want{retry.Stop, retry.Stop}},
		{"SchemaChanged", &cqlerr.SchemaChangedError{}, false, want{retry.Stop, retry.Stop}},
		{"Validation", &cql.ValidationError{Field: "value", Err: errors.New("bad")}, false, want{retry.Stop, retry.Stop}},
		{"DriverProtocol", &cql.ProtocolError{Stage: "rows", Err: errors.New("bad")}, true, want{retry.Stop, retry.Stop}},
		// The caller gave up: never replay.
		{"Canceled", context.Canceled, true, want{retry.Stop, retry.Stop}},
		{"CanceledBeforeSend", context.Canceled, false, want{retry.Stop, retry.Stop}},
		{"DeadlineExceeded", context.DeadlineExceeded, true, want{retry.Stop, retry.Stop}},
		// A connection failure: the attempt's status decides.
		{"ConnectionClosedAfterSend", cqlerr.ErrConnectionClosed, true, want{retry.Next, retry.Stop}},
		{"ConnectionClosedBeforeSend", cqlerr.ErrConnectionClosed, false, want{retry.Next, retry.Next}},
		{"NoResponseAfterSend", cqlerr.ErrTimeoutNoResponse, true, want{retry.Next, retry.Stop}},
		{"WriteFailedBeforeSend", io.ErrUnexpectedEOF, false, want{retry.Next, retry.Next}},
		{"NoStreams", cqlerr.ErrNoStreams, false, want{retry.Next, retry.Next}},
		{"Unknown", errors.New("unknown"), true, want{retry.Next, retry.Stop}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, idempotent := range []bool{true, false} {
				want := tc.want.nonIdempotent
				if idempotent {
					want = tc.want.idempotent
				}
				a := attempt(tc.err, tc.sent, idempotent)
				if got := (retry.Safe{}).Decide(a); got.Action != want || got.Delay != 0 || got.Consistency.Set {
					t.Errorf("idempotent=%t status=%v: %+v, want %v", idempotent, a.ExecutionStatus, got, want)
				}
				// Wrapping the error further must not change the decision.
				a.Err = fmt.Errorf("wrapped: %w", a.Err)
				if got := (retry.Safe{}).Decide(a); got.Action != want {
					t.Errorf("idempotent=%t wrapped: %v, want %v", idempotent, got.Action, want)
				}
				if got := (retry.Never{}).Decide(a); got != (retry.Decision{}) {
					t.Errorf("Never decided %+v", got)
				}
			}
		})
	}

	// A non-idempotent attempt is replayed after a failure only when its
	// status proves nothing was applied: the status, not the error, decides.
	t.Run("StatusDecides", func(t *testing.T) {
		for _, status := range []cqlerr.ExecutionStatus{cqlerr.MaybeExecuted, cqlerr.PartiallyExecuted, cqlerr.NotExecuted} {
			got := (retry.Safe{}).Decide(retry.Attempt{Err: errors.New("broken"), Number: 1, ExecutionStatus: status})
			want := retry.Stop
			if status == cqlerr.NotExecuted {
				want = retry.Next
			}
			if got.Action != want {
				t.Errorf("status %v: %v, want %v", status, got.Action, want)
			}
		}
		// The zero Attempt is the safe assumption: maybe executed.
		if got := (retry.Safe{}).Decide(retry.Attempt{Err: errors.New("broken"), Number: 1}); got.Action != retry.Stop {
			t.Errorf("zero status replayed: %v", got.Action)
		}
	})

	// Cassandra can reject an unlogged or counter batch as Unavailable or
	// Overloaded after it sent some partitions' writes: a non-idempotent one
	// is not replayed. A logged batch and Bootstrapping reject the batch
	// whole.
	t.Run("PartialBatch", func(t *testing.T) {
		unavailable := &cqlerr.UnavailableError{Required: 2, Alive: 1}
		overloaded := cqlerr.NewOtherServerError(cqlerr.CodeOverloaded, "")
		bootstrapping := cqlerr.NewOtherServerError(cqlerr.CodeBootstrapping, "")
		for _, tc := range []struct {
			name        string
			err         error
			batch       cql.Option[cql.BatchKind]
			idempotent  bool
			conditional bool
			want        retry.Action
		}{
			{"NotBatch/Unavailable", unavailable, cql.Option[cql.BatchKind]{}, false, false, retry.Next},
			{"Logged/Unavailable", unavailable, cql.Some(cql.LoggedBatch), false, false, retry.Next},
			{"Logged/Overloaded", overloaded, cql.Some(cql.LoggedBatch), false, false, retry.Next},
			{"Unlogged/Unavailable", unavailable, cql.Some(cql.UnloggedBatch), false, false, retry.Stop},
			{"Unlogged/Overloaded", overloaded, cql.Some(cql.UnloggedBatch), false, false, retry.Stop},
			{"Counter/Unavailable", unavailable, cql.Some(cql.CounterBatch), false, false, retry.Stop},
			{"Counter/Overloaded", overloaded, cql.Some(cql.CounterBatch), false, false, retry.Stop},
			{"UnknownKind/Unavailable", unavailable, cql.Some(cql.BatchKind(9)), false, false, retry.Stop},
			{"Unlogged/Bootstrapping", bootstrapping, cql.Some(cql.UnloggedBatch), false, false, retry.Next},
			{"Unlogged/Idempotent", unavailable, cql.Some(cql.UnloggedBatch), true, false, retry.Next},
			{"Counter/IdempotentOverloaded", overloaded, cql.Some(cql.CounterBatch), true, false, retry.Next},
			// Conditional is also set for a batch with a member of unknown
			// kind, which may span partitions.
			{"Unlogged/Conditional", unavailable, cql.Some(cql.UnloggedBatch), false, true, retry.Stop},
		} {
			a := attempt(tc.err, true, tc.idempotent)
			a.Batch, a.Conditional = tc.batch, tc.conditional
			if got := (retry.Safe{}).Decide(a); got.Action != tc.want {
				t.Errorf("%s: %v, want %v", tc.name, got.Action, tc.want)
			}
		}
	})

	t.Run("MaxAttempts", func(t *testing.T) {
		for _, tc := range []struct{ max, number int }{{0, 3}, {-1, 3}, {1, 1}, {5, 5}} {
			a := retry.Attempt{Err: &cqlerr.UnavailableError{}, Number: tc.number - 1, ExecutionStatus: cqlerr.NotExecuted}
			if a.Number > 0 {
				if got := (retry.Safe{MaxAttempts: tc.max}).Decide(a); got.Action != retry.Next {
					t.Errorf("max %d: attempt %d stopped", tc.max, a.Number)
				}
			}
			a.Number = tc.number
			if got := (retry.Safe{MaxAttempts: tc.max}).Decide(a); got.Action != retry.Stop {
				t.Errorf("max %d: attempt %d retried", tc.max, a.Number)
			}
		}
	})
}
