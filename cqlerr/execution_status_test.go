// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
)

func TestServerErrorExecutionStatus(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  ServerError
		want ExecutionStatus
	}{
		{"unavailable", &UnavailableError{}, NotExecuted},
		{"overloaded", NewOtherServerError(CodeOverloaded, "m"), NotExecuted},
		{"bootstrapping", NewOtherServerError(CodeBootstrapping, "m"), NotExecuted},
		{"syntax", NewOtherServerError(CodeSyntax, "m"), NotExecuted},
		{"invalid", NewOtherServerError(CodeInvalid, "m"), NotExecuted},
		{"unauthorized", NewOtherServerError(CodeUnauthorized, "m"), NotExecuted},
		{"credentials", NewOtherServerError(CodeBadCredentials, "m"), NotExecuted},
		{"config", NewOtherServerError(CodeConfig, "m"), NotExecuted},
		{"protocol", NewOtherServerError(CodeProtocol, "m"), NotExecuted},
		{"already exists", &AlreadyExistsError{}, NotExecuted},
		{"unprepared", &UnpreparedError{}, NotExecuted},
		{"function failure", &FunctionFailureError{}, NotExecuted},
		{"rate limit coordinator", &RateLimitReachedError{RejectedByCoordinator: true}, NotExecuted},
		{"rate limit replica", &RateLimitReachedError{}, MaybeExecuted},
		{"server", NewOtherServerError(CodeServer, "m"), MaybeExecuted},
		{"truncate", NewOtherServerError(CodeTruncate, "m"), MaybeExecuted},
		{"read timeout", &ReadTimeoutError{}, MaybeExecuted},
		{"read failure", &ReadFailureError{}, MaybeExecuted},
		{"cas write unknown", &CASWriteUnknownError{Received: 1}, MaybeExecuted},
		{"cdc write failure", NewOtherServerError(CodeCDCWriteFailure, ""), MaybeExecuted},
		{"unknown", NewOtherServerError(0x7777, "m"), MaybeExecuted},
		{"write timeout no acks", &WriteTimeoutError{WriteType: WriteTypeSimple}, MaybeExecuted},
		{"write timeout simple acked", &WriteTimeoutError{WriteType: WriteTypeSimple, Received: 1}, PartiallyExecuted},
		{"write timeout counter acked", &WriteTimeoutError{WriteType: WriteTypeCounter, Received: 1}, PartiallyExecuted},
		{"write timeout batch log", &WriteTimeoutError{WriteType: WriteTypeBatchLog, Received: 1}, MaybeExecuted},
		{"write timeout cas", &WriteTimeoutError{WriteType: WriteTypeCAS, Received: 1}, MaybeExecuted},
		{"write failure batch acked", &WriteFailureError{WriteType: WriteTypeBatch, Received: 2}, MaybeExecuted},
		{"write timeout logged batch acked", &WriteTimeoutError{WriteType: WriteTypeBatch, Received: 1}, MaybeExecuted},
		{"write timeout unlogged batch acked", &WriteTimeoutError{WriteType: WriteTypeUnloggedBatch, Received: 1}, PartiallyExecuted},
		{"write failure no acks", &WriteFailureError{WriteType: WriteTypeBatch}, MaybeExecuted},
	} {
		if got := serverErrorExecutionStatus(test.err); got != test.want {
			t.Errorf("%s: got %v want %v", test.name, got, test.want)
		}
	}
}

func TestExecutionStatus(t *testing.T) {
	t.Parallel()

	t.Run("Names", func(t *testing.T) {
		for status, want := range map[ExecutionStatus]string{NotExecuted: "not executed", MaybeExecuted: "maybe executed", PartiallyExecuted: "partially executed", Executed: "executed", Executed + 1: "ExecutionStatus(4)"} {
			if got := status.String(); got != want {
				t.Errorf("%d: %q, want %q", uint8(status), got, want)
			}
		}
	})

	t.Run("ZeroValueIsConservative", func(t *testing.T) {
		var zero ExecutionStatus
		if zero != MaybeExecuted {
			t.Fatal(zero)
		}
	})

	t.Run("QueryError", func(t *testing.T) {
		syntax := NewOtherServerError(CodeSyntax, "bad")
		acked := &WriteTimeoutError{WriteType: WriteTypeSimple, Received: 1}
		for _, test := range []struct {
			name    string
			state   queryErrorState
			want    ExecutionStatus
			message string
		}{
			{"unsent", queryErrorState{InternalErr: ErrConnectionClosed}, NotExecuted, ErrConnectionClosed.Error()},
			{"no response", queryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true}, MaybeExecuted, ErrTimeoutNoResponse.Error() + " (maybe executed)"},
			{"server rejected", queryErrorState{InternalErr: syntax, InternalPotentiallyExecuted: true}, NotExecuted, "bad"},
			{"partial write", queryErrorState{InternalErr: acked, InternalPotentiallyExecuted: true}, PartiallyExecuted, " (partially executed)"},
		} {
			err := newQueryError(test.state)
			if got := err.ExecutionStatus(); got != test.want {
				t.Errorf("%s: status %v want %v", test.name, got, test.want)
			}
			if !strings.HasSuffix(err.Error(), test.message) {
				t.Errorf("%s: message %q lacks %q", test.name, err.Error(), test.message)
			}
		}
	})

	t.Run("ExecutionStatusOf", func(t *testing.T) {
		attempt := newQueryError(queryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
		request := NewExecutionError(attempt, PartiallyExecuted)
		for _, test := range []struct {
			name string
			err  error
			want ExecutionStatus
		}{
			{"nil", nil, Executed},
			{"request wins over attempt", fmt.Errorf("wrapped: %w", request), PartiallyExecuted},
			{"attempt", fmt.Errorf("wrapped: %w", attempt), MaybeExecuted},
			{"bare server error", NewOtherServerError(CodeOverloaded, ""), NotExecuted},
			{"validation", &cql.ValidationError{Field: "x", Err: errors.New("bad")}, MaybeExecuted},
			{"codec", &cql.CodecError{Stage: "decode", Err: errors.New("bad")}, MaybeExecuted},
			{"schema changed", &SchemaChangedError{Statement: "s"}, NotExecuted},
			{"no connections", fmt.Errorf("pick: %w", ErrNoConnections), NotExecuted},
			{"unknown", errors.New("eof"), MaybeExecuted},
			{"bare cancellation", context.Canceled, NotExecuted},
		} {
			if got := ExecutionStatusOf(test.err); got != test.want {
				t.Errorf("%s: got %v want %v", test.name, got, test.want)
			}
		}
	})

	t.Run("ExecutionErrorMessage", func(t *testing.T) {
		attempt := newQueryError(queryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
		same := NewExecutionError(attempt, MaybeExecuted)
		if got := same.Error(); strings.Count(got, "(maybe executed)") != 1 {
			t.Fatalf("status repeated or missing: %q", got)
		}
		worse := NewExecutionError(attempt, PartiallyExecuted)
		if got := worse.Error(); !strings.HasSuffix(got, "(maybe executed) (partially executed)") {
			t.Fatalf("aggregate status missing: %q", got)
		}
		clean := NewExecutionError(&cql.ValidationError{Field: "x", Err: errors.New("bad")}, NotExecuted)
		if strings.Contains(clean.Error(), "executed") {
			t.Fatalf("not-executed status rendered: %q", clean.Error())
		}
	})
}

func TestExecutionErrorAttempts(t *testing.T) {
	t.Parallel()
	overloaded := newQueryError(queryErrorState{InternalErr: NewOtherServerError(CodeOverloaded, "overloaded"), InternalPotentiallyExecuted: true})
	timeout := newQueryError(queryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
	attempts := []AttemptFailure{
		{Err: overloaded, Address: "10.0.0.1:9042", Attempt: 1, ExecutionStatus: NotExecuted},
		{Err: timeout, Address: "10.0.0.2:9042", Target: metadata.Target{ShardID: cql.Option[uint32]{Value: 3, Set: true}}, Attempt: 2, ExecutionStatus: MaybeExecuted},
	}

	t.Run("LastAttemptIsCause", func(t *testing.T) {
		err := NewExecutionError(timeout, MaybeExecuted, attempts...)
		got := err.Error()
		if !strings.HasPrefix(got, "gocql: request: #2 10.0.0.2:9042/shard3: gocql: no response from server") || strings.Count(got, "no response from server") != 1 || !strings.Contains(got, "earlier attempts: #1 10.0.0.1:9042: overloaded") {
			t.Fatalf("message %q", got)
		}
	})

	t.Run("Cancelled", func(t *testing.T) {
		err := NewExecutionError(context.Canceled, MaybeExecuted, attempts...)
		got := err.Error()
		if !strings.HasPrefix(got, "gocql: request: context canceled (maybe executed); earlier attempts: #1") || !strings.Contains(got, "#2 10.0.0.2:9042/shard3:") {
			t.Fatalf("message %q", got)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cause not reachable")
		}
		// Earlier attempts are in Attempts only.
		if errors.Is(err, ErrTimeoutNoResponse) || errors.Is(err, CodeOverloaded) {
			t.Fatal("an earlier attempt's error is reachable")
		}
	})

	t.Run("Statement", func(t *testing.T) {
		err := NewExecutionError(timeout, MaybeExecuted, attempts...)
		err.Statement = "SELECT \"v\"\nFROM t"
		got := err.Error()
		if !strings.Contains(got, `(maybe executed); statement "SELECT \"v\"\nFROM t"; earlier attempts: #1`) {
			t.Fatalf("message %q", got)
		}
		err.BatchSize = 2
		if got := err.Error(); !strings.Contains(got, `; batch of 2 statements, first "SELECT`) {
			t.Fatalf("message %q", got)
		}
	})

	t.Run("EmptyAttempt", func(t *testing.T) {
		// errors.Is(nil, nil) holds; a value without a cause names no attempt.
		got := (&ExecutionError{Attempts: []AttemptFailure{{}}}).Error()
		if !strings.HasPrefix(got, "gocql: request: unknown failure") {
			t.Fatalf("message %q", got)
		}
	})

	t.Run("Bounded", func(t *testing.T) {
		many := make([]AttemptFailure, 10)
		for i := range many {
			many[i] = AttemptFailure{Err: overloaded, Address: "h", Attempt: i + 1}
		}
		got := NewExecutionError(context.Canceled, NotExecuted, many...).Error()
		if !strings.Contains(got, "(7 not shown)") || strings.Count(got, "overloaded: overloaded") != 3 || !strings.Contains(got, "#10 h") {
			t.Fatalf("message %q", got)
		}
	})
}
