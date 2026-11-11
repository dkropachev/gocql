// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

func TestServerErrorExecutionStatus(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  protocol.ServerError
		want cqltype.ExecutionStatus
	}{
		{"unavailable", &protocol.UnavailableError{}, cqltype.NotExecuted},
		{"overloaded", protocol.NewOtherServerError(protocol.CodeOverloaded, "m"), cqltype.NotExecuted},
		{"bootstrapping", protocol.NewOtherServerError(protocol.CodeBootstrapping, "m"), cqltype.NotExecuted},
		{"syntax", protocol.NewOtherServerError(protocol.CodeSyntax, "m"), cqltype.NotExecuted},
		{"invalid", protocol.NewOtherServerError(protocol.CodeInvalid, "m"), cqltype.NotExecuted},
		{"unauthorized", protocol.NewOtherServerError(protocol.CodeUnauthorized, "m"), cqltype.NotExecuted},
		{"credentials", protocol.NewOtherServerError(protocol.CodeBadCredentials, "m"), cqltype.NotExecuted},
		{"config", protocol.NewOtherServerError(protocol.CodeConfig, "m"), cqltype.NotExecuted},
		{"protocol", protocol.NewOtherServerError(protocol.CodeProtocol, "m"), cqltype.NotExecuted},
		{"already exists", &protocol.AlreadyExistsError{}, cqltype.NotExecuted},
		{"unprepared", &protocol.UnpreparedError{}, cqltype.NotExecuted},
		{"function failure", &protocol.FunctionFailureError{}, cqltype.NotExecuted},
		{"rate limit coordinator", &protocol.RateLimitReachedError{RejectedByCoordinator: true}, cqltype.NotExecuted},
		{"rate limit replica", &protocol.RateLimitReachedError{}, cqltype.MaybeExecuted},
		{"server", protocol.NewOtherServerError(protocol.CodeServer, "m"), cqltype.MaybeExecuted},
		{"truncate", protocol.NewOtherServerError(protocol.CodeTruncate, "m"), cqltype.MaybeExecuted},
		{"read timeout", &protocol.ReadTimeoutError{}, cqltype.MaybeExecuted},
		{"read failure", &protocol.ReadFailureError{}, cqltype.MaybeExecuted},
		{"cas write unknown", &protocol.CASWriteUnknownError{Received: 1}, cqltype.MaybeExecuted},
		{"cdc write failure", &protocol.CDCWriteFailureError{}, cqltype.MaybeExecuted},
		{"unknown", protocol.NewOtherServerError(0x7777, "m"), cqltype.MaybeExecuted},
		{"write timeout no acks", &protocol.WriteTimeoutError{WriteType: "SIMPLE"}, cqltype.MaybeExecuted},
		{"write timeout simple acked", &protocol.WriteTimeoutError{WriteType: "SIMPLE", Received: 1}, cqltype.PartiallyExecuted},
		{"write timeout counter acked", &protocol.WriteTimeoutError{WriteType: "COUNTER", Received: 1}, cqltype.PartiallyExecuted},
		{"write timeout batch log", &protocol.WriteTimeoutError{WriteType: "BATCH_LOG", Received: 1}, cqltype.MaybeExecuted},
		{"write timeout cas", &protocol.WriteTimeoutError{WriteType: "CAS", Received: 1}, cqltype.MaybeExecuted},
		{"write failure batch acked", &protocol.WriteFailureError{WriteType: "BATCH", Received: 2}, cqltype.PartiallyExecuted},
		{"write failure no acks", &protocol.WriteFailureError{WriteType: "BATCH"}, cqltype.MaybeExecuted},
	} {
		if got := protocol.ServerErrorExecutionStatus(test.err); got != test.want {
			t.Errorf("%s: got %v want %v", test.name, got, test.want)
		}
	}
}

func TestExecutionStatus(t *testing.T) {
	t.Parallel()

	t.Run("ZeroValueIsConservative", func(t *testing.T) {
		var zero cqltype.ExecutionStatus
		if zero != cqltype.MaybeExecuted {
			t.Fatal(zero)
		}
	})

	t.Run("Worse", func(t *testing.T) {
		order := []cqltype.ExecutionStatus{cqltype.NotExecuted, cqltype.MaybeExecuted, cqltype.PartiallyExecuted}
		for i, a := range order {
			for j, b := range order {
				want := order[max(i, j)]
				if got := a.Worse(b); got != want {
					t.Errorf("%v.Worse(%v) = %v want %v", a, b, got, want)
				}
			}
		}
	})

	t.Run("QueryError", func(t *testing.T) {
		syntax := protocol.NewOtherServerError(protocol.CodeSyntax, "bad")
		acked := &protocol.WriteTimeoutError{WriteType: "SIMPLE", Received: 1}
		for _, test := range []struct {
			name    string
			state   QueryErrorState
			want    cqltype.ExecutionStatus
			message string
		}{
			{"unsent", QueryErrorState{InternalErr: ErrConnectionClosed}, cqltype.NotExecuted, ErrConnectionClosed.Error()},
			{"no response", QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true}, cqltype.MaybeExecuted, ErrTimeoutNoResponse.Error() + " (maybe executed)"},
			{"server rejected", QueryErrorState{InternalErr: syntax, InternalPotentiallyExecuted: true}, cqltype.NotExecuted, "bad"},
			{"partial write", QueryErrorState{InternalErr: acked, InternalPotentiallyExecuted: true}, cqltype.PartiallyExecuted, " (partially executed)"},
		} {
			err := NewQueryError(test.state)
			if got := err.ExecutionStatus(); got != test.want {
				t.Errorf("%s: status %v want %v", test.name, got, test.want)
			}
			if !strings.HasSuffix(err.Error(), test.message) {
				t.Errorf("%s: message %q lacks %q", test.name, err.Error(), test.message)
			}
		}
	})

	t.Run("ExecutionStatusOf", func(t *testing.T) {
		attempt := NewQueryError(QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
		request := NewExecutionError(attempt, model.Result{}, cqltype.PartiallyExecuted, nil)
		for _, test := range []struct {
			name string
			err  error
			want cqltype.ExecutionStatus
		}{
			{"nil", nil, cqltype.NotExecuted},
			{"request wins over attempt", fmt.Errorf("wrapped: %w", request), cqltype.PartiallyExecuted},
			{"attempt", fmt.Errorf("wrapped: %w", attempt), cqltype.MaybeExecuted},
			{"bare server error", protocol.NewOtherServerError(protocol.CodeOverloaded, ""), cqltype.NotExecuted},
			{"validation", &cqltype.ValidationError{Field: "x", Err: errors.New("bad")}, cqltype.MaybeExecuted},
			{"codec", &cqltype.CodecError{Stage: "decode", Err: errors.New("bad")}, cqltype.MaybeExecuted},
			{"schema changed", &SchemaChangedError{Statement: "s"}, cqltype.NotExecuted},
			{"no connections", fmt.Errorf("pick: %w", ErrNoConnections), cqltype.NotExecuted},
			{"unknown", errors.New("eof"), cqltype.MaybeExecuted},
			{"bare cancellation", context.Canceled, cqltype.NotExecuted},
		} {
			if got := ExecutionStatusOf(test.err); got != test.want {
				t.Errorf("%s: got %v want %v", test.name, got, test.want)
			}
		}
	})

	t.Run("ExecutionErrorMessage", func(t *testing.T) {
		attempt := NewQueryError(QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
		same := NewExecutionError(attempt, model.Result{}, cqltype.MaybeExecuted, nil)
		if got := same.Error(); strings.Count(got, "(maybe executed)") != 1 {
			t.Fatalf("status repeated or missing: %q", got)
		}
		worse := NewExecutionError(attempt, model.Result{}, cqltype.PartiallyExecuted, nil)
		if got := worse.Error(); !strings.HasSuffix(got, "(maybe executed) (partially executed)") {
			t.Fatalf("aggregate status missing: %q", got)
		}
		clean := NewExecutionError(&cqltype.ValidationError{Field: "x", Err: errors.New("bad")}, model.Result{}, cqltype.NotExecuted, nil)
		if strings.Contains(clean.Error(), "executed") {
			t.Fatalf("not-executed status rendered: %q", clean.Error())
		}
	})
}

func TestExecutionErrorAttempts(t *testing.T) {
	t.Parallel()
	overloaded := NewQueryError(QueryErrorState{InternalErr: protocol.NewOtherServerError(protocol.CodeOverloaded, "overloaded"), InternalPotentiallyExecuted: true})
	timeout := NewQueryError(QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true})
	attempts := []AttemptFailure{
		{Err: overloaded, Address: "10.0.0.1:9042", Number: 1, Status: cqltype.NotExecuted},
		{Err: timeout, Address: "10.0.0.2:9042", Shard: cqltype.Option[uint32]{Value: 3, Set: true}, Number: 2, Status: cqltype.MaybeExecuted},
	}

	t.Run("LastAttemptIsCause", func(t *testing.T) {
		err := NewExecutionError(timeout, model.Result{}, cqltype.MaybeExecuted, attempts)
		got := err.Error()
		if strings.Count(got, "no response received") != 1 || !strings.Contains(got, "earlier attempts: #1 10.0.0.1:9042: overloaded") {
			t.Fatalf("message %q", got)
		}
	})

	t.Run("Cancelled", func(t *testing.T) {
		err := NewExecutionError(context.Canceled, model.Result{}, cqltype.MaybeExecuted, attempts)
		got := err.Error()
		if !strings.HasPrefix(got, "gocql: request: context canceled (maybe executed); earlier attempts: #1") || !strings.Contains(got, "#2 10.0.0.2:9042/shard3:") {
			t.Fatalf("message %q", got)
		}
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrTimeoutNoResponse) {
			t.Fatal("cause not reachable")
		}
		if server, ok := errors.AsType[protocol.ServerError](err); !ok || server.Code() != protocol.CodeOverloaded {
			t.Fatal("earlier server error not reachable")
		}
	})

	t.Run("Bounded", func(t *testing.T) {
		many := make([]AttemptFailure, 10)
		for i := range many {
			many[i] = AttemptFailure{Err: overloaded, Address: "h", Number: i + 1}
		}
		got := NewExecutionError(context.Canceled, model.Result{}, cqltype.NotExecuted, many).Error()
		if !strings.Contains(got, "(7 not shown)") || strings.Count(got, "overloaded: overloaded") != 3 || !strings.Contains(got, "#10 h") {
			t.Fatalf("message %q", got)
		}
	})
}
