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
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

func TestServerErrorExecutionStatus(t *testing.T) {
	t.Parallel()
	frame := func(code int) frm.ErrorFrame { return frm.ErrorFrame{Code: code, Message: "m"} }
	for _, test := range []struct {
		name string
		err  protocol.RequestError
		want cqltype.ExecutionStatus
	}{
		{"unavailable", &protocol.RequestErrUnavailable{ErrorFrame: frame(protocol.ErrCodeUnavailable)}, cqltype.NotExecuted},
		{"overloaded", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeOverloaded)}, cqltype.NotExecuted},
		{"bootstrapping", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeBootstrapping)}, cqltype.NotExecuted},
		{"syntax", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeSyntax)}, cqltype.NotExecuted},
		{"invalid", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeInvalid)}, cqltype.NotExecuted},
		{"unauthorized", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeUnauthorized)}, cqltype.NotExecuted},
		{"credentials", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeCredentials)}, cqltype.NotExecuted},
		{"config", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeConfig)}, cqltype.NotExecuted},
		{"protocol", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeProtocol)}, cqltype.NotExecuted},
		{"already exists", &protocol.RequestErrAlreadyExists{ErrorFrame: frame(protocol.ErrCodeAlreadyExists)}, cqltype.NotExecuted},
		{"unprepared", &protocol.RequestErrUnprepared{ErrorFrame: frame(protocol.ErrCodeUnprepared)}, cqltype.NotExecuted},
		{"function failure", &protocol.RequestErrFunctionFailure{ErrorFrame: frame(protocol.ErrCodeFunctionFailure)}, cqltype.NotExecuted},
		{"rate limit coordinator", &protocol.RequestErrRateLimitReached{RejectedByCoordinator: true}, cqltype.NotExecuted},
		{"rate limit replica", &protocol.RequestErrRateLimitReached{}, cqltype.MaybeExecuted},
		{"server", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeServer)}, cqltype.MaybeExecuted},
		{"truncate", &protocol.RequestErrServer{ErrorFrame: frame(protocol.ErrCodeTruncate)}, cqltype.MaybeExecuted},
		{"read timeout", &protocol.RequestErrReadTimeout{ErrorFrame: frame(protocol.ErrCodeReadTimeout)}, cqltype.MaybeExecuted},
		{"read failure", &protocol.RequestErrReadFailure{ErrorFrame: frame(protocol.ErrCodeReadFailure)}, cqltype.MaybeExecuted},
		{"cas write unknown", &protocol.RequestErrCASWriteUnknown{ErrorFrame: frame(protocol.ErrCodeCASWriteUnknown), Received: 1}, cqltype.MaybeExecuted},
		{"cdc write failure", &protocol.RequestErrCDCWriteFailure{ErrorFrame: frame(protocol.ErrCodeCDCWriteFailure)}, cqltype.MaybeExecuted},
		{"unknown", &protocol.UnknownServerError{ErrorFrame: frame(0x7777)}, cqltype.MaybeExecuted},
		{"write timeout no acks", &protocol.RequestErrWriteTimeout{WriteType: "SIMPLE"}, cqltype.MaybeExecuted},
		{"write timeout simple acked", &protocol.RequestErrWriteTimeout{WriteType: "SIMPLE", Received: 1}, cqltype.PartiallyExecuted},
		{"write timeout counter acked", &protocol.RequestErrWriteTimeout{WriteType: "COUNTER", Received: 1}, cqltype.PartiallyExecuted},
		{"write timeout batch log", &protocol.RequestErrWriteTimeout{WriteType: "BATCH_LOG", Received: 1}, cqltype.MaybeExecuted},
		{"write timeout cas", &protocol.RequestErrWriteTimeout{WriteType: "CAS", Received: 1}, cqltype.MaybeExecuted},
		{"write failure batch acked", &protocol.RequestErrWriteFailure{WriteType: "BATCH", Received: 2}, cqltype.PartiallyExecuted},
		{"write failure no acks", &protocol.RequestErrWriteFailure{WriteType: "BATCH"}, cqltype.MaybeExecuted},
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
		syntax := &protocol.RequestErrServer{ErrorFrame: frm.ErrorFrame{Code: protocol.ErrCodeSyntax, Message: "bad"}}
		acked := &protocol.RequestErrWriteTimeout{WriteType: "SIMPLE", Received: 1}
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
			{"bare server error", &protocol.RequestErrServer{ErrorFrame: frm.ErrorFrame{Code: protocol.ErrCodeOverloaded}}, cqltype.NotExecuted},
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
	overloaded := NewQueryError(QueryErrorState{InternalErr: &frm.ErrorFrame{Code: protocol.ErrCodeOverloaded, Message: "overloaded"}, InternalPotentiallyExecuted: true})
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
		if server, ok := errors.AsType[protocol.RequestError](err); !ok || server.GetCode() != protocol.ErrCodeOverloaded {
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
