// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// TestAttemptAs checks that AttemptAs searches the error first, then the
// attempts newest first.
func TestAttemptAs(t *testing.T) {
	t.Parallel()
	older, newer := &UnavailableError{Alive: 1}, &UnavailableError{Alive: 2}
	err := fmt.Errorf("op: %w", NewExecutionError(ErrRequestTimeout, MaybeExecuted,
		AttemptFailure{Err: older, Attempt: 1}, AttemptFailure{Err: fmt.Errorf("wrap: %w", newer), Attempt: 2}, AttemptFailure{Err: ErrTimeoutNoResponse, Attempt: 3}))
	if _, ok := errors.AsType[*UnavailableError](err); ok {
		t.Fatal("errors.As found an attempt's error")
	}
	if got, ok := AttemptAs[*UnavailableError](err); !ok || got != newer {
		t.Fatalf("AttemptAs = %v, %t; want the newest attempt's", got, ok)
	}
	// The error itself comes first.
	stopped := NewExecutionError(older, NotExecuted, AttemptFailure{Err: newer})
	if got, ok := AttemptAs[*UnavailableError](stopped); !ok || got != older {
		t.Fatalf("AttemptAs = %v, %t; want the error's own", got, ok)
	}
	if _, ok := AttemptAs[*WriteTimeoutError](err); ok {
		t.Fatal("AttemptAs found a type no attempt holds")
	}
	if _, ok := AttemptAs[*UnavailableError](nil); ok {
		t.Fatal("AttemptAs found an error in nil")
	}
}

// TestAttemptIs checks that AttemptIs searches the error first, then the
// attempts, as AttemptAs does.
func TestAttemptIs(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("op: %w", NewExecutionError(ErrRequestTimeout, MaybeExecuted,
		AttemptFailure{Err: &UnavailableError{}, Attempt: 1}, AttemptFailure{Err: fmt.Errorf("wrap: %w", ErrConnectionClosed), Attempt: 2}))
	if errors.Is(err, ErrConnectionClosed) {
		t.Fatal("errors.Is found an attempt's error")
	}
	for _, c := range []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"own error", err, ErrRequestTimeout, true},
		{"own error through Is", err, context.DeadlineExceeded, true},
		{"newest attempt", err, ErrConnectionClosed, true},
		{"older attempt's code", err, CodeUnavailable, true},
		{"absent", err, ErrNoConnections, false},
		{"plain error", ErrClosed, ErrClosed, true},
		{"nil", nil, ErrClosed, false},
	} {
		if got := AttemptIs(c.err, c.target); got != c.want {
			t.Errorf("%s: AttemptIs = %t, want %t", c.name, got, c.want)
		}
	}
}

// TestConditionalUnknown checks that ConditionalUnknown reports every
// conditional request that may have applied, not only those the server
// reported as a CAS timeout.
func TestConditionalUnknown(t *testing.T) {
	t.Parallel()
	failed := func(err error, status ExecutionStatus, kind cql.StatementKind) error {
		e := NewExecutionError(err, status, AttemptFailure{Err: err, ExecutionStatus: status})
		e.StatementKind = kind
		return e
	}
	casTimeout := &WriteTimeoutError{WriteType: WriteTypeCAS}
	for _, c := range []struct {
		name    string
		err     error
		unknown bool
	}{
		{"server CAS timeout", failed(casTimeout, MaybeExecuted, cql.ConditionalStatement), true},
		{"driver timeout", failed(ErrRequestTimeout, MaybeExecuted, cql.ConditionalStatement), true},
		{"lost connection", failed(ErrConnectionClosed, MaybeExecuted, cql.ConditionalStatement), true},
		{"caller canceled in flight", fmt.Errorf("op: %w", failed(context.Canceled, MaybeExecuted, cql.ConditionalStatement)), true},
		{"not sent", failed(ErrNoConnections, NotExecuted, cql.ConditionalStatement), false},
		{"ordinary", failed(ErrRequestTimeout, MaybeExecuted, cql.OrdinaryStatement), false},
		{"no execution error", casTimeout, false},
		{"nil", nil, false},
	} {
		if got := ConditionalUnknown(c.err); got != c.unknown {
			t.Errorf("%s: ConditionalUnknown = %t, want %t", c.name, got, c.unknown)
		}
	}
}
