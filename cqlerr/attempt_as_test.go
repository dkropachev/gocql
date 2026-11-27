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

// TestConditionalUnknown checks that ConditionalUnknown reports every
// conditional request that may have applied, which IsCASWriteUnknown misses
// unless the server said so.
func TestConditionalUnknown(t *testing.T) {
	t.Parallel()
	failed := func(err error, status ExecutionStatus, kind cql.StatementKind) error {
		e := NewExecutionError(err, status, AttemptFailure{Err: err, ExecutionStatus: status})
		e.StatementKind = kind
		return e
	}
	casTimeout := &WriteTimeoutError{WriteType: WriteTypeCAS}
	for _, c := range []struct {
		name         string
		err          error
		unknown, cas bool
	}{
		{"server CAS timeout", failed(casTimeout, MaybeExecuted, cql.ConditionalStatement), true, true},
		{"driver timeout", failed(ErrRequestTimeout, MaybeExecuted, cql.ConditionalStatement), true, false},
		{"lost connection", failed(ErrConnectionClosed, MaybeExecuted, cql.ConditionalStatement), true, false},
		{"caller canceled in flight", fmt.Errorf("op: %w", failed(context.Canceled, MaybeExecuted, cql.ConditionalStatement)), true, false},
		{"not sent", failed(ErrNoConnections, NotExecuted, cql.ConditionalStatement), false, false},
		{"ordinary", failed(ErrRequestTimeout, MaybeExecuted, cql.OrdinaryStatement), false, false},
		{"no execution error", casTimeout, false, true},
		{"nil", nil, false, false},
	} {
		if got := ConditionalUnknown(c.err); got != c.unknown {
			t.Errorf("%s: ConditionalUnknown = %t, want %t", c.name, got, c.unknown)
		}
		if got := IsCASWriteUnknown(c.err); got != c.cas {
			t.Errorf("%s: IsCASWriteUnknown = %t, want %t", c.name, got, c.cas)
		}
	}
}
