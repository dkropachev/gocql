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

func TestRetryable(t *testing.T) {
	t.Parallel()
	writeTimeout := &WriteTimeoutError{WriteType: WriteTypeSimple}
	failed := func(err error, status ExecutionStatus, idempotent bool) error {
		e := NewExecutionError(err, status)
		e.Idempotent = idempotent
		return e
	}
	conditional := func(status ExecutionStatus) error {
		e := NewExecutionError(writeTimeout, status)
		e.Idempotent = true
		e.StatementKind = cql.ConditionalStatement
		return e
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unavailable", &UnavailableError{}, true},
		{"overloaded", NewOtherServerError(CodeOverloaded, ""), true},
		{"syntax", failed(NewOtherServerError(CodeSyntax, ""), NotExecuted, true), false},
		{"write timeout outside a request", writeTimeout, false},
		{"write timeout", failed(writeTimeout, MaybeExecuted, false), false},
		{"write timeout idempotent", failed(writeTimeout, MaybeExecuted, true), true},
		{"wrapped idempotent", fmt.Errorf("op: %w", failed(writeTimeout, MaybeExecuted, true)), true},
		{"partially executed", failed(writeTimeout, PartiallyExecuted, false), false},
		{"partially executed idempotent", failed(writeTimeout, PartiallyExecuted, true), true},
		{"no connections", ErrNoConnections, true},
		{"driver timeout", failed(fmt.Errorf("attempt: %w", ErrRequestTimeout), MaybeExecuted, true), true},
		{"driver timeout not idempotent", failed(ErrRequestTimeout, MaybeExecuted, false), false},
		{"caller deadline", failed(context.DeadlineExceeded, MaybeExecuted, true), false},
		{"caller deadline unsent", context.DeadlineExceeded, false},
		{"caller cancel", failed(context.Canceled, NotExecuted, true), false},
		{"canceled with timeout", errors.Join(context.Canceled, ErrRequestTimeout), false},
		{"closed", ErrClosed, false},
		{"conditional idempotent maybe applied", conditional(MaybeExecuted), false},
		{"conditional idempotent not executed", conditional(NotExecuted), true},
	}
	for _, c := range cases {
		if got := Retryable(c.err); got != c.want {
			t.Errorf("%s: Retryable(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}
