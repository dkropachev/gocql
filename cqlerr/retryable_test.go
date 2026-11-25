// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestRetryable(t *testing.T) {
	t.Parallel()
	writeTimeout := &WriteTimeoutError{WriteType: WriteTypeSimple}
	cases := []struct {
		name       string
		err        error
		idempotent bool
		want       bool
	}{
		{"nil", nil, true, false},
		{"unavailable", &UnavailableError{}, false, true},
		{"overloaded", NewOtherServerError(CodeOverloaded, ""), false, true},
		{"syntax", NewOtherServerError(CodeSyntax, ""), true, false},
		{"write timeout", writeTimeout, false, false},
		{"write timeout idempotent", writeTimeout, true, true},
		{"partially executed", NewExecutionError(writeTimeout, PartiallyExecuted), false, false},
		{"partially executed idempotent", NewExecutionError(writeTimeout, PartiallyExecuted), true, true},
		{"no connections", ErrNoConnections, false, true},
		{"driver timeout", NewExecutionError(fmt.Errorf("attempt: %w", ErrRequestTimeout), MaybeExecuted), true, true},
		{"driver timeout not idempotent", NewExecutionError(ErrRequestTimeout, MaybeExecuted), false, false},
		{"caller deadline", NewExecutionError(context.DeadlineExceeded, MaybeExecuted), true, false},
		{"caller deadline unsent", context.DeadlineExceeded, true, false},
		{"caller cancel", NewExecutionError(context.Canceled, NotExecuted), true, false},
		{"canceled with timeout", errors.Join(context.Canceled, ErrRequestTimeout), true, false},
		{"closed", ErrClosed, true, false},
	}
	for _, c := range cases {
		if got := Retryable(c.err, c.idempotent); got != c.want {
			t.Errorf("%s: Retryable(%v, %v) = %v, want %v", c.name, c.err, c.idempotent, got, c.want)
		}
	}
}
