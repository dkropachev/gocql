// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Cassandra over protocol 5 reports an LWT with an unknown outcome as
// CAS_WRITE_UNKNOWN; ScyllaDB and Cassandra over protocol 4 as a write timeout
// of write type CAS. One check covers both.
func TestIsCASWriteUnknown(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&CASWriteUnknownError{}, true},
		{fmt.Errorf("wrapped: %w", &CASWriteUnknownError{}), true},
		{&WriteTimeoutError{WriteType: WriteTypeCAS}, true},
		{&WriteTimeoutError{WriteType: WriteTypeSimple}, false},
		{&ReadTimeoutError{}, false},
		{nil, false},
	} {
		if got := IsCASWriteUnknown(tc.err); got != tc.want {
			t.Errorf("%v: %t, want %t", tc.err, got, tc.want)
		}
		if tc.want && ExecutionStatusOf(tc.err) != MaybeExecuted {
			t.Errorf("%v: status %v, want MaybeExecuted", tc.err, ExecutionStatusOf(tc.err))
		}
	}
}

// An ExecutionError wraps only its cause, so IsCASWriteUnknown looks at every
// attempt: an LWT timeout the caller's cancellation ended is still reported.
func TestIsCASWriteUnknownInAttempts(t *testing.T) {
	timeout := &WriteTimeoutError{WriteType: WriteTypeCAS}
	err := NewExecutionError(context.Canceled, MaybeExecuted, AttemptFailure{Err: timeout, Attempt: 1})
	if errors.Is(err, CodeWriteTimeout) {
		t.Fatal("ExecutionError unwraps an earlier attempt")
	}
	if !IsCASWriteUnknown(err) || !IsCASWriteUnknown(fmt.Errorf("wrapped: %w", err)) {
		t.Fatal("CAS timeout of an earlier attempt not reported")
	}
	if IsCASWriteUnknown(NewExecutionError(context.Canceled, MaybeExecuted, AttemptFailure{Err: &WriteTimeoutError{WriteType: WriteTypeSimple}})) {
		t.Fatal("a simple write timeout reported as CAS")
	}
}
