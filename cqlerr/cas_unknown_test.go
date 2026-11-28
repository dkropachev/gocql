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
// of write type CAS. Both are MaybeExecuted.
func TestCASWriteUnknownStatus(t *testing.T) {
	for _, err := range []error{
		&CASWriteUnknownError{},
		fmt.Errorf("wrapped: %w", &CASWriteUnknownError{}),
		&WriteTimeoutError{WriteType: WriteTypeCAS},
	} {
		if ExecutionStatusOf(err) != MaybeExecuted {
			t.Errorf("%v: status %v, want MaybeExecuted", err, ExecutionStatusOf(err))
		}
	}
}

// An ExecutionError wraps only its cause, so the CAS timeout of an attempt
// the caller's cancellation ended is found by AttemptAs, not errors.As.
func TestCASWriteUnknownInAttempts(t *testing.T) {
	timeout := &WriteTimeoutError{WriteType: WriteTypeCAS}
	err := NewExecutionError(context.Canceled, MaybeExecuted, AttemptFailure{Err: timeout, Attempt: 1})
	if errors.Is(err, CodeWriteTimeout) {
		t.Fatal("ExecutionError unwraps an earlier attempt")
	}
	if found, ok := AttemptAs[*WriteTimeoutError](fmt.Errorf("wrapped: %w", err)); !ok || found.WriteType != WriteTypeCAS {
		t.Fatal("CAS timeout of an earlier attempt not found")
	}
	if !AttemptIs(err, CodeWriteTimeout) {
		t.Fatal("AttemptIs missed the code of an earlier attempt")
	}
}
