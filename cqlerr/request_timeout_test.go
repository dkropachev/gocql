// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestRequestTimeout checks that the driver's budget is told apart from the
// caller's deadline while both still match context.DeadlineExceeded.
func TestRequestTimeout(t *testing.T) {
	t.Parallel()
	if !errors.Is(ErrRequestTimeout, context.DeadlineExceeded) {
		t.Fatal("ErrRequestTimeout does not match context.DeadlineExceeded")
	}
	if errors.Is(context.DeadlineExceeded, ErrRequestTimeout) {
		t.Fatal("the caller's deadline matches ErrRequestTimeout")
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Nanosecond, ErrRequestTimeout)
	defer cancel()
	<-ctx.Done()
	err := NewExecutionError(fmt.Errorf("attempt: %w", context.Cause(ctx)), MaybeExecuted)
	if !errors.Is(err, ErrRequestTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v: lost the cause", err)
	}
	if KindOf(err) != KindTimeout || !KindOf(err).Transient() {
		t.Fatalf("KindOf(%v) = %v", err, KindOf(err))
	}
	// A deadline outranks a server error of the same tree, as
	// context.DeadlineExceeded does.
	joined := errors.Join(NewOtherServerError(CodeInvalid, ""), ErrRequestTimeout)
	if KindOf(joined) != KindTimeout {
		t.Fatalf("KindOf(%v) = %v", joined, KindOf(joined))
	}
	// It answers net.Error's Timeout and Temporary as
	// context.DeadlineExceeded does.
	var netErr interface {
		Timeout() bool
		Temporary() bool
	}
	if !errors.As(ErrRequestTimeout, &netErr) || !netErr.Timeout() || !netErr.Temporary() {
		t.Fatal("ErrRequestTimeout does not report Timeout and Temporary")
	}
	if ExecutionStatusOf(ErrRequestTimeout) != NotExecuted {
		t.Fatalf("a bare request timeout reports %v", ExecutionStatusOf(ErrRequestTimeout))
	}
}

// TestExecutionErrorUnwrap checks that an ExecutionError wraps its cause
// only, so errors.As agrees with KindOf.
func TestExecutionErrorUnwrap(t *testing.T) {
	t.Parallel()
	unavailable := &UnavailableError{}
	err := NewExecutionError(ErrTimeoutNoResponse, MaybeExecuted, AttemptFailure{Err: unavailable, Number: 1, ExecutionStatus: NotExecuted}, AttemptFailure{Err: ErrTimeoutNoResponse, Number: 2, ExecutionStatus: MaybeExecuted})
	if !errors.Is(err, ErrTimeoutNoResponse) || errors.Unwrap(err) != ErrTimeoutNoResponse {
		t.Fatal("cause not reachable")
	}
	if _, ok := errors.AsType[*UnavailableError](err); ok {
		t.Fatal("errors.As found an earlier attempt's error")
	}
	if KindOf(err) != KindTimeout || err.ExecutionStatus() != MaybeExecuted || len(err.Attempts) != 2 {
		t.Fatalf("KindOf %v, status %v, %d attempts", KindOf(err), err.ExecutionStatus(), len(err.Attempts))
	}
}
