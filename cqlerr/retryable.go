// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"context"
	"errors"
)

// Retryable reports whether sending the request that failed with err again
// can succeed and is safe. It combines KindOf, ExecutionStatusOf and the
// idempotence the driver resolved for the request (ExecutionError.Idempotent),
// so the caller does not restate it:
//
//   - nil and errors whose Kind is not Transient are not retryable;
//   - the caller's own cancellation or expired deadline is not either: an
//     error matching context.Canceled, or context.DeadlineExceeded without
//     matching ErrRequestTimeout, ends under a context no retry can outlive,
//     so Retryable needs no context;
//   - a request that NotExecuted reports is retryable;
//   - one that MaybeExecuted or PartiallyExecuted reports is retryable only
//     when err holds an ExecutionError whose Idempotent is set.
//
// The driver's own Timeout and PageTimeout, which match ErrRequestTimeout,
// are retryable under the rules above.
func Retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) ||
		(errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrRequestTimeout)) {
		return false
	}
	if !KindOf(err).Transient() {
		return false
	}
	switch ExecutionStatusOf(err) {
	case NotExecuted:
		return true
	case MaybeExecuted, PartiallyExecuted:
		failure, ok := errors.AsType[*ExecutionError](err)
		return ok && failure.Idempotent
	default:
		return false
	}
}
