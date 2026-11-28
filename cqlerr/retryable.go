// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "errors"

// Retryable reports whether sending the request that failed with err again
// can succeed and is safe. It is meant for the error a call returns, which
// holds an ExecutionError; a retry policy, whose retry.Attempt.Err holds
// none, reads retry.Attempt.Retryable instead. It combines KindOf,
// ExecutionStatusOf, the idempotence the driver resolved for the request
// (ExecutionError.Idempotent) and ConditionalUnknown, so the caller does not
// restate them:
//
//   - nil and errors whose Kind is not Transient are not retryable, the
//     caller's own cancellation or expired deadline (KindCanceled) among
//     them: it ends under a context no retry can outlive, so Retryable
//     needs no context;
//   - a request that NotExecuted reports is retryable;
//   - one that MaybeExecuted or PartiallyExecuted reports is retryable only
//     when err holds an ExecutionError whose Idempotent is set and
//     ConditionalUnknown is false: an LWT that may have applied is not
//     retryable even when marked idempotent, since its condition may no
//     longer hold.
//
// The driver's own Timeout and PageTimeout, which match ErrRequestTimeout,
// are retryable under the rules above.
func Retryable(err error) bool {
	if err == nil || !KindOf(err).Transient() {
		return false
	}
	switch ExecutionStatusOf(err) {
	case NotExecuted:
		return true
	case MaybeExecuted, PartiallyExecuted:
		failure, ok := errors.AsType[*ExecutionError](err)
		return ok && failure.Idempotent && !ConditionalUnknown(failure)
	default:
		return false
	}
}
