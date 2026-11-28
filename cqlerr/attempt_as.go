// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "errors"

// AttemptAs finds the first error of type T in err's tree, as errors.AsType
// does, and otherwise in the errors of an ExecutionError's Attempts, newest
// first. An ExecutionError wraps only why the request stopped, so errors.As
// misses an earlier attempt's error: a request that failed Unavailable and
// then ran out of its Timeout reports ErrRequestTimeout, and AttemptAs finds
// the UnavailableError of the attempt before. AttemptIs is its errors.Is
// counterpart.
func AttemptAs[T error](err error) (T, bool) {
	if found, ok := errors.AsType[T](err); ok {
		return found, true
	}
	if request, ok := errors.AsType[*ExecutionError](err); ok {
		for i := len(request.Attempts) - 1; i >= 0; i-- {
			if found, ok := errors.AsType[T](request.Attempts[i].Err); ok {
				return found, true
			}
		}
	}
	var zero T
	return zero, false
}

// AttemptIs reports whether err's tree matches target, as errors.Is does, or
// otherwise the error of one of an ExecutionError's Attempts does, newest
// first, as AttemptAs searches. It finds an earlier attempt's sentinel or
// server error code, such as CodeUnavailable, that errors.Is misses.
func AttemptIs(err, target error) bool {
	if errors.Is(err, target) {
		return true
	}
	if request, ok := errors.AsType[*ExecutionError](err); ok {
		for i := len(request.Attempts) - 1; i >= 0; i-- {
			if errors.Is(request.Attempts[i].Err, target) {
				return true
			}
		}
	}
	return false
}
