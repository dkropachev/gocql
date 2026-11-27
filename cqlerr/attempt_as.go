// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "errors"

// AttemptAs finds the first error of type T in err's tree, as errors.AsType
// does, and otherwise in the errors of an ExecutionError's Attempts, newest
// first. An ExecutionError wraps only why the request stopped, so errors.As
// misses an earlier attempt's error: a request that failed Unavailable and
// then ran out of its Timeout reports ErrRequestTimeout, and AttemptAs finds
// the UnavailableError of the attempt before.
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
