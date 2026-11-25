// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "testing"

func TestFailureReasonString(t *testing.T) {
	t.Parallel()
	for r, want := range map[FailureReason]string{
		FailureReasonUnknown:               "UNKNOWN",
		FailureReasonReadTooManyTombstones: "READ_TOO_MANY_TOMBSTONES",
		FailureReasonCoordinatorBehind:     "COORDINATOR_BEHIND",
		0xF000:                             "FailureReason(0xf000)",
	} {
		if got := r.String(); got != want {
			t.Errorf("%d: String() = %q, want %q", uint16(r), got, want)
		}
	}
}

// TestServerErrorKind checks that every server error reports through
// ServerError.Kind the kind KindOf gives it.
func TestServerErrorKind(t *testing.T) {
	t.Parallel()
	for _, err := range []ServerError{
		&UnavailableError{}, &WriteTimeoutError{}, &WriteFailureError{}, &ReadTimeoutError{},
		&AlreadyExistsError{}, &UnpreparedError{}, &ReadFailureError{}, &FunctionFailureError{},
		&CASWriteUnknownError{}, &RateLimitReachedError{},
		NewOtherServerError(CodeOverloaded, ""), NewOtherServerError(CodeSyntax, ""), NewOtherServerError(CodeServer, ""),
	} {
		if got, want := err.Kind(), KindOf(err); got != want {
			t.Errorf("%T %v: Kind() = %v, KindOf = %v", err, err.Code(), got, want)
		}
	}
}
