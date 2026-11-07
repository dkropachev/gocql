// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"errors"
	"fmt"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestServerErrorCodes(t *testing.T) {
	t.Parallel()
	timeout := &RequestErrWriteTimeout{ErrorFrame: frm.ErrorFrame{Code: ErrCodeWriteTimeout, Message: "Operation timed out"}}
	syntax := &RequestErrServer{ErrorFrame: frm.ErrorFrame{Code: ErrCodeSyntax, Message: "line 1:0 no viable alternative"}}
	wrapped := fmt.Errorf("attempt: %w", timeout)

	if !errors.Is(wrapped, ErrorCode(ErrCodeWriteTimeout)) || errors.Is(wrapped, ErrorCode(ErrCodeReadTimeout)) {
		t.Fatal("code sentinel does not match by code")
	}
	if !errors.Is(syntax, ErrorCode(ErrCodeSyntax)) {
		t.Fatal("generic server error does not match its code")
	}
	if got, want := timeout.Error(), "write timeout: Operation timed out"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got, want := (&UnknownServerError{ErrorFrame: frm.ErrorFrame{Code: 0x7777}}).Error(), "server error 0x7777"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got, want := (&RequestErrRateLimitReached{ErrorFrame: frm.ErrorFrame{Code: 0xF000, Message: "slow down"}}).Error(), "rate limit reached: slow down"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
