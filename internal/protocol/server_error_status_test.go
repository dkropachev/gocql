// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"errors"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestServerErrorCodes(t *testing.T) {
	t.Parallel()
	timeout := runtimeerrors.WithMessage(&cqlerr.WriteTimeoutError{}, "Operation timed out")
	syntax := cqlerr.NewOtherServerError(cqlerr.CodeSyntax, "line 1:0 no viable alternative")
	wrapped := fmt.Errorf("attempt: %w", timeout)

	if !errors.Is(wrapped, cqlerr.CodeWriteTimeout) || errors.Is(wrapped, cqlerr.CodeReadTimeout) {
		t.Fatal("code does not match by code")
	}
	if !errors.Is(syntax, cqlerr.CodeSyntax) || errors.Is(syntax, cqlerr.CodeInvalid) {
		t.Fatal("generic server error does not match its code")
	}
	if got, want := timeout.Error(), "write timeout: Operation timed out"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got, want := cqlerr.NewOtherServerError(0x7777, "").Error(), "server error 0x7777"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got, want := (runtimeerrors.WithMessage(&cqlerr.RateLimitReachedError{}, "slow down")).Error(), "rate limit reached: slow down"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	// Every concrete type reports its code without a decoder setting it, so
	// an error built by a caller's test still matches.
	for _, test := range []struct {
		err  cqlerr.ServerError
		code cqlerr.ErrorCode
	}{
		{&cqlerr.UnavailableError{}, cqlerr.CodeUnavailable},
		{&cqlerr.WriteTimeoutError{}, cqlerr.CodeWriteTimeout},
		{&cqlerr.WriteFailureError{}, cqlerr.CodeWriteFailure},
		{&cqlerr.CDCWriteFailureError{}, cqlerr.CodeCDCWriteFailure},
		{&cqlerr.ReadTimeoutError{}, cqlerr.CodeReadTimeout},
		{&cqlerr.ReadFailureError{}, cqlerr.CodeReadFailure},
		{&cqlerr.AlreadyExistsError{}, cqlerr.CodeAlreadyExists},
		{&cqlerr.UnpreparedError{}, cqlerr.CodeUnprepared},
		{&cqlerr.FunctionFailureError{}, cqlerr.CodeFunctionFailure},
		{&cqlerr.CASWriteUnknownError{}, cqlerr.CodeCASWriteUnknown},
		{&cqlerr.RateLimitReachedError{}, cqlerr.CodeRateLimitReached},
		{cqlerr.NewOtherServerError(cqlerr.CodeOverloaded, ""), cqlerr.CodeOverloaded},
	} {
		if test.err.Code() != test.code || !errors.Is(test.err, test.code) || test.err.Error() != test.code.Error() {
			t.Errorf("%T: code %v, want %v", test.err, test.err.Code(), test.code)
		}
	}
}

// Server warnings sent with an ERROR response stay on the error, and a code
// wider than 16 bits is not mistaken for the known code in its low bits.
func TestDecodeServerError(t *testing.T) {
	t.Parallel()
	decode := func(code int32, warnings []string) cqlerr.ServerError {
		f := NewFramer(nil, ProtoVersion4)
		f.WriteInt(code)
		f.WriteString("busy")
		return decodeServerError(framerErrorBody{&Framer{Buf: f.Buf, Proto: ProtoVersion4}}, warnings, ProtoVersion4, 0)
	}
	err := decode(int32(cqlerr.CodeOverloaded), []string{"w"})
	if got := err.Warnings(); len(got) != 1 || got[0] != "w" || err.Message() != "busy" || err.Code() != cqlerr.CodeOverloaded {
		t.Fatalf("decoded %#v", err)
	}
	if wide, ok := decode(0x10000|int32(cqlerr.CodeUnavailable), nil).(*cqlerr.OtherServerError); !ok || wide.Message() != "busy" {
		t.Fatalf("wide code decoded as %#v", wide)
	}
}
