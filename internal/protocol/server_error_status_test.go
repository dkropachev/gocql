// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"errors"
	"fmt"
	"testing"
)

func TestServerErrorCodes(t *testing.T) {
	t.Parallel()
	timeout := &WriteTimeoutError{serverError: serverError{message: "Operation timed out"}}
	syntax := NewOtherServerError(CodeSyntax, "line 1:0 no viable alternative")
	wrapped := fmt.Errorf("attempt: %w", timeout)

	if !errors.Is(wrapped, CodeWriteTimeout) || errors.Is(wrapped, CodeReadTimeout) {
		t.Fatal("code does not match by code")
	}
	if !errors.Is(syntax, CodeSyntax) || errors.Is(syntax, CodeInvalid) {
		t.Fatal("generic server error does not match its code")
	}
	if got, want := timeout.Error(), "write timeout: Operation timed out"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got, want := NewOtherServerError(0x7777, "").Error(), "server error 0x7777"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if got, want := (&RateLimitReachedError{serverError: serverError{message: "slow down"}}).Error(), "rate limit reached: slow down"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	// Every concrete type reports its code without a decoder setting it, so
	// an error built by a caller's test still matches.
	for _, test := range []struct {
		err  ServerError
		code ErrorCode
	}{
		{&UnavailableError{}, CodeUnavailable},
		{&WriteTimeoutError{}, CodeWriteTimeout},
		{&WriteFailureError{}, CodeWriteFailure},
		{&CDCWriteFailureError{}, CodeCDCWriteFailure},
		{&ReadTimeoutError{}, CodeReadTimeout},
		{&ReadFailureError{}, CodeReadFailure},
		{&AlreadyExistsError{}, CodeAlreadyExists},
		{&UnpreparedError{}, CodeUnprepared},
		{&FunctionFailureError{}, CodeFunctionFailure},
		{&CASWriteUnknownError{}, CodeCASWriteUnknown},
		{&RateLimitReachedError{}, CodeRateLimitReached},
		{NewOtherServerError(CodeOverloaded, ""), CodeOverloaded},
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
	decode := func(code int32, warnings []string) ServerError {
		f := NewFramer(nil, ProtoVersion4)
		f.WriteInt(code)
		f.WriteString("busy")
		return decodeServerError(framerErrorBody{&Framer{Buf: f.Buf, Proto: ProtoVersion4}}, warnings, ProtoVersion4, 0)
	}
	err := decode(int32(CodeOverloaded), []string{"w"})
	if got := err.Warnings(); len(got) != 1 || got[0] != "w" || err.Message() != "busy" || err.Code() != CodeOverloaded {
		t.Fatalf("decoded %#v", err)
	}
	if wide, ok := decode(0x10000|int32(CodeUnavailable), nil).(*OtherServerError); !ok || wide.Message() != "busy" {
		t.Fatalf("wide code decoded as %#v", wide)
	}
}
