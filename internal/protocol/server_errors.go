// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"github.com/scylladb/gocql/v2/cqlerr"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/link"
)

// decodeServerErrorHook is package cqlerr's decoder of ERROR bodies.
var decodeServerErrorHook = link.Get[func(any, []string, bool, int) cqlerr.ServerError](link.DecodeServerError)

// decodeServerError decodes an ERROR response body into its typed error. r
// is a response body positioned at the error code, such as a *TypedParser or
// a framerErrorBody. It is the only decoder of server errors.
func decodeServerError(r any, warnings []string, proto byte, rateLimitCode int) cqlerr.ServerError {
	return decodeServerErrorHook(r, warnings, VersionFor(proto).FailureReasonMap, rateLimitCode)
}

// ErrorResponseFrame is a parsed ERROR response: the frame that carries a
// server error. The header stays here, off the error a caller sees; Err is
// what the driver returns.
type ErrorResponseFrame struct {
	Err cqlerr.ServerError
	frm.FrameHeader
}

func (f *ErrorResponseFrame) Error() string { return f.Err.Error() }

func (f *ErrorResponseFrame) Unwrap() error { return f.Err }
