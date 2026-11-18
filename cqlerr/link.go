// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "github.com/scylladb/gocql/v2/internal/link"

// The engine decodes server errors through internal/link; see
// internal/protocol.
func init() {
	link.Set(link.DecodeServerError, func(body any, warnings []string, failureReasonMap bool, rateLimitCode int) ServerError {
		return decodeServerError(body.(errorBody), warnings, failureReasonMap, rateLimitCode)
	})
	link.Set(link.ServerErrorMessage, func(err ServerError, message string, warnings []string) {
		err.setBase(message, warnings)
	})
}

// setBase sets the message and warnings decodeServerError otherwise sets.
func (e *serverError) setBase(message string, warnings []string) {
	e.message, e.warnings = message, warnings
}
