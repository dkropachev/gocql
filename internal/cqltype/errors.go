// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"fmt"

	"github.com/scylladb/gocql/v2/cql"
)

// TypeValidation returns a *cql.ValidationError for field.
func TypeValidation(field, format string, args ...any) error {
	return &cql.ValidationError{Field: field, Err: fmt.Errorf(format, args...)}
}

// NewFrameError returns a *cql.ProtocolError for a malformed frame.
func NewFrameError(format string, args ...any) *cql.ProtocolError {
	return &cql.ProtocolError{Stage: "frame", Offset: -1, Err: fmt.Errorf(format, args...)}
}
