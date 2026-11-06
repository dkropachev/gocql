// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"context"
	"errors"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// Safe retries bounded transient failures only when replay is safe.
type Safe struct{ MaxAttempts int }

// Decide implements Policy.
func (p Safe) Decide(a Attempt) Decision {
	limit := p.MaxAttempts
	if limit <= 0 {
		limit = 3
	}
	if a.Number >= limit || errors.Is(a.Err, context.Canceled) || errors.Is(a.Err, context.DeadlineExceeded) {
		return Decision{}
	}
	var unavailable *protocol.RequestErrUnavailable
	if errors.As(a.Err, &unavailable) {
		return Decision{Action: Next}
	}
	var read *protocol.RequestErrReadTimeout
	if errors.As(a.Err, &read) && read.Received >= read.BlockFor && read.DataPresent == 0 {
		return Decision{Action: Same}
	}
	if !a.Idempotent {
		var query *runtimeerrors.QueryError
		if !a.BytesWritten && errors.As(a.Err, &query) && !query.PotentiallyExecuted() {
			return Decision{Action: Next}
		}
		return Decision{}
	}
	var server protocol.RequestError
	if errors.As(a.Err, &server) {
		switch server.GetCode() {
		case protocol.ErrCodeOverloaded, protocol.ErrCodeBootstrapping, protocol.ErrCodeServer:
			return Decision{Action: Next}
		default:
			return Decision{}
		}
	}
	var schema *runtimeerrors.SchemaChangedError
	var validation *cqltype.ValidationError
	var protocolErr *cqltype.ProtocolError
	if errors.As(a.Err, &schema) || errors.As(a.Err, &validation) || errors.As(a.Err, &protocolErr) {
		return Decision{}
	}
	return Decision{Action: Next}
}
