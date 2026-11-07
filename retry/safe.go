// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"context"
	"errors"

	"github.com/scylladb/gocql/v2/cqlerr"
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
	var unavailable *cqlerr.RequestErrUnavailable
	if errors.As(a.Err, &unavailable) {
		return Decision{Action: Next}
	}
	var read *cqlerr.RequestErrReadTimeout
	if errors.As(a.Err, &read) && read.Received >= read.BlockFor && read.DataPresent == 0 {
		return Decision{Action: Same}
	}
	var server cqlerr.RequestError
	if errors.As(a.Err, &server) {
		switch server.GetCode() {
		case cqlerr.ErrCodeOverloaded, cqlerr.ErrCodeBootstrapping:
			// Rejected before execution: another host may serve even a
			// non-idempotent request.
			return Decision{Action: Next}
		}
		if a.Idempotent && server.GetCode() == cqlerr.ErrCodeServer {
			return Decision{Action: Next}
		}
		return Decision{}
	}
	var schema *cqlerr.SchemaChangedError
	var validation *cqlerr.ValidationError
	var protocolErr *cqlerr.ProtocolError
	if errors.As(a.Err, &schema) || errors.As(a.Err, &validation) || errors.As(a.Err, &protocolErr) {
		return Decision{}
	}
	if a.Idempotent || a.ExecutionStatus == cqlerr.NotExecuted {
		return Decision{Action: Next}
	}
	return Decision{}
}
