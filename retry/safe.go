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
	if _, ok := errors.AsType[*cqlerr.UnavailableError](a.Err); ok {
		return Decision{Action: Next}
	}
	if read, ok := errors.AsType[*cqlerr.ReadTimeoutError](a.Err); ok && read.Received >= read.BlockFor && !read.DataPresent {
		return Decision{Action: Same}
	}
	if server, ok := errors.AsType[cqlerr.ServerError](a.Err); ok {
		if cqlerr.KindOf(server) == cqlerr.KindOverloaded {
			// Rejected before execution: another host may serve even a
			// non-idempotent request.
			return Decision{Action: Next}
		}
		if a.Idempotent && server.Code() == cqlerr.CodeServer {
			return Decision{Action: Next}
		}
		return Decision{}
	}
	if _, ok := errors.AsType[*cqlerr.SchemaChangedError](a.Err); ok {
		return Decision{}
	}
	if _, ok := errors.AsType[*cqlerr.ValidationError](a.Err); ok {
		return Decision{}
	}
	if _, ok := errors.AsType[*cqlerr.ProtocolError](a.Err); ok {
		return Decision{}
	}
	if a.Idempotent || a.ExecutionStatus == cqlerr.NotExecuted {
		return Decision{Action: Next}
	}
	return Decision{}
}
