// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"context"
	"errors"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Safe retries bounded transient failures only when replay is safe.
//
// It moves to the next host after Unavailable, Overloaded and Bootstrapping,
// which reject a request before it runs, with one exception: a
// non-idempotent unlogged or counter batch is not retried after Unavailable
// or Overloaded. Cassandra checks such a batch one partition at a time and
// can reject it after it sent the earlier partitions' writes, so a replay
// could apply them twice; the driver then reports the attempt MaybeExecuted.
// The rule holds on every server, ScyllaDB included: the driver does not rely
// on the order in which a server checks a batch's partitions. It also holds
// for a conditional batch, whose kind the driver may not know. A logged batch
// is checked whole before its batch log is written, so it is retried.
//
// It retries in place a read timeout that had enough replicas but no data,
// and moves an idempotent request on after a coordinator server error. Every
// other server error stops. Any other failure moves on only for an idempotent
// request or an attempt whose status is cqlerr.NotExecuted. The caller's
// cancellation or deadline never retries.
//
// The zero Safe is ready to use.
type Safe struct {
	// MaxAttempts bounds the attempts, the first included; zero or less
	// means three.
	MaxAttempts int
}

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
		if partialBatch(a) {
			return Decision{}
		}
		return Decision{Action: Next}
	}
	if read, ok := errors.AsType[*cqlerr.ReadTimeoutError](a.Err); ok && read.Received >= read.BlockFor && !read.DataPresent {
		return Decision{Action: Same}
	}
	if server, ok := errors.AsType[cqlerr.ServerError](a.Err); ok {
		if cqlerr.KindOf(server) == cqlerr.KindOverloaded {
			// Rejected before execution: another host may serve even a
			// non-idempotent request, unless it is a batch the server may
			// have applied in part.
			if server.Code() == cqlerr.CodeOverloaded && partialBatch(a) {
				return Decision{}
			}
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
	if _, ok := errors.AsType[*cql.ValidationError](a.Err); ok {
		return Decision{}
	}
	if _, ok := errors.AsType[*cql.ProtocolError](a.Err); ok {
		return Decision{}
	}
	if a.Idempotent || a.ExecutionStatus == cqlerr.NotExecuted {
		return Decision{Action: Next}
	}
	return Decision{}
}

// partialBatch reports whether a is an attempt of a non-idempotent batch a
// server may have applied in part before it rejected it as Unavailable or
// Overloaded: an unlogged or counter batch.
func partialBatch(a Attempt) bool {
	return !a.Idempotent && a.Batch.Set && a.Batch.Value != cql.LoggedBatch
}
