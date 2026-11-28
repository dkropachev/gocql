// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"
	"unicode/utf8"
)

// describeStatement records request's resolved idempotence and statement
// kind in the ExecutionError in err, and adds its statement when the session reports
// statements in errors
// (ConfigBuilder.WithErrorStatementText): its CQL text cut to the configured
// limit, and for a batch the statement count with the first statement. Bound
// values are never added. An error that already names a statement keeps it.
func (rt *InternalSessionRuntime) describeStatement(err error, request *InternalExecutionRequest) {
	if request == nil {
		return
	}
	if failure, ok := errors.AsType[*ExecutionError](err); ok {
		failure.StatementKind = request.statementKind()
		if request.options != nil {
			// A request refused before its options were resolved keeps
			// what the resolved request recorded, if anything.
			failure.Idempotent = request.isIdempotent()
		}
	}
	var entries []batchEntry
	if request.batch != nil {
		entries = request.batch.entries
	}
	rt.describe(err, request.statement, entries, request.batch != nil)
}

// describeBatch is describeStatement for a batch refused before it became a
// request.
func (rt *InternalSessionRuntime) describeBatch(err error, batch *Batch) {
	if batch != nil {
		rt.describe(err, "", batch.entries, true)
	}
}

// describe adds statement, or the first of a batch's entries, to the
// ExecutionError in err; see describeStatement.
func (rt *InternalSessionRuntime) describe(err error, statement string, entries []batchEntry, batch bool) {
	if rt == nil {
		return
	}
	limit := rt.config.ErrorStatementBytes
	if limit <= 0 {
		return
	}
	failure, ok := errors.AsType[*ExecutionError](err)
	if !ok || failure.Statement != "" {
		return
	}
	count := 0
	if batch {
		if len(entries) == 0 {
			return
		}
		statement, count = entries[0].statement, len(entries)
	}
	if statement == "" {
		return
	}
	failure.Statement = truncateStatement(statement, limit)
	failure.BatchSize = count
}

// truncateStatement returns statement cut to at most limit bytes at a
// character boundary, followed by "..." when it was cut.
func truncateStatement(statement string, limit int) string {
	if len(statement) <= limit {
		return statement
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(statement[cut]) {
		cut--
	}
	return statement[:cut] + "..."
}
