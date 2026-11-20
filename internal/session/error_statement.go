// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"
	"unicode/utf8"
)

// describeStatement adds request's statement to the ExecutionError in err when
// the session reports statements in errors (ConfigBuilder.WithErrorStatementText):
// its CQL text cut to the configured limit, and for a batch the statement
// count with the first statement. Bound values are never added. An error that
// already names a statement keeps it.
func (rt *InternalSessionRuntime) describeStatement(err error, request *InternalExecutionRequest) {
	if rt == nil || request == nil {
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
	statement, count := request.statement, 0
	if batch := request.batch; batch != nil {
		if len(batch.entries) == 0 {
			return
		}
		statement, count = batch.entries[0].statement, len(batch.entries)
	}
	if statement == "" {
		return
	}
	failure.Statement = truncateStatement(statement, limit)
	failure.BatchStatements = count
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
