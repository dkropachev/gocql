// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package scylla holds API that only ScyllaDB supports. Each function takes a
// *gocql.Session[gocql.ScyllaV4], so using it with a session of another
// dialect does not compile.
package scylla

import (
	gocql "github.com/scylladb/gocql/v2"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// BatchStatement returns the index, in batch, of the statement whose result
// is row i of the rows session.RunBatch returned for batch: ScyllaDB returns
// one row per conditional statement, in statement order, and none for the
// other statements. It reports false when batch has fewer than i+1
// conditional statements, or when the kind of a statement up to that one is
// unknown: declare Statement.Kind for unprepared statements, since ScyllaDB
// marks only prepared ones. The session is not used; it makes the function
// take ScyllaDB sessions only, because Cassandra's rows map to rows of the
// partition instead.
func BatchStatement(session *gocql.Session[gocql.ScyllaV4], batch *gocql.Batch, i int) (int, bool) {
	return (*implSession.Batch)(batch).InternalConditionalStatement(i)
}
