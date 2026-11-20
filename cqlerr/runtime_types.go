// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"errors"

	"github.com/scylladb/gocql/v2/cql"
)

// Query and paging errors.
var (
	// ErrClosed reports use of a closed query, handle or connection.
	ErrClosed = errors.New("gocql: closed")
	// ErrPagingStateLimit reports a paging state over the size limit, or a
	// paging history that grew past its limits.
	ErrPagingStateLimit = errors.New("gocql: paging state history limit exceeded")
	// ErrPagingComplete reports an attempt to resume paging from the last
	// page of a result. Nothing was sent.
	ErrPagingComplete = errors.New("gocql: completed page cannot be resumed")
	// ErrPagingNoProgress reports that the server returned a paging state
	// the query had already seen, so paging would loop.
	ErrPagingNoProgress = errors.New("gocql: paging made no progress")
	// ErrNoRows reports that a query expected to return a row returned none.
	ErrNoRows = errors.New("gocql: no rows")
)

// SchemaChangedError reports that a prepared statement's bound parameters
// changed between executions.
//
// The driver returns it filled in; its zero value is an error without detail.
type SchemaChangedError struct {
	Statement         string
	Previous, Current cql.Columns
}

// Error names the statement whose parameters changed.
func (e *SchemaChangedError) Error() string {
	return "gocql: bound parameter schema changed for " + e.Statement
}

// Kind returns KindInvalid.
func (e *SchemaChangedError) Kind() Kind { return KindInvalid }
