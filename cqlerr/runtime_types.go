// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"errors"

	"github.com/scylladb/gocql/v2/cql"
)

var (
	ErrClosed           = errors.New("gocql: closed")
	ErrPagingStateLimit = errors.New("gocql: paging state history limit exceeded")
	ErrPagingComplete   = errors.New("gocql: completed page cannot be resumed")
	ErrPagingNoProgress = errors.New("gocql: paging made no progress")
	ErrNoRows           = errors.New("gocql: no rows")
)

// SchemaChangedError reports that a prepared statement's bound parameters
// changed between executions.
type SchemaChangedError struct {
	Statement         string
	Previous, Current cql.Columns
}

func (e *SchemaChangedError) Error() string {
	return "gocql: bound parameter schema changed for " + e.Statement
}

// Kind returns KindInvalid.
func (e *SchemaChangedError) Kind() Kind { return KindInvalid }
