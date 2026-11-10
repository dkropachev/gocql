// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// PreparedStream is an immutable statement prepared for caller-storage decoding.
// Share it between goroutines and create one Query per worker. It needs no Close.
// PrepareStream validates the streaming decoder against known result metadata;
// execution revalidates it whenever the actual result metadata changes.
type PreparedStream[P, R any] implExecution.PreparedStream[P, R]

// Query creates a reusable stream query with fixed options. Close the query when
// its worker finishes, and close each execution cursor before reusing it.
func (p *PreparedStream[P, R]) Query(options StreamOptions) (*StreamQuery[P, R], error) {
	q, err := (*implExecution.PreparedStream[P, R])(p).Query(internalStreamOptions(options))
	return (*StreamQuery[P, R])(q), err
}

// Parameters returns the immutable bind-marker metadata.
func (p *PreparedStream[P, R]) Parameters() cql.Columns {
	return (*implExecution.PreparedStream[P, R])(p).Parameters()
}

// Columns returns the prepared statement's immutable result metadata.
func (p *PreparedStream[P, R]) Columns() cql.Columns {
	return (*implExecution.PreparedStream[P, R])(p).Columns()
}

// Info returns preparation information.
func (p *PreparedStream[P, R]) Info() PreparedInfo {
	return (*implExecution.PreparedStream[P, R])(p).Info()
}
