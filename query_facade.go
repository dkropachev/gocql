// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/execution"
)

type Query[P, R any] = execution.Query[P, R]

// Deprecated: use cqlerr.ErrBusy.
var ErrBusy = execution.ErrBusy

// Deprecated: use cqlerr.ErrStaleRows.
var ErrStaleRows = execution.ErrStaleRows

type StreamQuery[P, R any] = execution.StreamQuery[P, R]

// Deprecated: use (*Prepared[P, B]).StreamQuery.
func NewStreamQuery[P, B, R any](prepared *Prepared[P, B], decoder cql.StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	return prepared.StreamQuery(decoder, options)
}

func NewUnpreparedStreamQuery[P, B, R any](session *Session, statement cql.Statement[P, B], decoder cql.StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	return execution.NewUnpreparedStreamQuery(session, statement, decoder, options)
}

func NewUnpreparedQuery[P, R any](session *Session, statement cql.Statement[P, R], options Options) (*Query[P, R], error) {
	return execution.NewUnpreparedQuery(session, statement, options)
}
