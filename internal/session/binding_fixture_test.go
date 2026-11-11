// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session_test

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// Internal immutable-binding fixtures remain separate from the public query contract.
type Bound[R any] = implSession.Bound[R]

func BindUnpreparedBorrowed[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implSession.BindUnpreparedBorrowed[P, R](binding.Statement[P, R]{CQL: a0.CQL, Params: a0.Params, Rows: a0.Rows, Kind: a0.Kind, BindMarkers: a0.BindMarkers}, a1, a2)
}

func BindUnpreparedOwned[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implSession.BindUnpreparedOwned[P, R](binding.Statement[P, R]{CQL: a0.CQL, Params: a0.Params, Rows: a0.Rows, Kind: a0.Kind, BindMarkers: a0.BindMarkers}, a1, a2)
}
