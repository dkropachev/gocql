// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// These bindings expose internal execution seams to the historical protocol and
// cancellation fixtures only. They are absent from the supported public API.
import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

type Bound[R any] = implSession.Bound[R]

func BindUnpreparedBorrowed[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implSession.BindUnpreparedBorrowed[P, R](internalStatement(a0), a1, a2)
}

func Run[R any](a0 context.Context, a1 *testSession, a2 *Bound[R], a3 CompiledOptions) (Rows[R], error) {
	result, err := implSession.Run[R](a0, (*implSession.Session)(a1).Core(), a2, implSession.CompiledOptions(a3))
	return Rows[R](result), err
}
