// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// These bindings expose internal execution seams to the historical protocol and
// cancellation fixtures only. They are absent from the supported public API.
import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

type Bound[R any] = implExecution.Bound[R]

func BindUnpreparedBorrowed[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedBorrowed[P, R](a0, a1, a2)
}

func Run[R any](a0 context.Context, a1 *Session, a2 *Bound[R], a3 BoundOptions) (Rows[R], error) {
	result, err := implExecution.Run[R](a0, implExecution.InternalCore((*implExecution.Session)(a1)), a2, a3)
	return Rows[R](result), err
}
