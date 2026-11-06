// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build integration

package integration

// These bindings expose internal execution seams to the historical protocol and
// cancellation fixtures only. They are absent from the supported public API.
import (
	"context"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

type Bound[R any] = implExecution.Bound[R]

func BatchEntry[R any](a0 *Bound[R]) (gocql.BatchItem, error) { return implExecution.BatchEntry[R](a0) }

func BindBorrowed[P any, R any](a0 *gocql.Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindBorrowed[P, R](a0, a1)
}

func BindOwned[P any, R any](a0 *gocql.Prepared[P, R], a1 P) (*Bound[R], error) {
	return implExecution.BindOwned[P, R](a0, a1)
}

func BindUnpreparedOwned[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implExecution.BindUnpreparedOwned[P, R](a0, a1, a2)
}

func Exec[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (gocql.Result, error) {
	return implExecution.Exec[R](a0, a1, a2, a3)
}

func Run[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (gocql.Rows[R], error) {
	return implExecution.Run[R](a0, a1, a2, a3)
}

func RunPage[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.PageState, a4 gocql.SessionOptions) (*gocql.Page[R], error) {
	return implExecution.RunPage[R](a0, a1, a2, a3, a4)
}

func RunStream[B any, R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[B], a3 cql.StreamRowDecoder[R], a4 gocql.StreamOptions) (*gocql.RowStream[R], error) {
	return implExecution.RunStream[B, R](a0, a1, a2, a3, a4)
}

func RunUnpaged[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (*gocql.Page[R], error) {
	return implExecution.RunUnpaged[R](a0, a1, a2, a3)
}
