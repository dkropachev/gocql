// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build integration

package integration

// These bindings expose internal execution seams to the historical protocol and
// cancellation fixtures only. They are absent from the supported public API.
import (
	"context"
	"errors"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

type Bound[R any] = implExecution.Bound[R]

// batchItem is one binding of a test batch. It is copied into a gocql.Batch
// when the batch is sent, so closing the binding first fails the batch.
type batchItem struct {
	add func(*gocql.Batch, *gocql.Session) error
}

func BatchEntry[R any](bound *Bound[R]) (batchItem, error) {
	if bound == nil {
		return batchItem{}, errors.New("nil binding")
	}
	return batchItem{add: func(batch *gocql.Batch, s *gocql.Session) error {
		return implExecution.InternalAddBound(batch, implExecution.InternalCore(s), bound)
	}}, nil
}

func buildBatch(s *gocql.Session, items []batchItem) (*gocql.Batch, error) {
	batch := new(gocql.Batch)
	for _, item := range items {
		if err := item.add(batch, s); err != nil {
			return nil, err
		}
	}
	return batch, nil
}

func execBatch(ctx context.Context, s *gocql.Session, kind cql.BatchKind, items []batchItem, options gocql.SessionOptions) (gocql.Result, error) {
	batch, err := buildBatch(s, items)
	if err != nil {
		return gocql.Result{}, err
	}
	return implExecution.InternalExecBatch(ctx, implExecution.InternalCore(s), kind, batch, options)
}

func runBatch[R any](ctx context.Context, s *gocql.Session, kind cql.BatchKind, items []batchItem, decoder cql.RowDecoder[R], options gocql.SessionOptions) (*gocql.Page[R], error) {
	batch, err := buildBatch(s, items)
	if err != nil {
		return nil, err
	}
	return implExecution.InternalRunBatch(ctx, implExecution.InternalCore(s), kind, batch, decoder, options)
}

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
	return implExecution.Exec[R](a0, implExecution.InternalCore(a1), a2, a3)
}

func Run[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (gocql.Rows[R], error) {
	return implExecution.Run[R](a0, implExecution.InternalCore(a1), a2, a3)
}

func RunPage[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.PageState, a4 gocql.SessionOptions) (*gocql.Page[R], error) {
	return implExecution.RunPage[R](a0, implExecution.InternalCore(a1), a2, a3, a4)
}

func RunStream[B any, R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[B], a3 cql.StreamRowDecoder[R], a4 gocql.StreamOptions) (*gocql.RowStream[R], error) {
	return implExecution.RunStream[B, R](a0, implExecution.InternalCore(a1), a2, a3, a4)
}

func RunUnpaged[R any](a0 context.Context, a1 *gocql.Session, a2 *Bound[R], a3 gocql.SessionOptions) (*gocql.Page[R], error) {
	return implExecution.RunUnpaged[R](a0, implExecution.InternalCore(a1), a2, a3)
}
