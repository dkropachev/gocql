// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build integration

package integration

// These bindings expose internal execution seams to the historical protocol and
// cancellation fixtures only. They are absent from the supported public API.
import (
	"context"
	"errors"

	"github.com/scylladb/gocql/v2/internal/model"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

type Bound[R any] = implSession.Bound[R]

// batchItem is one binding of a test batch. It is copied into a gocql.Batch
// when the batch is sent, so closing the binding first fails the batch.
type batchItem struct {
	add func(*gocql.Batch, *gocql.Session[testDialect]) error
}

func BatchEntry[R any](bound *Bound[R]) (batchItem, error) {
	if bound == nil {
		return batchItem{}, errors.New("nil binding")
	}
	return batchItem{add: func(batch *gocql.Batch, s *gocql.Session[testDialect]) error {
		return implSession.InternalAddBound((*implSession.Batch)(batch), (*implSession.Session)(s).Core(), bound)
	}}, nil
}

func buildBatch(s *gocql.Session[testDialect], kind cql.BatchKind, items []batchItem) (*gocql.Batch, error) {
	batch := &gocql.Batch{Kind: kind}
	for _, item := range items {
		if err := item.add(batch, s); err != nil {
			return nil, err
		}
	}
	return batch, nil
}

func execBatch(ctx context.Context, s *gocql.Session[testDialect], kind cql.BatchKind, items []batchItem, options gocql.CompiledOptions) (gocql.Result, error) {
	batch, err := buildBatch(s, kind, items)
	if err != nil {
		return gocql.Result{}, err
	}
	return implSession.InternalExecBatch(ctx, (*implSession.Session)(s).Core(), (*implSession.Batch)(batch), implSession.CompiledOptions(options))
}

func runBatch[R any](ctx context.Context, s *gocql.Session[testDialect], kind cql.BatchKind, items []batchItem, decoder cql.RowDecoder[R], options gocql.CompiledOptions) (*gocql.Page[R], error) {
	batch, err := buildBatch(s, kind, items)
	if err != nil {
		return nil, err
	}
	result, err := implSession.InternalRunBatch(ctx, (*implSession.Session)(s).Core(), (*implSession.Batch)(batch), decoder, implSession.CompiledOptions(options))
	return (*gocql.Page[R])(result), err
}

func BindBorrowed[P any, R any](a0 *gocql.Prepared[P, R], a1 P) (*Bound[R], error) {
	return implSession.BindBorrowed[P, R]((*implSession.Prepared[P, R])(a0), a1)
}

func BindOwned[P any, R any](a0 *gocql.Prepared[P, R], a1 P) (*Bound[R], error) {
	return implSession.BindOwned[P, R]((*implSession.Prepared[P, R])(a0), a1)
}

func BindUnpreparedOwned[P any, R any](a0 cql.Statement[P, R], a1 cql.Columns, a2 P) (*Bound[R], error) {
	return implSession.BindUnpreparedOwned[P, R](fixtureStatement(a0), a1, a2)
}

func Exec[R any](a0 context.Context, a1 *gocql.Session[testDialect], a2 *Bound[R], a3 gocql.CompiledOptions) (gocql.Result, error) {
	return implSession.Exec[R](a0, (*implSession.Session)(a1).Core(), a2, implSession.CompiledOptions(a3))
}

func Run[R any](a0 context.Context, a1 *gocql.Session[testDialect], a2 *Bound[R], a3 gocql.CompiledOptions) (gocql.Rows[R], error) {
	result, err := implSession.Run[R](a0, (*implSession.Session)(a1).Core(), a2, implSession.CompiledOptions(a3))
	return gocql.Rows[R](result), err
}

func RunPage[R any](a0 context.Context, a1 *gocql.Session[testDialect], a2 *Bound[R], a3 gocql.PageState, a4 gocql.CompiledOptions) (*gocql.Page[R], error) {
	result, err := implSession.RunPage[R](a0, (*implSession.Session)(a1).Core(), a2, model.PageState(a3), 0, implSession.CompiledOptions(a4))
	return (*gocql.Page[R])(result), err
}

func RunStream[B any, R any](a0 context.Context, a1 *gocql.Session[testDialect], a2 *Bound[B], a3 stream.RowDecoder[R], a4 gocql.StreamOptions) (*gocql.RowStream[R], error) {
	result, err := implSession.IntegrationRunStream[B, R](a0, (*implSession.Session)(a1).Core(), a2, a3, implSession.StreamOptions{Options: implSession.CompiledOptions(bindOptions(a1, a4.Options)).Values(), Limits: a4.Limits, Incremental: a4.Incremental})
	return (*gocql.RowStream[R])(result), err
}

// preparedData crosses the public facade only for protocol/cache fixtures.
func preparedData[P, R any](p *gocql.Prepared[P, R]) *implSession.PreparedStorage[P, R] {
	return implSession.PreparedData((*implSession.Prepared[P, R])(p))
}
