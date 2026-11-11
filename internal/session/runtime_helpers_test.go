// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/model"
)

// RunStream streams the rows of bound on session, as StreamQuery.Run does
// for a captured query.
func RunStream[B, R any](ctx context.Context, session *implCluster.Core, bound *Bound[B], decoder codec.StreamRowDecoder[R], options StreamOptions) (*RowStream[R], error) {
	frozen, err := queryOptions(session, options.Options)
	if err != nil {
		return nil, err
	}
	return runtimeOf(session).runStream(ctx, bound, decoder, frozen, options.Limits, options.Incremental, nil)
}

// RunOne executes bound on s and decodes its first row into dst, reporting
// whether a row existed; without one, dst is cleared.
func RunOne[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options CompiledOptions, dst *R) (bool, model.Result, error) {
	found, result, err := runtimeOf(s).each(ctx, b, options, dst, nil, new(eachScratch))
	if !found && err == nil {
		clearDestination(dst)
	}
	return found, result, err
}

func RowsData[R any](v Rows[R]) *rowsStorage[R] { return &v.impl.privateStorage }

func RowStreamData[R any](v *RowStream[R]) *rowStreamStorage[R] { return &v.privateStorage }

func PageData[R any](v *Page[R]) *pageStorage[R] { return &v.privateStorage }
