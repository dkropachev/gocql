//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
)

// IntegrationRunStream streams the rows of bound on session for integration
// tests, which cannot reach the unexported runtime.
func IntegrationRunStream[B, R any](ctx context.Context, session *implCluster.Core, bound *Bound[B], decoder codec.StreamRowDecoder[R], options StreamOptions) (*RowStream[R], error) {
	frozen, err := queryOptions(session, options.Options)
	if err != nil {
		return nil, err
	}
	return runtimeOf(session).runStream(ctx, bound, decoder, frozen, options.Limits, options.Incremental, nil)
}
