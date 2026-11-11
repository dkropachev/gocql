// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// Reusable unprepared queries are not public API; tests use them to run
// statements against loopback servers without a PREPARE round trip.

// unpreparedQuery compiles parameters once against statement.BindMarkers
// and captures each execution's input in query-owned storage, including
// values passed to custom codecs.
func unpreparedQuery[P, R any](s *implCluster.Core, statement binding.Statement[P, R], values Options) (*Query[P, R], error) {
	options, err := queryOptions(s, values)
	if err != nil {
		return nil, err
	}
	return newUnpreparedQuery(s, statement, options)
}

// unpreparedStreamQuery uses the same owned input and receive-mode contract
// for a streaming statement executed without server preparation.
func unpreparedStreamQuery[P, R any](session *implCluster.Core, statement StreamStatement[P, R], options StreamOptions) (*StreamQuery[P, R], error) {
	base := binding.Statement[P, codec.EmptyRecord]{Kind: statement.Kind, CQL: statement.CQL, Params: statement.Params, Rows: execRowsDecoder{}, BindMarkers: statement.BindMarkers}
	if err := binding.ValidateStatement(&base); err != nil {
		return nil, err
	}
	decoder := statement.Rows
	if decoder == nil {
		return nil, cqltype.TypeValidation("stream decoder", "nil decoder")
	}
	if _, err := transport.InternalCheckedStreamLimits(options.Limits); err != nil {
		return nil, err
	}
	if v := options.Options; v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, cqltype.TypeValidation("stream prefetch", "must be zero")
	}
	q, err := unpreparedQuery(session, base, options.Options)
	if err != nil {
		return nil, err
	}
	// Streams hand their context to exclusive transport I/O; keep the
	// standard per-execution context there.
	q.state.execCtx = nil
	return &StreamQuery[P, R]{query: q, decoder: decoder, options: options}, nil
}
