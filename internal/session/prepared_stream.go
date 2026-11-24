// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// StreamStatement pairs CQL and its parameters with a streaming result decoder.
type StreamStatement[P, R any] struct {
	CQL         string
	Params      cql.ParamEncoder[P]
	Rows        cql.StreamRowDecoder[R]
	BindMarkers cql.Columns
	Kind        cql.StatementKind
	Idempotent  bool
}

// PreparedStream is immutable and shareable. Each worker creates its own Query.
// Its initial plan is immutable; each query revalidates changed result metadata.
type PreparedStream[P, R any] struct {
	prepared *Prepared[P, cql.EmptyRow]
	decoder  cql.StreamRowDecoder[R]
	plan     cql.CompiledStreamRow[R]
	columns  cql.Columns
}

func (s *Session) PrepareStream[P, R any](ctx context.Context, statement StreamStatement[P, R], options PrepareOptions) (*PreparedStream[P, R], error) {
	return PrepareStream(ctx, s.Core(), statement, options)
}

func PrepareStream[P, R any](ctx context.Context, session *implCluster.Core, statement StreamStatement[P, R], options PrepareOptions) (*PreparedStream[P, R], error) {
	if statement.Rows == nil {
		return nil, cqltype.TypeValidation("stream decoder", "nil decoder")
	}
	base := binding.Statement[P, cql.EmptyRow]{CQL: statement.CQL, Params: statement.Params, Kind: statement.Kind, Idempotent: statement.Idempotent, Rows: execRowsDecoder{}}
	prepared, err := prepareStatement(ctx, session, &base, options)
	if err != nil {
		return nil, err
	}
	var plan cql.CompiledStreamRow[R]
	// Conditional PREPARE responses can omit result metadata. In that case
	// the first actual response validates the stream decoder before any output.
	if prepared.columns.Len() != 0 || prepared.kind == cql.OrdinaryStatement {
		plan, err = statement.Rows.Compile(prepared.columns)
		if err != nil {
			return nil, err
		}
		if plan == nil {
			return nil, cqltype.TypeValidation("stream plan", "decoder returned nil plan")
		}
	}
	p := &Prepared[P, cql.EmptyRow]{privateStorage: PreparedStorage[P, cql.EmptyRow]{InternalState: prepared.state, kind: prepared.kind, idempotent: prepared.idempotent, params: prepared.params, rows: execRowsDecoder{}}}
	return &PreparedStream[P, R]{prepared: p, decoder: statement.Rows, columns: prepared.columns, plan: plan}, nil
}

// Query creates independent worker-owned execution state with fixed options.
func (p *PreparedStream[P, R]) Query(options StreamOptions) (*StreamQuery[P, R], error) {
	if p == nil || p.prepared == nil {
		return nil, cqltype.TypeValidation("prepared stream", "nil handle")
	}
	q, err := newStreamQuery(p.prepared, p.decoder, options)
	if err != nil {
		return nil, err
	}
	q.plans.columns, q.plans.plan = p.columns, p.plan
	return q, nil
}

func (p *PreparedStream[P, R]) Parameters() cql.Columns {
	if p == nil {
		return cql.Columns{}
	}
	return p.prepared.Parameters()
}
func (p *PreparedStream[P, R]) Columns() cql.Columns {
	if p == nil {
		return cql.Columns{}
	}
	return p.prepared.Columns()
}
func (p *PreparedStream[P, R]) Info() PreparedInfo {
	if p == nil {
		return PreparedInfo{}
	}
	return p.prepared.Info()
}
