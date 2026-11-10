// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// StreamStatement pairs CQL and its parameters with a streaming result decoder.
type StreamStatement[P, R any] struct {
	CQL    string
	Params ParamEncoder[P]
	Rows   StreamRowDecoder[R]
	Kind   StatementKind
}

// PreparedStream is immutable and shareable. Each worker creates its own Query.
// Its initial plan is immutable; each query revalidates changed result metadata.
type PreparedStream[P, R any] struct {
	prepared *Prepared[P, EmptyRecord]
	decoder  StreamRowDecoder[R]
	columns  Columns
	plan     StreamRowPlan[R]
}

func (s *Session) PrepareStream[P, R any](ctx context.Context, statement StreamStatement[P, R], options ...PrepareOptions) (*PreparedStream[P, R], error) {
	var o PrepareOptions
	if len(options) > 1 {
		return nil, typeValidation("prepare options", "at most one PrepareOptions")
	}
	if len(options) == 1 {
		o = options[0]
	}
	return PrepareStream(ctx, InternalCore(s), statement, o)
}

func PrepareStream[P, R any](ctx context.Context, session *implCluster.Core, statement StreamStatement[P, R], options PrepareOptions) (*PreparedStream[P, R], error) {
	if statement.Rows == nil {
		return nil, typeValidation("stream decoder", "nil decoder")
	}
	base := Statement[P, EmptyRecord]{CQL: statement.CQL, Params: statement.Params, Kind: statement.Kind, Rows: InternalExecRowsDecoder{}}
	prepared, err := prepareStatement(ctx, session, &base, options)
	if err != nil {
		return nil, err
	}
	var plan StreamRowPlan[R]
	// Conditional PREPARE responses can omit result metadata. In that case
	// the first actual response validates the stream decoder before any output.
	if prepared.columns.Len() != 0 || prepared.kind == OrdinaryStatement {
		plan, err = statement.Rows.Compile(prepared.columns)
		if err != nil {
			return nil, err
		}
		if plan == nil {
			return nil, typeValidation("stream plan", "decoder returned nil plan")
		}
	}
	p := &Prepared[P, EmptyRecord]{privateStorage: PreparedStorage[P, EmptyRecord]{InternalState: prepared.state, InternalKind: prepared.kind, InternalParams: prepared.params, InternalRows: InternalExecRowsDecoder{}}}
	return &PreparedStream[P, R]{prepared: p, decoder: statement.Rows, columns: prepared.columns, plan: plan}, nil
}

// Query creates independent worker-owned execution state with fixed options.
func (p *PreparedStream[P, R]) Query(options StreamOptions) (*StreamQuery[P, R], error) {
	if p == nil || p.prepared == nil {
		return nil, typeValidation("prepared stream", "nil handle")
	}
	q, err := p.prepared.StreamQuery(p.decoder, options)
	if err != nil {
		return nil, err
	}
	q.plans.columns, q.plans.plan = p.columns, p.plan
	return q, nil
}

func (p *PreparedStream[P, R]) Parameters() Columns {
	if p == nil {
		return Columns{}
	}
	return p.prepared.Parameters()
}
func (p *PreparedStream[P, R]) Columns() Columns {
	if p == nil {
		return Columns{}
	}
	return p.prepared.Columns()
}
func (p *PreparedStream[P, R]) Info() PreparedInfo {
	if p == nil {
		return PreparedInfo{}
	}
	return p.prepared.Info()
}
