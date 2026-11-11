// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// Prepared is an immutable typed statement prepared on a session. Share it
// between goroutines and call Exec, One or Each for ordinary executions. Each
// call may take one CompiledOptions, built once by Session.CompileOptions, or omit
// options for the session defaults. A Prepared has no Close obligation.
type Prepared[P, R any] implSession.Prepared[P, R]

func (p *Prepared[P, R]) internal() *implSession.Prepared[P, R] {
	return (*implSession.Prepared[P, R])(p)
}

// Exec executes a statement with no result columns. It is safe for concurrent
// use. At most one CompiledOptions may be passed.
func (p *Prepared[P, R]) Exec(ctx context.Context, value P, options ...CompiledOptions) (Result, error) {
	o, err := internalCompiledOptions(options)
	if err != nil {
		return implSession.InternalExecutionErrorResult(err), err
	}
	return p.internal().Exec(ctx, value, o)
}

// One decodes the first row into dst, reporting whether a row exists. Further
// rows are ignored; if none exists dst is cleared. Borrowed fields are detached
// before return, while built-in owning fields retain reusable destination
// buffers. The result stays valid after the call. One is safe for concurrent
// use with independent destinations. At most one CompiledOptions may be passed.
func (p *Prepared[P, R]) One(ctx context.Context, value P, dst *R, options ...CompiledOptions) (bool, Result, error) {
	o, err := internalCompiledOptions(options)
	if err != nil {
		return false, implSession.InternalExecutionErrorResult(err), err
	}
	return p.internal().One(ctx, value, dst, o)
}

// Each decodes every row across all pages into dst and calls fn. Borrowed
// fields are valid only until fn returns; clone any values kept beyond that
// point. Returning StopIteration stops successfully; other callback errors stop iteration and are returned. Each is safe for concurrent
// use with independent destinations. At most one CompiledOptions may be passed.
// dst is decoded into for every row. With cql.ReuseBuffers, or a RowInto reset
// that keeps slices, []byte fields reuse their capacity, so a slice kept from
// one call of fn aliases the next row's data: keep bytes.Clone(row.Field).
func (p *Prepared[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error, options ...CompiledOptions) (Result, error) {
	o, err := internalCompiledOptions(options)
	if err != nil {
		return implSession.InternalExecutionErrorResult(err), err
	}
	return p.internal().Each(ctx, value, dst, fn, o)
}

// Query creates a worker-owned reusable query with fixed options. It also
// provides cursors and explicit paging. Close it when the worker is done.
func (p *Prepared[P, R]) Query(options Options) (*Query[P, R], error) {
	q, err := p.internal().Query(internalOptions(options))
	return (*Query[P, R])(q), err
}

// StreamQuery creates a reusable query decoding into seeded caller storage.
// Page buffering preserves normal connection multiplexing. Incremental receive
// additionally bounds receive scratch and reserves one connection. Close both
// the query and its execution cursor. Capacity failures may leave partial row
// data; only a successful NextInto publishes a complete row.
func (p *Prepared[P, B]) StreamQuery[R any](decoder cql.StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	q, err := p.internal().StreamQuery(decoder, internalStreamOptions(options))
	return (*StreamQuery[P, R])(q), err
}

// AddToBatch copies an encoded parameter snapshot into batch. Calls on p are
// concurrency-safe, but appending to one batch requires exclusive ownership.
func (p *Prepared[P, R]) AddToBatch(batch *Batch, value P) error {
	return p.internal().AddToBatch((*implSession.Batch)(batch), value)
}

// Parameters returns the statement's immutable bind-marker metadata.
func (p *Prepared[P, R]) Parameters() cql.Columns { return p.internal().Parameters() }

// Columns returns the statement's initial immutable result metadata. Actual
// execution metadata can change after schema updates; cursor Columns reports it.
func (p *Prepared[P, R]) Columns() cql.Columns { return p.internal().Columns() }

// Info returns the statement's preparation information.
func (p *Prepared[P, R]) Info() PreparedInfo { return p.internal().Info() }
