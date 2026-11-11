// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	"github.com/scylladb/gocql/v2/internal/model"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// Prepared is an immutable typed statement prepared on a session. Share it
// between goroutines and call Exec, One, Each or All for ordinary executions,
// or Run or RunPage for a handle the caller closes. A Prepared has no Close
// obligation.
//
// Each call ends in options ...exec.CompiledOptions. The variadic is Go's form of
// an optional parameter: omit it for the session defaults, as in
// p.Exec(ctx, v), or pass exactly one value; a second fails the call with a
// validation error. The options are exec.CompiledOptions rather than exec.Options
// because these calls are the hot path: Session.CompileOptions validates,
// copies and resolves the settings once at setup, and each call only passes
// the frozen result.
type Prepared[P, R any] implSession.Prepared[P, R]

func (p *Prepared[P, R]) internal() *implSession.Prepared[P, R] {
	return (*implSession.Prepared[P, R])(p)
}

// Exec executes a statement with no result columns. It is safe for concurrent
// use. At most one exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) Exec(ctx context.Context, value P, options ...exec.CompiledOptions) (Result, error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return tooManyOptionsResult()
	}
	return p.internal().Exec(ctx, value, o)
}

// One decodes the first row into dst, reporting whether a row exists. Further
// rows are ignored; if none exists dst is cleared. Borrowed fields are detached
// before return, while built-in owning fields retain reusable destination
// buffers. The result stays valid after the call. One is safe for concurrent
// use with independent destinations. At most one exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) One(ctx context.Context, value P, dst *R, options ...exec.CompiledOptions) (bool, Result, error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return tooManyOptionsFound()
	}
	return p.internal().One(ctx, value, dst, o)
}

// Each decodes every row across all pages into dst and calls fn. Borrowed
// fields are valid only until fn returns; clone any values kept beyond that
// point. Returning StopIteration stops successfully; other callback errors stop iteration and are returned. Each is safe for concurrent
// use with independent destinations. At most one exec.CompiledOptions may be passed.
// dst is decoded into for every row. With cql.ReuseBuffers, or a RowInto reset
// that keeps slices, []byte fields reuse their capacity, so a slice kept from
// one call of fn aliases the next row's data: keep bytes.Clone(row.Field).
func (p *Prepared[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error, options ...exec.CompiledOptions) (Result, error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return tooManyOptionsResult()
	}
	return p.internal().Each(ctx, value, dst, fn, o)
}

// All iterates every row across all pages, decoding each into dst, as
// Query.All does. Rows stream page by page; they are not loaded into memory.
// The yielded row is valid only until the next iteration. An error is yielded
// once as (nil, err); breaking out of the loop releases the execution. All is
// safe for concurrent use with independent destinations. At most one
// exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) All(ctx context.Context, value P, dst *R, options ...exec.CompiledOptions) iter.Seq2[*R, error] {
	// All stays small enough to inline, so a range loop over it keeps the
	// iterator and what it captures on the stack.
	return func(yield func(*R, error) bool) { preparedAll(p, ctx, value, dst, options, yield) }
}

func preparedAll[P, R any](p *Prepared[P, R], ctx context.Context, value P, dst *R, options []exec.CompiledOptions, yield func(*R, error) bool) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		yield(nil, tooManyOptions())
		return
	}
	p.internal().All(ctx, value, dst, o)(yield)
}

// Run returns a cursor over every row across all pages, as Query.Run does,
// for callers that pull rows or pick each row's destination. The rows hold
// one of the statement's pooled queries until they are closed: close them,
// including on early termination. Session.Close stops rows still open, and
// rows dropped without Close release their query once the garbage collector
// finds them. Run is safe for concurrent use; each Rows is not. At most one
// exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) Run(ctx context.Context, value P, options ...exec.CompiledOptions) (Rows[R], error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return Rows[R]{}, tooManyOptions()
	}
	rows, err := p.internal().Run(ctx, value, o)
	return Rows[R](rows), err
}

// RunPage executes one page, resuming from state, as Query.RunPage does with
// pageSize: zero uses the resolved page size and cql.Unpaged requests the
// whole result. It serves stateless pagination from concurrent handlers: keep
// Page.State, not the page. The page holds one of the statement's pooled
// queries until it is closed, as Run's rows do. RunPage is safe for concurrent
// use; each Page is not. At most one exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) RunPage(ctx context.Context, value P, state cql.PageState, pageSize int32, options ...exec.CompiledOptions) (*Page[R], error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return nil, tooManyOptions()
	}
	page, err := p.internal().RunPage(ctx, value, model.PageState(state), pageSize, o)
	return (*Page[R])(page), err
}

// Query creates a worker-owned reusable query with fixed options. It also
// provides cursors and explicit paging. Close it when the worker is done.
func (p *Prepared[P, R]) Query(options exec.Options) (*Query[P, R], error) {
	q, err := p.internal().Query(internalOptions(options))
	return (*Query[P, R])(q), err
}

// AddToBatch encodes value straight into storage batch owns. Calls on p are
// concurrency-safe and take no pooled query, but appending to one batch
// requires exclusive ownership of it.
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
