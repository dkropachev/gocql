// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/routing"
)

// Prepared is an immutable typed statement prepared on a session. Share it
// between goroutines and call Exec, One, Each or All for ordinary executions,
// or Run or RunPage for a handle the caller closes. A Prepared has no Close
// obligation.
//
// Each call ends in options ...exec.CompiledOptions. The variadic is Go's
// form of an optional parameter: omit it for the session defaults, as in
// p.Exec(ctx, v), or for the defaults of a view made by With, or pass
// exactly one value; a second fails the call with a validation error. The
// options are exec.CompiledOptions rather than exec.Options because these
// calls are the hot path: Session.CompileOptions validates, copies and
// resolves the settings once at setup, and each call only passes the frozen
// result.
//
// Session.Prepare makes a Prepared. A zero Prepared has no columns, and its
// methods that execute, batch or make a Worker return a cql.ValidationError.
type Prepared[P, R any] implSession.Prepared[P, R]

func (p *Prepared[P, R]) internal() *implSession.Prepared[P, R] {
	return (*implSession.Prepared[P, R])(p)
}

// Exec executes a statement with no result columns. It is safe for concurrent
// use. At most one exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) Exec(ctx context.Context, value P, options ...exec.CompiledOptions) (cql.Result, error) {
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
// buffers. The result stays valid after the call. With a nil dst, One only
// reports whether a row exists. One is safe for concurrent use with
// independent destinations. At most one exec.CompiledOptions may be passed.
func (p *Prepared[P, R]) One(ctx context.Context, value P, dst *R, options ...exec.CompiledOptions) (bool, cql.Result, error) {
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
// dst is decoded into for every row and passed to fn, the same pointer each
// time: copy a row to keep it. A nil dst decodes each row into a new R, which
// fn may keep. With cql.ReuseBuffers, or a RowInto reset that keeps slices,
// []byte fields reuse their capacity, so a slice kept from one call of fn
// aliases the next row's data: keep bytes.Clone(row.Field).
func (p *Prepared[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error, options ...exec.CompiledOptions) (cql.Result, error) {
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
// Worker.All does. Rows stream page by page; they are not loaded into memory.
// The yielded row is dst, reused for every row, or a new R for each row when
// dst is nil; its borrowed fields are valid only until the next iteration. An
// error is yielded
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

// Run returns a cursor over every row across all pages, as Worker.Run does,
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

// RunPage executes one page, resuming from state, as Worker.RunPage does with
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
	page, err := p.internal().RunPage(ctx, value, cql.PageState(state), pageSize, o)
	return (*Page[R])(page), err
}

// Worker creates a reusable execution of the statement, owned by one
// goroutine, with fixed options. It also provides cursors and explicit
// paging. Close it when the goroutine is done with it. At most one
// exec.Options may be passed; omitting it uses the defaults of a view made by
// With, or else the session defaults. Options with a Timestamp, passed or
// taken from the view's defaults, are rejected with a cql.ValidationError,
// since every write of the worker would carry it.
func (p *Prepared[P, R]) Worker(options ...exec.Options) (*Worker[P, R], error) {
	var o exec.Options
	switch len(options) {
	case 0:
		if d := p.internal().Defaults(); d != (implSession.CompiledOptions{}) {
			o = exec.CompiledOptions(d).Values()
		}
	case 1:
		o = options[0]
	default:
		return nil, tooManyWorkerOptions(false)
	}
	if o.Timestamp.Set {
		return nil, fixedWorkerTimestamp()
	}
	q, err := p.internal().Query(internalOptions(o))
	return (*Worker[P, R])(q), err
}

// Routing reports where an execution of the statement with value and
// options would be routed, without executing it: the keyspace, table, token
// and statement kind, and the consistency, host and shard the options
// resolve to. Pass the token to metadata.Snapshot.Replicas for the candidate
// replicas; the routing policy picks among them when the statement runs, so
// Routing does not say which replica serves it. Routing takes no pooled
// query and is safe for concurrent use. At most one exec.CompiledOptions may
// be passed; a closed session returns cqlerr.ErrClosed.
func (p *Prepared[P, R]) Routing(value P, options ...exec.CompiledOptions) (routing.Info, error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return routing.Info{}, tooManyOptions()
	}
	return p.internal().Routing(value, o)
}

// With returns a view of p whose calls passed no options use defaults
// instead of the session defaults: Exec, One, Each, All, Run, RunPage and
// Routing, and Worker, which takes the values defaults were compiled from. A
// zero exec.CompiledOptions passed to a call counts as none. Options passed
// to a call replace defaults whole, as they replace the session defaults,
// rather than adding to them field by field.
//
// The view shares p's preparation and its pool of queries: making one costs
// one small allocation, and neither the view nor p needs closing. With on a
// view replaces its defaults. Keep a view per set of defaults, as a
// statement read at QUORUM in one place and at ONE in another, rather than
// passing the options on every call.
func (p *Prepared[P, R]) With(defaults exec.CompiledOptions) *Prepared[P, R] {
	return (*Prepared[P, R])(p.internal().With(implSession.CompiledOptions(defaults)))
}

// AddToBatch encodes value straight into storage batch owns. Calls on p are
// concurrency-safe and take no pooled query, but appending to one batch
// requires exclusive ownership of it. A batch that holds members of another
// session, or already holds MaxBatchStatements, is rejected with a
// *cql.ValidationError and left unchanged.
func (p *Prepared[P, R]) AddToBatch(batch *Batch, value P) error {
	return p.internal().AddToBatch((*implSession.Batch)(batch), value)
}

// BindMarkers returns the statement's immutable bind-marker metadata, one
// column per marker, as the server reported it when preparing.
func (p *Prepared[P, R]) BindMarkers() cql.Columns { return p.internal().BindMarkers() }

// Columns returns the statement's initial immutable result metadata. Actual
// execution metadata can change after schema updates; cursor Columns reports it.
func (p *Prepared[P, R]) Columns() cql.Columns { return p.internal().Columns() }

// Info returns the statement's preparation information.
func (p *Prepared[P, R]) Info() PreparedInfo { return PreparedInfo(p.internal().Info()) }
