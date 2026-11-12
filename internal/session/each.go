// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"iter"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// eachScratch holds the row cursor and reader that a decoder receives by
// pointer. A Query embeds one, so its executions allocate neither.
type eachScratch struct {
	reader codec.RowReader
	cursor wire.Cursor
}

// nilEachCallback reports Each's nil callback. It stays out of line: building
// the error inside Query.Each slows its row loop by about 1.4%.
//
//go:noinline
func nilEachCallback() (model.Result, error) {
	err := cqltype.TypeValidation("row callback", "nil callback")
	return InternalExecutionErrorResult(err), err
}

// errStopEach ends Each early without reporting an error.
var errStopEach = errors.New("gocql: stop iteration")

// StopIteration ends an Each callback successfully. It is recognized only as a
// callback result, before request observers and error metrics are notified.
// Returning a wrapped StopIteration has the same effect; joined errors remain
// failures so that stopping cannot hide another cause.
var StopIteration = errors.New("gocql: stop iteration successfully")

func isStopIteration(err error) bool {
	for err != nil {
		if err == StopIteration {
			return true
		}
		wrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = wrapper.Unwrap()
	}
	return false
}

// each decodes every row into dst, calling fn after each, or stops after the
// first row when fn is nil.
func (rt *InternalSessionRuntime) each[R any](ctx context.Context, b *Bound[R], options CompiledOptions, dst *R, fn func(*R) error, scratch *eachScratch) (bool, model.Result, error) {
	if b == nil {
		return false, model.Result{}, cqltype.TypeValidation("binding", "nil binding")
	}
	if dst == nil {
		return false, model.Result{}, cqltype.TypeValidation("destination", "nil row destination")
	}
	runctx, cancel, request, err := rt.admit(ctx, b, options)
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	found, result, err := rt.eachPage(runctx, b, request, dst, fn, scratch)
	*scratch = eachScratch{} // drop response references
	cancel()
	request.InternalLogical.InternalFinish(result, err)
	request.InternalLogical.releaseInput()
	return found, result, err
}

func (rt *InternalSessionRuntime) eachPage[R any](ctx context.Context, b *Bound[R], request *InternalExecutionRequest, dst *R, fn func(*R) error, scratch *eachScratch) (bool, model.Result, error) {
	plans := b.privateStorage.plans
	if plans == nil {
		plans = &InternalResultPlanCache[R]{}
	}
	var result model.Result
	var history paging.InternalPagingHistory
	var inline [paging.InternalInlineHistory]model.PageState // the history's first tokens
	state := model.StartPage()
	for first := true; ; first = false {
		if !first && request.query != nil {
			// The previous page was released before this request, so the
			// query's response storage is free for every continuation page.
			request.responseDest = &request.query.response
			request.pinned = request.served
		}
		response, err := rt.executePage(ctx, request, state)
		if err != nil {
			if first {
				return false, InternalExecutionErrorResult(err), err
			}
			model.MergeResult(&result, InternalExecutionErrorResult(err))
			return false, result, err
		}
		if first {
			result = response.Result
		} else {
			model.MergeResult(&result, response.Result)
		}
		state = response.State
		err = rt.decodeEach(ctx, response, b.privateStorage.InternalRows, plans, dst, fn, scratch)
		response.Owner.Release()
		if errors.Is(err, errStopEach) {
			return true, result, nil
		}
		if err != nil {
			return false, result, err
		}
		if !state.More() {
			return fn != nil, result, nil
		}
		if first {
			history = paging.InternalPagingHistory{InternalMaxCount: rt.config.Limits.MaxPagingStates, InternalMaxBytes: rt.config.Limits.MaxPagingStateBytes, InternalInline: &inline}
		}
		if err := history.InternalAdd(state); err != nil {
			return false, result, err
		}
	}
}

// decodeEach decodes one page, calling fn after each row; a nil fn stops after
// the first row with errStopEach. The caller releases the response.
func (rt *InternalSessionRuntime) decodeEach[R any](ctx context.Context, response *transport.InternalResponsePage, decoder codec.RowDecoder[R], plans *InternalResultPlanCache[R], dst *R, fn func(*R) error, scratch *eachScratch) error {
	plan, err := plans.InternalSelectPlan(response, decoder)
	if err != nil {
		return err
	}
	if response.RowCount < 0 {
		return &cqltype.ProtocolError{Stage: "rows", Err: errors.New("negative row count")}
	}
	// Built-in plans clear the destination themselves; only an application
	// plan needs it cleared here.
	manages := codec.ManagesRowDestination(plan)
	scratch.cursor = wire.CursorOver(response.Rows)
	reader := &scratch.reader
	for i := int32(0); i < response.RowCount; i++ {
		*reader = codec.RowReaderFromCursor(response.Columns, &scratch.cursor, response.Owner)
		if !manages {
			clearDestination(dst)
		}
		if err := plan.DecodeInto(reader, dst); err != nil {
			return err
		}
		if err := codec.FinishRowReader(reader); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if rt.Closed() {
			return runtimeerrors.ErrClosed
		}
		if fn == nil {
			codec.DetachRow(plan, dst)
			return errStopEach
		}
		if err := fn(dst); err != nil {
			if isStopIteration(err) {
				return errStopEach
			}
			return err
		}
	}
	if err := scratch.cursor.RequireEnd(); err != nil {
		return &cqltype.ProtocolError{Stage: "rows", Err: err}
	}
	return nil
}

// Each executes the query with value and decodes every row, across all
// pages, into dst, calling fn after each row. Values that borrow response
// storage are valid only until fn returns. A non-nil error from fn stops the
// iteration and is returned. The query is ready for its next execution when
// Each returns. The Result reports the coordinator, attempts, latency, row
// count and any warnings, trace IDs or custom payload.
func (q *Query[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error) (model.Result, error) {
	if fn == nil {
		return nilEachCallback()
	}
	if err := q.capture(ctx, value); err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.owner.InternalRelease()
	_, result, err := q.state.runtime.each(ctx, &q.bound, q.options, dst, fn, &q.scratch)
	return result, err
}

// One executes the query with value and decodes its first row into dst,
// reporting whether a row existed; without one, dst is cleared. Borrowed fields
// are detached, so the result stays valid after One returns. The query is ready
// for its next execution when One returns.
func (q *Query[P, R]) One(ctx context.Context, value P, dst *R) (bool, model.Result, error) {
	if err := q.capture(ctx, value); err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	defer q.state.owner.InternalRelease()
	found, result, err := q.state.runtime.each(ctx, &q.bound, q.options, dst, nil, &q.scratch)
	if !found && err == nil {
		clearDestination(dst)
	}
	return found, result, err
}

// All executes the query with value and returns an iterator over every row,
// across all pages, each decoded into dst. Rows stream page by page; they are
// not loaded into memory. The yielded row is dst and stays valid, borrowed
// fields included, until the next iteration. An error is yielded once, as
// (nil, err), and ends the iteration; stopping the loop early releases the
// execution. All runs Each, so it decodes, pages and releases exactly as
// Each does; the query is busy while the loop runs.
func (q *Query[P, R]) All(ctx context.Context, value P, dst *R) iter.Seq2[*R, error] {
	return func(yield func(*R, error) bool) {
		if _, err := q.Each(ctx, value, dst, yieldRow(yield)); err != nil {
			yield(nil, err)
		}
	}
}

// yieldRow adapts a range-over-func yield to an Each callback: the loop
// stopping becomes StopIteration, which ends Each successfully.
func yieldRow[R any](yield func(*R, error) bool) func(*R) error {
	return func(row *R) error {
		if !yield(row, nil) {
			return StopIteration
		}
		return nil
	}
}
