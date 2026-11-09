// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// eachScratch holds the row cursor and reader that a decoder receives by
// pointer. A Query embeds one, so its executions allocate neither.
type eachScratch struct {
	reader RowReader
	cursor wire.Cursor
}

// nilEachCallback reports Each's nil callback. It stays out of line: building
// the error inside Query.Each slows its row loop by about 1.4%.
//
//go:noinline
func nilEachCallback() (Result, error) {
	err := typeValidation("row callback", "nil callback")
	return InternalExecutionErrorResult(err), err
}

// errStopEach ends Each early without reporting an error.
var errStopEach = errors.New("gocql: stop iteration")

// RunEach executes the bound statement and decodes every row, across all pages,
// into dst, calling fn after each row. The execution is finished and all
// response storage released before Each returns, so there is no iterator to
// close or wait for. Values that borrow response storage are valid only until
// fn returns. A non-nil error from fn stops the iteration and is returned.
func RunEach[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options BoundOptions, dst *R, fn func(*R) error) (Result, error) {
	_, result, err := each(ctx, s, b, options, dst, fn, new(eachScratch))
	return result, err
}

// each runs RunEach, or RunOne when fn is nil.
func each[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options BoundOptions, dst *R, fn func(*R) error, scratch *eachScratch) (bool, Result, error) {
	if b == nil {
		return false, Result{}, typeValidation("binding", "nil binding")
	}
	if dst == nil {
		return false, Result{}, typeValidation("destination", "nil row destination")
	}
	runctx, cancel, request, err := InternalAdmitExecution(ctx, s, b, options)
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	found, result, err := eachPage(runctx, s, b, request, dst, fn, scratch)
	*scratch = eachScratch{} // drop response references
	cancel()
	request.InternalLogical.InternalFinish(result, err)
	request.InternalLogical.InternalReleaseInput()
	return found, result, err
}

func eachPage[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], request *InternalExecutionRequest, dst *R, fn func(*R) error, scratch *eachScratch) (bool, Result, error) {
	rt := InternalRuntimeFor(s)
	plans := b.privateStorage.plans
	if plans == nil {
		plans = &InternalResultPlanCache[R]{}
	}
	var result Result
	var history pagingHistory
	var inline [paging.InternalInlineHistory]PageState // the history's first tokens
	state := StartPage()
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
			mergeResult(&result, InternalExecutionErrorResult(err))
			return false, result, err
		}
		if first {
			result = response.Result
		} else {
			mergeResult(&result, response.Result)
		}
		state = response.State
		err = decodeEach(ctx, s, response, b.privateStorage.InternalRows, plans, dst, fn, scratch)
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
			history = pagingHistory{InternalMaxCount: rt.InternalConfig.Limits.MaxPagingStates, InternalMaxBytes: rt.InternalConfig.Limits.MaxPagingStateBytes, InternalInline: &inline}
		}
		if err := history.InternalAdd(state); err != nil {
			return false, result, err
		}
	}
}

// decodeEach decodes one page, calling fn after each row; a nil fn stops after
// the first row with errStopEach. The caller releases the response.
func decodeEach[R any](ctx context.Context, s *implCluster.Core, response *responsePage, decoder RowDecoder[R], plans *InternalResultPlanCache[R], dst *R, fn func(*R) error, scratch *eachScratch) error {
	plan, err := plans.InternalSelectPlan(response, decoder)
	if err != nil {
		return err
	}
	if response.RowCount < 0 {
		return &ProtocolError{Stage: "rows", Err: errors.New("negative row count")}
	}
	preserves := codec.PreservesRowDestination(plan)
	scratch.cursor = wire.CursorOver(response.Rows)
	reader := &scratch.reader
	for i := int32(0); i < response.RowCount; i++ {
		*reader = codec.RowReaderFromCursor(response.Columns, &scratch.cursor, response.Owner)
		if !preserves {
			InternalClearDestination(dst)
		}
		if err := plan.DecodeInto(reader, dst); err != nil {
			return err
		}
		if err := finishRowReader(reader); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.Closed() {
			return ErrClosed
		}
		if fn == nil {
			return errStopEach
		}
		if err := fn(dst); err != nil {
			return err
		}
	}
	if err := scratch.cursor.RequireEnd(); err != nil {
		return &ProtocolError{Stage: "rows", Err: err}
	}
	return nil
}

// RunOne executes the bound statement and decodes its first row into dst,
// reporting whether a row existed. Further rows are ignored, as with
// database/sql's QueryRow. Values that borrow response storage are invalid
// once RunOne returns; use owning codecs for them.
func RunOne[R any](ctx context.Context, s *implCluster.Core, b *Bound[R], options BoundOptions, dst *R) (bool, Result, error) {
	found, result, err := each(ctx, s, b, options, dst, nil, new(eachScratch))
	if !found && err == nil {
		InternalClearDestination(dst)
	}
	return found, result, err
}

// Each executes the query with value and decodes every row, across all
// pages, into dst, calling fn after each row. Values that borrow response
// storage are valid only until fn returns. A non-nil error from fn stops the
// iteration and is returned. The query is ready for its next execution when
// Each returns. The Result reports the coordinator, attempts, latency, row
// count and any warnings, trace IDs or custom payload.
func (q *Query[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error) (Result, error) {
	if fn == nil {
		return nilEachCallback()
	}
	if err := q.capture(ctx, value); err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.owner.InternalRelease()
	_, result, err := each(ctx, q.state.runtime.Core, &q.bound, q.options, dst, fn, &q.scratch)
	return result, err
}

// One executes the query with value and decodes its first row into dst,
// reporting whether a row existed; without one, dst is cleared. Values that
// borrow response storage are invalid once One returns. The query is ready
// for its next execution when One returns.
func (q *Query[P, R]) One(ctx context.Context, value P, dst *R) (bool, Result, error) {
	if err := q.capture(ctx, value); err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	defer q.state.owner.InternalRelease()
	found, result, err := each(ctx, q.state.runtime.Core, &q.bound, q.options, dst, nil, &q.scratch)
	if !found && err == nil {
		InternalClearDestination(dst)
	}
	return found, result, err
}
