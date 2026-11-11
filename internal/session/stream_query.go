// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"sync"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/transport"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type streamPlanCache[R any] struct {
	plan    codec.StreamRowPlan[R]
	columns cqltype.Columns
	mu      sync.Mutex
}

// rememberPlan carries an incremental cursor's checked plan back to its query.
// Buffered cursors update the same cache while selecting their page plan.
func (s *RowStream[R]) rememberPlan(columns cqltype.Columns, plan codec.StreamRowPlan[R]) {
	if cache := s.privateStorage.bufferedPlans; cache != nil {
		cache.mu.Lock()
		cache.columns, cache.plan = columns, plan
		cache.mu.Unlock()
	}
}

// StreamQuery reuses owned parameters and an immutable streaming decoder plan.
// Page buffering is the default; Incremental explicitly bounds receive storage.
type StreamQuery[P, R any] struct {
	plans   streamPlanCache[R]
	decoder codec.StreamRowDecoder[R]
	query   *Query[P, codec.EmptyRecord]
	options StreamOptions
}

// StreamQuery returns a reusable query that runs prepared and streams each row
// into decoder instead of prepared's buffered row decoder.
func (prepared *Prepared[P, B]) StreamQuery[R any](decoder codec.StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	if prepared == nil || prepared.privateStorage.InternalState == nil {
		return nil, cqltype.TypeValidation("prepared", "nil handle")
	}
	if decoder == nil {
		return nil, cqltype.TypeValidation("stream decoder", "nil decoder")
	}
	if _, err := transport.InternalCheckedStreamLimits(options.Limits); err != nil {
		return nil, err
	}
	if v := options.Options; v.Prefetch.Set && v.Prefetch.Value != 0 {
		return nil, cqltype.TypeValidation("stream prefetch", "must be zero")
	}
	p := &Prepared[P, codec.EmptyRecord]{privateStorage: PreparedStorage[P, codec.EmptyRecord]{InternalState: prepared.privateStorage.InternalState, kind: prepared.privateStorage.kind, params: prepared.privateStorage.params, rows: execRowsDecoder{}}}
	q, err := p.Query(options.Options)
	if err != nil {
		return nil, err
	}
	// Streams hand their context to exclusive transport I/O; keep the
	// standard per-execution context there.
	q.state.execCtx = nil
	return &StreamQuery[P, R]{query: q, decoder: decoder, options: options}, nil
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

func (q *StreamQuery[P, R]) Run(ctx context.Context, value P) (*RowStream[R], error) {
	if q == nil {
		return nil, cqltype.TypeValidation("query", "nil query")
	}
	if err := q.query.capture(ctx, value); err != nil {
		return nil, err
	}
	defer q.query.state.owner.InternalRelease()
	return q.query.state.runtime.runStream(ctx, &q.query.bound, q.decoder, q.query.options, q.options.Limits, q.options.Incremental, &q.plans)
}

// Close stops the query permanently and returns at once, as Query.Close
// does. A RowStream that is still open is stopped and reports ErrClosed.
func (q *StreamQuery[P, R]) Close() {
	if q == nil {
		return
	}
	q.query.Close()
}

// Wait waits until the query's last execution has drained, as Query.Wait
// does.
func (q *StreamQuery[P, R]) Wait(ctx context.Context) error {
	if q == nil {
		return cqltype.TypeValidation("query", "nil query")
	}
	return q.query.Wait(ctx)
}

func (s *RowStream[R]) loadBufferedPage() error {
	p := &s.privateStorage
	response, err := InternalRuntimeFor(p.session).executePage(p.ctx, p.request, s.state)
	if err != nil {
		return err
	}
	// Release the preceding page before retaining this one. Validation is shared
	// only for identical immutable column storage; changed metadata is rechecked.
	previous := p.bufferedReader
	if p.bufferedPage != nil {
		p.bufferedPage.Owner.Release()
	}
	p.bufferedPage, p.bufferedIndex = response, 0
	limits := wire.StreamLimits{DiscardBytes: p.limits.DiscardBytes}
	configured := InternalRuntimeFor(p.session).config.Limits
	if configured.MaxMetadataDepth > 0 {
		limits.MaxDepth = min(configured.MaxMetadataDepth, 128) + 2
	}
	if configured.MaxFrameBytes > 0 {
		limits.MaxBytes = configured.MaxFrameBytes
	}
	if configured.MaxDecompressedBytes > 0 && (limits.MaxBytes == 0 || configured.MaxDecompressedBytes < limits.MaxBytes) {
		limits.MaxBytes = configured.MaxDecompressedBytes
	}
	body, err := wire.NewStream(p.ctx, wire.NewCursor(response.Rows), response.Rows.Len(), limits)
	if err != nil {
		return err
	}
	reader, err := codec.NewStreamRowReaderReusingValidation(response.Columns, body, previous)
	if err != nil {
		return err
	}
	p.bufferedBody, p.bufferedReader = body, reader
	cache := p.bufferedPlans
	if cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
	}
	var plan codec.StreamRowPlan[R]
	if cache != nil && cache.plan != nil && cache.columns.Equal(response.Columns) {
		plan = cache.plan
	} else if p.plan != nil && p.columns.Equal(response.Columns) {
		plan = p.plan
	} else {
		plan, err = p.decoder.Compile(response.Columns)
		if err != nil {
			return err
		}
		if plan == nil {
			return cqltype.TypeValidation("stream plan", "decoder returned nil plan")
		}
		if cache != nil {
			cache.columns, cache.plan = response.Columns, plan
		}
	}
	p.plan = plan
	p.mu.Lock()
	p.columns = response.Columns
	prior := p.info.Result
	p.info.Result = response.Result
	p.info.Result.Attempts += prior.Attempts
	p.info.Result.Latency += prior.Latency
	p.info.Result.Rows = p.info.DeliveredRows
	p.info.Pages++
	p.mu.Unlock()
	return nil
}

// InternalActive serializes this with Close and the cancellation callback.
func (s *RowStream[R]) nextBuffered(dst *R) bool {
	p := &s.privateStorage
	fail := func(err error) bool { rowStreamFail(s, err); rowStreamCleanup(s, false); return false }
	for {
		page := p.bufferedPage
		if p.bufferedIndex < page.RowCount {
			codec.BeginStreamRow(p.bufferedReader)
			p.mu.Lock()
			p.info.OutputStarted = true
			p.mu.Unlock()
			p.plan.Reset(dst)
			if err := p.plan.DecodeInto(p.bufferedReader, dst); err != nil {
				return fail(err)
			}
			if err := p.bufferedReader.RequireEnd(); err != nil {
				return fail(err)
			}
			p.bufferedIndex++
			if p.bufferedIndex == page.RowCount {
				if err := p.bufferedBody.RequireEnd(); err != nil {
					return fail(err)
				}
			}
			if err := p.ctx.Err(); err != nil {
				return fail(err)
			}
			p.mu.Lock()
			p.info.DeliveredRows++
			p.info.Result.Rows = p.info.DeliveredRows
			p.mu.Unlock()
			return true
		}
		if err := p.bufferedBody.RequireEnd(); err != nil {
			return fail(err)
		}
		next := page.State
		if !next.More() {
			p.mu.Lock()
			p.terminal = true
			p.mu.Unlock()
			rowStreamCleanup(s, true)
			return false
		}
		if err := p.history.InternalAdd(next); err != nil {
			return fail(err)
		}
		s.state = next
		if err := s.loadBufferedPage(); err != nil {
			return fail(err)
		}
	}
}
