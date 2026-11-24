// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageLease struct {
	privateStorage pageLeaseStorage
}

func (l *PageLease) Close() {
	if l != nil {
		l.privateStorage.once.Do(func() { l.privateStorage.owner.Release() })
	}
}

// Page holds exactly one response page. It is not safe for concurrent use;
// within Rows it is used only under the rows' own lock. A nil or zero Page
// holds no rows, and its state is complete.
type Page[R any] struct {
	privateStorage pageStorage[R]
}

func InternalNewPage[R any](response *transport.InternalResponsePage, decoder cql.RowDecoder[R], release func()) (*Page[R], error) {
	return newPageWithPlans(response, decoder, nil, release)
}
func newPageWithPlans[R any](response *transport.InternalResponsePage, decoder cql.RowDecoder[R], plans *InternalResultPlanCache[R], release func()) (*Page[R], error) {
	if response == nil {
		return nil, cqltype.TypeValidation("response", "nil response")
	}
	if decoder == nil {
		response.Owner.Release()
		return nil, cqltype.TypeValidation("rows", "nil row decoder")
	}
	var plan cql.CompiledRow[R]
	var err error
	if plans == nil {
		plan, err = codec.CompileRow(decoder, response.Columns)
	} else {
		plan, err = plans.InternalSelectPlan(response, decoder)
	}
	if err != nil {
		response.Owner.Release()
		return nil, err
	}
	if response.RowCount < 0 {
		response.Owner.Release()
		return nil, &cql.ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	page := new(Page[R])
	page.initResponse(response, decoder, plan, release)
	return page, nil
}

// newPageInto is InternalNewPageWithPlans building into caller-owned storage.
func newPageInto[R any](page *Page[R], response *transport.InternalResponsePage, decoder cql.RowDecoder[R], plans *InternalResultPlanCache[R]) error {
	plan, err := plans.InternalSelectPlan(response, decoder)
	if err != nil {
		response.Owner.Release()
		return err
	}
	if response.RowCount < 0 {
		response.Owner.Release()
		return &cql.ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	page.initResponse(response, decoder, plan, nil)
	return nil
}

func (p *Page[R]) initResponse(response *transport.InternalResponsePage, decoder cql.RowDecoder[R], plan cql.CompiledRow[R], release func()) {
	p.privateStorage.decoder = decoder
	p.privateStorage.response = response
	p.privateStorage.plan = plan
	p.privateStorage.rowDest = rowDestinationOf(plan)
	p.privateStorage.InternalCursor = wire.CursorOver(response.Rows)
	p.privateStorage.release = release
}

// NextInto decodes the next row into dst, reporting whether there was one.
// All iterates the page's remaining rows as Rows.All does. State remains
// available once the loop has closed the page.
func (p *Page[R]) All(dst *R) iter.Seq2[*R, error] {
	return allRows(dst, p.NextInto, p.Close)
}

func (p *Page[R]) NextInto(dst *R) bool {
	if p == nil || p.privateStorage.response == nil {
		clearDestination(dst)
		return false
	}
	if !p.privateStorage.cursor.current() {
		return false
	}
	defer p.afterAdvance()
	ok := p.advance(dst)
	if !ok && p.privateStorage.rowDest != preservedRowDestination {
		clearDestination(dst)
	}
	return ok
}

// cancelled returns the error of the page's cancelled context, or nil. A
// context Query.Close cancelled reports cqlerr.ErrClosed. It is out of line to
// keep the row loop small.
//
//go:noinline
func (p *Page[R]) cancelled() error {
	if p.privateStorage.ctx == nil {
		return nil
	}
	err := transport.ContextError(p.privateStorage.ctx)
	if err != nil && p.privateStorage.cursor.stopped() {
		err = cqlerr.ErrClosed
	}
	return err
}

func clearDestination[R any](dst *R) {
	if dst != nil {
		var zero R
		*dst = zero
	}
}
func (p *Page[R]) advance(dst *R) bool {
	p.privateStorage.valid = false
	p.privateStorage.destination = nil
	if p.privateStorage.closed || p.privateStorage.exhausted || p.privateStorage.err != nil {
		return false
	}
	if dst == nil {
		p.privateStorage.err = cqltype.TypeValidation("destination", "nil row destination")
		return false
	}
	if p.privateStorage.session != nil && p.privateStorage.session.Closed() {
		p.privateStorage.err = cqlerr.ErrClosed
		p.closeLocked()
		return false
	}
	if err := p.cancelled(); err != nil {
		p.privateStorage.err = err
		p.closeLocked()
		return false
	}
	if p.privateStorage.index >= p.privateStorage.response.RowCount {
		p.privateStorage.exhausted = true
		p.privateStorage.reader = cql.RowReader{}
		if err := p.privateStorage.InternalCursor.RequireEnd(); err != nil {
			p.privateStorage.err = &cql.ProtocolError{Stage: "rows", Err: err}
		}
		return false
	}
	p.privateStorage.reader = codec.RowReaderFromCursor(p.privateStorage.response.Columns, &p.privateStorage.InternalCursor, p.privateStorage.response.Owner)
	// The plan receives the actual application destination, never a row
	// temporary. A built-in plan clears it itself.
	if p.privateStorage.rowDest == clearedRowDestination {
		clearDestination(dst)
	}
	err := p.privateStorage.plan.DecodeInto(&p.privateStorage.reader, dst)
	if err == nil {
		err = codec.FinishRowReader(&p.privateStorage.reader)
	}
	if err == nil {
		err = p.cancelled()
	}
	if err == nil && p.privateStorage.session != nil && p.privateStorage.session.Closed() {
		err = cqlerr.ErrClosed
	}
	if err != nil {
		p.privateStorage.err = err
		return false
	}
	p.privateStorage.index++
	p.privateStorage.valid = true
	p.privateStorage.destination = dst
	return true
}
func (p *Page[R]) Err() error {
	if p == nil {
		return nil
	}
	return p.privateStorage.err
}
func (p *Page[R]) Close() error {
	if p == nil || p.privateStorage.response == nil {
		return nil
	}
	if !p.privateStorage.cursor.current() {
		return p.Err()
	}
	if p.privateStorage.cancel != nil {
		p.privateStorage.cancel()
	}
	defer p.afterAdvance()
	if !p.privateStorage.closed && p.privateStorage.err == nil && p.privateStorage.cursor.stopped() {
		p.privateStorage.err = cqlerr.ErrClosed
	}
	p.closeLocked()
	return p.privateStorage.err
}
func (p *Page[R]) closeLocked() {
	if p.privateStorage.closed {
		return
	}
	p.privateStorage.closed = true
	p.privateStorage.valid = false
	p.privateStorage.destination = nil
	p.privateStorage.reader = cql.RowReader{}
	p.privateStorage.response.Owner.Release()
	if p.privateStorage.release != nil {
		p.privateStorage.pendingRelease = p.privateStorage.release
		p.privateStorage.release = nil
	}
}
func (p *Page[R]) Retain() (*PageLease, error) {
	if p == nil || p.privateStorage.response == nil {
		return nil, cqlerr.ErrNoCurrentRow
	}
	if !p.privateStorage.valid || p.privateStorage.closed {
		return nil, cqlerr.ErrNoCurrentRow
	}
	if !p.privateStorage.response.Owner.Retain() {
		return nil, cql.ErrReleased
	}
	return func() *PageLease {
		opaqueStorageValuex := new(PageLease)
		opaqueStorageValuex.privateStorage.owner = p.privateStorage.response.Owner
		return opaqueStorageValuex
	}(), nil
}
func (p *Page[R]) Clone() (R, error) {
	if p == nil || p.privateStorage.response == nil {
		var zero R
		return zero, cqlerr.ErrNoCurrentRow
	}
	if !p.privateStorage.valid || p.privateStorage.closed {
		var zero R
		return zero, cqlerr.ErrNoCurrentRow
	}
	return p.privateStorage.decoder.Clone(*p.privateStorage.destination), nil
}
func (p *Page[R]) State() cql.PageState {
	if p == nil || p.privateStorage.response == nil {
		return cqltype.CompletePageState()
	}
	return p.privateStorage.response.State
}
func (p *Page[R]) Info() cql.Result {
	if p == nil || p.privateStorage.response == nil {
		return cql.Result{}
	}
	return codec.CloneResult(p.privateStorage.response.Result)
}

type prefetchJob struct {
	future *InternalPageFuture
	state  cql.PageState
}

// rowsImpl is the storage and logic behind a Rows handle. A reusable query
// embeds one and reinitialises it for each execution; other executions
// allocate their own.
type rowsImpl[R any] struct {
	privateStorage rowsStorage[R]
	gen            atomic.Uint64 // the generation handles must carry to use it
}

func (r *rowsImpl[R]) NextInto(dst *R) bool {
	if r == nil {
		clearDestination(dst)
		return false
	}
	if !r.privateStorage.cursor.current() {
		return false
	}
	r.privateStorage.mu.Lock()
	defer r.unlockAfterAdvance()
	ok := r.advance(dst)
	if !ok && (r.privateStorage.page == nil || r.privateStorage.page.privateStorage.rowDest != preservedRowDestination) {
		clearDestination(dst)
	}
	return ok
}
func (r *rowsImpl[R]) advance(dst *R) bool {
	if r == nil || r.privateStorage.closed || r.privateStorage.err != nil {
		return false
	}
	r.privateStorage.valid = false
	if dst == nil {
		r.privateStorage.err = cqltype.TypeValidation("destination", "nil row destination")
		r.closeLocked()
		return false
	}
	for {
		if r.privateStorage.rejectColumns && r.privateStorage.page != nil && r.privateStorage.page.Columns().Len() != 0 && r.privateStorage.terminalErr == nil {
			r.privateStorage.terminalErr = cqltype.TypeValidation("Exec", "result columns require Run")
		}
		if r.privateStorage.page.NextInto(dst) {
			// Session.Close cancels the context of every open Rows, so
			// one check after decoding catches both a cancelled caller and
			// a closed session.
			if err := transport.ContextError(r.privateStorage.ctx); err != nil {
				r.stop(err)
				return false
			}
			r.privateStorage.valid = true
			return true
		}
		if err := r.privateStorage.page.Err(); err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		if err := transport.ContextError(r.privateStorage.ctx); err != nil {
			r.stop(err)
			return false
		}
		state := r.privateStorage.page.State()
		_ = r.privateStorage.page.Close()
		if !state.More() {
			if r.privateStorage.err == nil {
				r.privateStorage.err = r.privateStorage.terminalErr
			}
			r.closeLocked()
			return false
		}
		if r.privateStorage.nextErr != nil {
			r.privateStorage.err = r.privateStorage.nextErr
			r.closeLocked()
			return false
		}
		if r.privateStorage.future == nil {
			if err := r.privateStorage.history.InternalAdd(state); err != nil {
				r.privateStorage.err = err
				r.closeLocked()
				return false
			}
		}
		var response *transport.InternalResponsePage
		var err error
		if r.privateStorage.future != nil {
			response, err = r.privateStorage.future.InternalTake(r.privateStorage.ctx)
			r.privateStorage.future = nil
		} else {
			response, err = InternalRuntimeFor(r.privateStorage.session).executePage(r.privateStorage.ctx, r.privateStorage.request, state)
		}
		if err != nil {
			codec.MergeResult(&r.privateStorage.info, InternalExecutionErrorResult(err))
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		codec.MergeResult(&r.privateStorage.info, response.Result)
		r.privateStorage.page, err = newPageWithPlans(response, r.privateStorage.decoder, r.privateStorage.plans, nil)
		if err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		r.startPrefetch()
	}
}

// stop ends the rows on a cancelled context, reporting ErrClosed when the
// cancellation came from Session.Close. It is out of line to keep the row
// loop small.
//
//go:noinline
func (r *rowsImpl[R]) stop(err error) {
	if r.privateStorage.session != nil && r.privateStorage.session.Closed() {
		err = cqlerr.ErrClosed
	}
	r.privateStorage.err = err
	r.closeLocked()
}
func (r *rowsImpl[R]) Err() error {
	if r == nil {
		return nil
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	return r.privateStorage.err
}
func (r *rowsImpl[R]) Close() error {
	if r == nil {
		return nil
	}
	if !r.privateStorage.cursor.current() {
		return r.Err()
	}
	if r.privateStorage.cancel != nil {
		r.privateStorage.cancel()
	}
	r.privateStorage.mu.Lock()
	defer r.unlockAfterAdvance()
	r.closeLocked()
	return r.privateStorage.err
}
func (r *rowsImpl[R]) closeLocked() {
	if r.privateStorage.closed {
		return
	}
	r.privateStorage.closed = true
	r.privateStorage.valid = false
	if r.privateStorage.page != nil {
		if err := r.privateStorage.page.Close(); r.privateStorage.err == nil {
			r.privateStorage.err = err
		}
	}
	if r.privateStorage.cancel != nil {
		r.privateStorage.cancel()
	}
	if r.privateStorage.future != nil {
		r.privateStorage.future.InternalClose()
		r.privateStorage.future = nil
	}
	r.privateStorage.history.InternalClear()
	if r.privateStorage.release != nil {
		r.privateStorage.pendingRelease = r.privateStorage.release
		r.privateStorage.release = nil
	}
	if r.privateStorage.execution != nil {
		r.privateStorage.pendingExecution = r.privateStorage.execution
		r.privateStorage.execution = nil
	}
}
func (r *rowsImpl[R]) Retain() (*PageLease, error) {
	if r == nil {
		return nil, cqlerr.ErrNoCurrentRow
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		return nil, cqlerr.ErrNoCurrentRow
	}
	return r.privateStorage.page.Retain()
}
func (r *rowsImpl[R]) Clone() (R, error) {
	if r == nil {
		var z R
		return z, cqlerr.ErrNoCurrentRow
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		var z R
		return z, cqlerr.ErrNoCurrentRow
	}
	return r.privateStorage.page.Clone()
}
func (r *rowsImpl[R]) Info() cql.Result {
	if r == nil {
		return cql.Result{}
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	return codec.CloneResult(r.privateStorage.info)
}

type InternalResultPlanCache[R any] struct {
	plan  cql.CompiledRow[R]
	entry *model.ResultSchemaIdentity
	// current mirrors entry and plan for lock-free hits on identified
	// responses. It is replaced under InternalMu whenever entry changes.
	current  atomic.Pointer[resultPlanSnapshot[R]]
	columns  cql.Columns
	revision uint64
	mu       sync.Mutex
}

type resultPlanSnapshot[R any] struct {
	entry *model.ResultSchemaIdentity
	plan  cql.CompiledRow[R]
}

func (c *InternalResultPlanCache[R]) InternalSelectPlan(response *transport.InternalResponsePage, decoder cql.RowDecoder[R]) (cql.CompiledRow[R], error) {
	entry := response.InternalMetadata
	if entry == nil {
		return c.selectUnidentified(response, decoder)
	}
	if current := c.current.Load(); current != nil && current.entry == entry {
		return current.plan, nil
	}
	c.mu.Lock()
	if c.plan != nil && c.entry == entry {
		plan := c.plan
		c.mu.Unlock()
		return plan, nil
	}
	previous, columns := c.plan, c.columns
	c.revision++
	revision := c.revision
	c.mu.Unlock()
	var plan cql.CompiledRow[R]
	var err error
	if previous != nil && columns.Equal(response.Columns) {
		plan = previous
	} else {
		plan, err = codec.CompileRow(decoder, response.Columns)
	}
	if err != nil {
		return nil, err
	}
	if entry.InternalTracker != nil {
		entry.InternalTracker.InternalMu.RLock()
		defer entry.InternalTracker.InternalMu.RUnlock()
	}
	c.mu.Lock()
	if c.revision == revision && entry.InternalEpoch.InternalCurrent() {
		c.entry = entry
		c.columns = response.Columns
		c.plan = plan
		c.current.Store(&resultPlanSnapshot[R]{entry: entry, plan: plan})
	}
	c.mu.Unlock()
	return plan, nil
}

// selectUnidentified serves responses without a schema identity, such as
// unprepared queries. Their columns are compared directly instead of minting
// a throwaway identity for every response.
func (c *InternalResultPlanCache[R]) selectUnidentified(response *transport.InternalResponsePage, decoder cql.RowDecoder[R]) (cql.CompiledRow[R], error) {
	c.mu.Lock()
	if c.plan != nil && c.columns.Equal(response.Columns) {
		plan := c.plan
		c.mu.Unlock()
		return plan, nil
	}
	c.revision++
	revision := c.revision
	c.mu.Unlock()
	plan, err := codec.CompileRow(decoder, response.Columns)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.revision == revision {
		c.entry = nil
		c.columns = response.Columns
		c.plan = plan
		c.current.Store(nil)
	}
	c.mu.Unlock()
	return plan, nil
}

// pageFuture transfers one prefetched response without making Close wait for I/O.
// A closed future releases a late response before retiring its input admission.
type InternalPageFuture struct {
	err          error
	InternalDone chan struct{}
	response     *transport.InternalResponsePage
	mu           sync.Mutex
	closed       bool
}

func (f *InternalPageFuture) InternalComplete(response *transport.InternalResponsePage, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		if response != nil {
			response.Owner.Release()
		}
	} else {
		f.response = response
		f.err = err
	}
	close(f.InternalDone)
}
func (f *InternalPageFuture) InternalTake(ctx context.Context) (*transport.InternalResponsePage, error) {
	select {
	case <-ctx.Done():
		f.InternalClose()
		return nil, transport.ContextError(ctx)
	case <-f.InternalDone:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	response := f.response
	f.response = nil
	return response, f.err
}
func (f *InternalPageFuture) InternalClose() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	if f.response != nil {
		f.response.Owner.Release()
		f.response = nil
	}
}
func (r *rowsImpl[R]) startPrefetch() {
	if r.privateStorage.request.options.InternalProfile.Prefetch == 0 || r.privateStorage.page == nil || r.privateStorage.closed {
		return
	}
	state := r.privateStorage.page.State()
	if !state.More() {
		return
	}
	if err := r.privateStorage.history.InternalAdd(state); err != nil {
		r.privateStorage.nextErr = err
		return
	}
	future := &InternalPageFuture{InternalDone: make(chan struct{})}
	r.privateStorage.future = future
	if r.privateStorage.prefetchJobs == nil {
		perfAdd(4, 1)
		jobs := make(chan prefetchJob, 1)
		r.privateStorage.prefetchJobs = jobs
		r.privateStorage.request.InternalOwner.retain()
		go func() {
			defer r.privateStorage.request.InternalOwner.InternalRelease()
			for {
				select {
				case <-r.privateStorage.ctx.Done():
					return
				case job := <-jobs:
					response, err := InternalRuntimeFor(r.privateStorage.session).executePage(r.privateStorage.ctx, r.privateStorage.request, job.state)
					job.future.InternalComplete(response, err)
				}
			}
		}()
	}
	r.privateStorage.prefetchJobs <- prefetchJob{state: state, future: future}
}

// Columns returns the actual physical result metadata, including for empty pages.
func (p *Page[R]) Columns() cql.Columns {
	if p == nil || p.privateStorage.response == nil {
		return cql.Columns{}
	}
	return p.privateStorage.response.Columns
}

// Columns returns the current page's actual physical result metadata.
func (r *rowsImpl[R]) Columns() cql.Columns {
	if r == nil {
		return cql.Columns{}
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if r.privateStorage.page == nil {
		return cql.Columns{}
	}
	return r.privateStorage.page.Columns()
}

type execRowsDecoder struct{}
type execRowsPlan struct {
	codec.ManagedRowDestination
	count int
}

func (execRowsDecoder) Compile(columns cql.Columns) (cql.CompiledRow[cql.EmptyRow], error) {
	return execRowsPlan{count: columns.Len()}, nil
}
func (execRowsDecoder) Clone(value cql.EmptyRow) cql.EmptyRow { return value }
func (p execRowsPlan) Decode(reader *cql.RowReader) (cql.EmptyRow, error) {
	for i := 0; i < p.count; i++ {
		if err := reader.SkipColumn(); err != nil {
			return cql.EmptyRow{}, err
		}
	}
	return cql.EmptyRow{}, nil
}
func (execRowsPlan) Clone(value cql.EmptyRow) cql.EmptyRow { return value }

func (p execRowsPlan) DecodeInto(reader *cql.RowReader, dst *cql.EmptyRow) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil row destination")
	}
	*dst = cql.EmptyRow{}
	for i := 0; i < p.count; i++ {
		if err := reader.SkipColumn(); err != nil {
			return err
		}
	}
	return nil
}

// Completion observers may inspect their page; invoke them once its state is
// final.
func (p *Page[R]) afterAdvance() {
	release := p.privateStorage.pendingRelease
	p.privateStorage.pendingRelease = nil
	if release != nil {
		release()
	}
}
func (r *rowsImpl[R]) unlockAfterAdvance() {
	release := r.privateStorage.pendingRelease
	r.privateStorage.pendingRelease = nil
	execution := r.privateStorage.pendingExecution
	r.privateStorage.pendingExecution = nil
	info, err := r.privateStorage.info, r.privateStorage.err
	r.privateStorage.mu.Unlock()
	if release != nil {
		release()
	}
	if execution != nil {
		execution.InternalFinish(info, err)
		execution.releaseInput()
	}
}

// stopExecution implements executionStopper for query and session shutdown.
func (r *rowsImpl[R]) stopExecution(l *logicalExecution) {
	r.privateStorage.mu.Lock()
	if r.privateStorage.future != nil {
		r.privateStorage.future.InternalClose()
		r.privateStorage.future = nil
	}
	// Open rows the stop reached between calls report ErrClosed; a NextInto
	// running at that moment already ended on the cancellation.
	if !r.privateStorage.closed && r.privateStorage.err == nil {
		r.privateStorage.err = cqlerr.ErrClosed
	}
	r.privateStorage.mu.Unlock()
	l.InternalFinish(r.Info(), cqlerr.ErrClosed)
	l.releaseInput()
}

type pageLeaseStorage struct {
	owner *wire.Lease
	once  sync.Once
}

type pageStorage[R any] struct {
	ctx            context.Context
	plan           cql.CompiledRow[R]
	decoder        cql.RowDecoder[R] // compiled plan; detaches its rows
	err            error
	pendingRelease func()
	destination    *R
	cancel         context.CancelFunc
	session        *implCluster.Core
	response       *transport.InternalResponsePage
	release        func()
	cursor         queryCursor
	reader         cql.RowReader
	InternalCursor wire.Cursor
	index          int32
	valid          bool
	closed         bool
	exhausted      bool
	// rowDest caches how plan treats the row destination.
	rowDest rowDestination
}

// rowDestination is how a page's plan treats the row destination, decided
// once per page.
type rowDestination uint8

const (
	// clearedRowDestination: the page clears the destination before each row.
	clearedRowDestination rowDestination = iota
	// managedRowDestination: the plan clears it itself.
	managedRowDestination
	// preservedRowDestination: the plan resets it itself, and the page keeps
	// it when iteration ends (codec.PreservesRowDestination).
	preservedRowDestination
)

func rowDestinationOf[R any](plan cql.CompiledRow[R]) rowDestination {
	switch {
	case codec.PreservesRowDestination(plan):
		return preservedRowDestination
	case codec.ManagesRowDestination(plan):
		return managedRowDestination
	}
	return clearedRowDestination
}

type rowsStorage[R any] struct {
	nextErr        error
	terminalErr    error
	ctx            context.Context
	decoder        cql.RowDecoder[R]
	err            error
	prefetchJobs   chan prefetchJob
	pendingRelease func()
	plans          *InternalResultPlanCache[R]
	future         *InternalPageFuture
	cancel         context.CancelFunc
	session        *implCluster.Core
	request        *InternalExecutionRequest
	page           *Page[R]
	release        func()
	// execution is finished and released when the rows close; pendingExecution
	// carries that release past the unlock, like pendingRelease.
	execution, pendingExecution *logicalExecution
	cursor                      queryCursor
	history                     paging.InternalPagingHistory
	info                        cql.Result
	mu                          sync.Mutex
	rejectColumns               bool
	closed                      bool
	valid                       bool
}
