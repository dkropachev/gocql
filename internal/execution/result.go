// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"

	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageLease struct {
	privateStorage PageLeaseStorage
}

func (l *PageLease) Close() {
	if l != nil {
		l.privateStorage.once.Do(func() { l.privateStorage.owner.Release() })
	}
}

// Page holds exactly one response page. It is not safe for concurrent use;
// within Rows it is used only under the rows' own lock.
type Page[R any] struct {
	privateStorage PageStorage[R]
}

func InternalNewPage[R any](response *transport.InternalResponsePage, decoder codec.RowDecoder[R], release func()) (*Page[R], error) {
	return newPageWithPlans(response, decoder, nil, release)
}
func newPageWithPlans[R any](response *transport.InternalResponsePage, decoder codec.RowDecoder[R], plans *InternalResultPlanCache[R], release func()) (*Page[R], error) {
	if response == nil {
		return nil, cqltype.TypeValidation("response", "nil response")
	}
	if decoder == nil {
		response.Owner.Release()
		return nil, cqltype.TypeValidation("rows", "nil row decoder")
	}
	var plan codec.RowPlan[R]
	var err error
	if plans == nil {
		plan, err = codec.CompileRowPlan(decoder, response.Columns)
	} else {
		plan, err = plans.InternalSelectPlan(response, decoder)
	}
	if err != nil {
		response.Owner.Release()
		return nil, err
	}
	if response.RowCount < 0 {
		response.Owner.Release()
		return nil, &cqltype.ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	page := new(Page[R])
	page.initResponse(response, plan, release)
	return page, nil
}

// newPageInto is InternalNewPageWithPlans building into caller-owned storage.
func newPageInto[R any](page *Page[R], response *transport.InternalResponsePage, decoder codec.RowDecoder[R], plans *InternalResultPlanCache[R]) error {
	plan, err := plans.InternalSelectPlan(response, decoder)
	if err != nil {
		response.Owner.Release()
		return err
	}
	if response.RowCount < 0 {
		response.Owner.Release()
		return &cqltype.ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	page.initResponse(response, plan, nil)
	return nil
}

func (p *Page[R]) initResponse(response *transport.InternalResponsePage, plan codec.RowPlan[R], release func()) {
	p.privateStorage.response = response
	p.privateStorage.plan = plan
	p.privateStorage.InternalCursor = wire.CursorOver(response.Rows)
	p.privateStorage.release = release
}

// NextInto decodes the next row into dst, reporting whether there was one.
func (p *Page[R]) NextInto(dst *R) bool {
	if p == nil {
		clearDestination(dst)
		return false
	}
	if !p.privateStorage.cursor.current() {
		return false
	}
	defer p.afterAdvance()
	ok := p.advance(dst)
	if !ok && !codec.PreservesRowDestination(p.privateStorage.plan) {
		clearDestination(dst)
	}
	return ok
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
		p.privateStorage.err = runtimeerrors.ErrClosed
		p.closeLocked()
		return false
	}
	if p.privateStorage.ctx != nil && p.privateStorage.ctx.Err() != nil {
		p.privateStorage.err = p.privateStorage.ctx.Err()
		p.closeLocked()
		return false
	}
	if p.privateStorage.index >= p.privateStorage.response.RowCount {
		p.privateStorage.exhausted = true
		p.privateStorage.reader = codec.RowReader{}
		if err := (&p.privateStorage.InternalCursor).RequireEnd(); err != nil {
			p.privateStorage.err = &cqltype.ProtocolError{Stage: "rows", Err: err}
		}
		return false
	}
	p.privateStorage.reader = codec.RowReaderFromCursor(p.privateStorage.response.Columns, &p.privateStorage.InternalCursor, p.privateStorage.response.Owner)
	// The plan receives the actual application destination, never a row temporary.
	if !codec.PreservesRowDestination(p.privateStorage.plan) {
		clearDestination(dst)
	}
	err := p.privateStorage.plan.DecodeInto(&p.privateStorage.reader, dst)
	if err == nil {
		err = codec.FinishRowReader(&p.privateStorage.reader)
	}
	if err == nil && p.privateStorage.ctx != nil {
		err = p.privateStorage.ctx.Err()
	}
	if err == nil && p.privateStorage.session != nil && p.privateStorage.session.Closed() {
		err = runtimeerrors.ErrClosed
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
	if p == nil {
		return nil
	}
	if !p.privateStorage.cursor.current() {
		return p.Err()
	}
	if p.privateStorage.cancel != nil {
		p.privateStorage.cancel()
	}
	defer p.afterAdvance()
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
	p.privateStorage.reader = codec.RowReader{}
	p.privateStorage.response.Owner.Release()
	if p.privateStorage.release != nil {
		p.privateStorage.pendingRelease = p.privateStorage.release
		p.privateStorage.release = nil
	}
}
func (p *Page[R]) Retain() (*PageLease, error) {
	if p == nil {
		return nil, runtimeerrors.ErrNoRows
	}
	if !p.privateStorage.valid || p.privateStorage.closed {
		return nil, runtimeerrors.ErrNoRows
	}
	if !p.privateStorage.response.Owner.Retain() {
		return nil, cqltype.ErrReleased
	}
	return func() *PageLease {
		opaqueStorageValuex := new(PageLease)
		opaqueStorageValuex.privateStorage.owner = p.privateStorage.response.Owner
		return opaqueStorageValuex
	}(), nil
}
func (p *Page[R]) Clone() (R, error) {
	if p == nil {
		var zero R
		return zero, runtimeerrors.ErrNoRows
	}
	if !p.privateStorage.valid || p.privateStorage.closed {
		var zero R
		return zero, runtimeerrors.ErrNoRows
	}
	return p.privateStorage.plan.Clone(*p.privateStorage.destination), nil
}
func (p *Page[R]) State() model.PageState {
	if p == nil {
		return model.CompletePageState()
	}
	return p.privateStorage.response.State
}
func (p *Page[R]) Info() model.Result {
	if p == nil {
		return model.Result{}
	}
	return model.CloneResult(p.privateStorage.response.Result)
}

type prefetchJob struct {
	future *InternalPageFuture
	state  model.PageState
}

// rowsImpl is the storage and logic behind a Rows handle. A reusable query
// embeds one and reinitialises it for each execution; other executions
// allocate their own.
type rowsImpl[R any] struct {
	privateStorage RowsStorage[R]
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
	if !ok && (r.privateStorage.page == nil || !codec.PreservesRowDestination(r.privateStorage.page.privateStorage.plan)) {
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
		if r.privateStorage.session != nil && r.privateStorage.session.Closed() {
			r.privateStorage.err = runtimeerrors.ErrClosed
			r.closeLocked()
			return false
		}
		if err := r.privateStorage.ctx.Err(); err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		if r.privateStorage.page.NextInto(dst) {
			if err := r.privateStorage.ctx.Err(); err != nil {
				r.privateStorage.err = err
				r.closeLocked()
				return false
			}
			if r.privateStorage.session != nil && r.privateStorage.session.Closed() {
				r.privateStorage.err = runtimeerrors.ErrClosed
				r.closeLocked()
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
			model.MergeResult(&r.privateStorage.info, InternalExecutionErrorResult(err))
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		model.MergeResult(&r.privateStorage.info, response.Result)
		r.privateStorage.page, err = newPageWithPlans(response, r.privateStorage.decoder, r.privateStorage.plans, nil)
		if err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		r.startPrefetch()
	}
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
		return nil, runtimeerrors.ErrNoRows
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		return nil, runtimeerrors.ErrNoRows
	}
	return r.privateStorage.page.Retain()
}
func (r *rowsImpl[R]) Clone() (R, error) {
	if r == nil {
		var z R
		return z, runtimeerrors.ErrNoRows
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		var z R
		return z, runtimeerrors.ErrNoRows
	}
	return r.privateStorage.page.Clone()
}
func (r *rowsImpl[R]) Info() model.Result {
	if r == nil {
		return model.Result{}
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	return model.CloneResult(r.privateStorage.info)
}

type InternalResultPlanCache[R any] struct {
	plan  codec.RowPlan[R]
	entry *model.ResultSchemaIdentity
	// current mirrors entry and plan for lock-free hits on identified
	// responses. It is replaced under InternalMu whenever entry changes.
	current  atomic.Pointer[resultPlanSnapshot[R]]
	columns  cqltype.Columns
	revision uint64
	mu       sync.Mutex
}

type resultPlanSnapshot[R any] struct {
	entry *model.ResultSchemaIdentity
	plan  codec.RowPlan[R]
}

func (c *InternalResultPlanCache[R]) InternalSelectPlan(response *transport.InternalResponsePage, decoder codec.RowDecoder[R]) (codec.RowPlan[R], error) {
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
	var plan codec.RowPlan[R]
	var err error
	if previous != nil && columns.Equal(response.Columns) {
		plan = previous
	} else {
		plan, err = codec.CompileRowPlan(decoder, response.Columns)
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
func (c *InternalResultPlanCache[R]) selectUnidentified(response *transport.InternalResponsePage, decoder codec.RowDecoder[R]) (codec.RowPlan[R], error) {
	c.mu.Lock()
	if c.plan != nil && c.columns.Equal(response.Columns) {
		plan := c.plan
		c.mu.Unlock()
		return plan, nil
	}
	c.revision++
	revision := c.revision
	c.mu.Unlock()
	plan, err := codec.CompileRowPlan(decoder, response.Columns)
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
		return nil, ctx.Err()
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
	if r.privateStorage.request.InternalOptions.InternalProfile.Prefetch == 0 || r.privateStorage.page == nil || r.privateStorage.closed {
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
func (p *Page[R]) Columns() cqltype.Columns {
	if p == nil || p.privateStorage.response == nil {
		return cqltype.Columns{}
	}
	return p.privateStorage.response.Columns
}

// Columns returns the current page's actual physical result metadata.
func (r *rowsImpl[R]) Columns() cqltype.Columns {
	if r == nil {
		return cqltype.Columns{}
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if r.privateStorage.page == nil {
		return cqltype.Columns{}
	}
	return r.privateStorage.page.Columns()
}

type execRowsDecoder struct{}
type execRowsPlan struct{ count int }

func (execRowsDecoder) Compile(columns cqltype.Columns) (codec.RowPlan[codec.EmptyRecord], error) {
	return execRowsPlan{count: columns.Len()}, nil
}
func (execRowsDecoder) Clone(value codec.EmptyRecord) codec.EmptyRecord { return value }
func (p execRowsPlan) Decode(reader *codec.RowReader) (codec.EmptyRecord, error) {
	for i := 0; i < p.count; i++ {
		if err := reader.SkipColumn(); err != nil {
			return codec.EmptyRecord{}, err
		}
	}
	return codec.EmptyRecord{}, nil
}
func (execRowsPlan) Clone(value codec.EmptyRecord) codec.EmptyRecord { return value }

func (p execRowsPlan) DecodeInto(reader *codec.RowReader, dst *codec.EmptyRecord) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil row destination")
	}
	*dst = codec.EmptyRecord{}
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
	r.privateStorage.mu.Unlock()
	l.InternalFinish(r.Info(), runtimeerrors.ErrClosed)
	l.releaseInput()
}
