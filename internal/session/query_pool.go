// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"iter"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"weak"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// queryPoolBase is what the session runtime sees of a query pool: its idle
// queries, so that closing the session stops them. The runtime refers to it
// weakly, so a pool nobody uses is collected with its queries. A query that
// is running is not tracked here: the session runtime counts busy pooled
// queries, and Close waits for that count to reach zero.
type queryPoolBase struct {
	// takeIdle empties the pool's slots and returns the queries they held.
	takeIdle func() []*queryState
	closed   atomic.Bool
}

// close stops the pool from taking or creating queries and returns its idle
// queries, for the caller to stop. Queries running at the time close
// themselves when they become idle.
func (b *queryPoolBase) close() []*queryState {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	return b.takeIdle()
}

// queryPoolEntry is the session runtime's weak reference to a pool.
type queryPoolEntry struct {
	ref weak.Pointer[queryPoolBase]
}

// registerPool makes the session's close stop the queries of base. The entry
// goes away when base is collected.
func (rt *InternalSessionRuntime) registerPool(s *implCluster.Core, base *queryPoolBase) error {
	data := implCluster.SessionData(s)
	data.InternalSessionStateMu.RLock()
	defer data.InternalSessionStateMu.RUnlock()
	if data.InternalIsClosing || data.InternalIsClosed {
		return runtimeerrors.ErrClosed
	}
	entry := &queryPoolEntry{ref: weak.Make(base)}
	rt.InternalMu.Lock()
	if rt.pools == nil {
		rt.pools = make(map[*queryPoolEntry]struct{})
	}
	rt.pools[entry] = struct{}{}
	rt.InternalMu.Unlock()
	runtime.AddCleanup(base, func(entry *queryPoolEntry) {
		rt.InternalMu.Lock()
		delete(rt.pools, entry)
		rt.InternalMu.Unlock()
	}, entry)
	return nil
}

// livePools returns the session's pools that are still reachable. The caller
// holds rt.InternalMu.
func (rt *InternalSessionRuntime) livePools() []*queryPoolBase {
	pools := make([]*queryPoolBase, 0, len(rt.pools))
	for entry := range rt.pools {
		if base := entry.ref.Value(); base != nil {
			pools = append(pools, base)
		}
	}
	return pools
}

// queryPools reports how many query pools the session tracks.
func queryPools(s *implCluster.Core) int {
	rt := InternalRuntimeFor(s)
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	return len(rt.pools)
}

// queryPool keeps idle queries of one statement for synchronous one-shot
// calls, each with its own options. A query returns to the pool when it
// becomes idle, once its execution drained, not when the call returns.
//
// Idle queries sit in a fixed array of slots, taken with Swap and filled with
// CompareAndSwap from a random start, so callers on different processors
// rarely touch the same slot and no lock is taken. The slot count caps the
// idle queries: one that finds every slot full is closed, so a burst leaves
// at most that many behind.
type queryPool[P, R any] struct {
	base     *queryPoolBase
	newQuery func(*queryPoolBase) (*Query[P, R], error)
	slots    []atomic.Pointer[Query[P, R]]
}

// idleQueryCap is the number of idle queries a pool keeps.
func idleQueryCap() int {
	return max(16, 4*runtime.GOMAXPROCS(0))
}

func newQueryPool[P, R any](s *implCluster.Core, newQuery func(*queryPoolBase) (*Query[P, R], error)) (*queryPool[P, R], error) {
	p := &queryPool[P, R]{base: new(queryPoolBase), slots: make([]atomic.Pointer[Query[P, R]], idleQueryCap()), newQuery: newQuery}
	p.base.takeIdle = p.takeIdle
	if err := InternalRuntimeFor(s).registerPool(s, p.base); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *queryPool[P, R]) get() (*Query[P, R], error) {
	if p.base.closed.Load() {
		return nil, runtimeerrors.ErrClosed
	}
	n := len(p.slots)
	for i, start := 0, int(rand.Uint32N(uint32(n))); i < n; i++ {
		slot := &p.slots[(start+i)%n]
		if slot.Load() != nil {
			if q := slot.Swap(nil); q != nil {
				return q, nil
			}
		}
	}
	q, err := p.newQuery(p.base)
	if err != nil {
		return nil, err
	}
	q.state.perCall = true
	// The owner's last release makes the query idle and hands it back. A
	// query reaches idle only from an execution, and one closed during it
	// was closed with the pool, which put ignores.
	q.state.owner.onIdle = func() {
		if q.state.held {
			// A handle held this execution; it no longer needs tracking.
			q.state.held = false
			q.state.runtime.unregisterQuery(&q.state)
		}
		q.state.idle()
		p.put(q)
	}
	return q, nil
}

// put keeps q, which just became idle, or closes it when every slot is full.
func (p *queryPool[P, R]) put(q *Query[P, R]) {
	if !p.base.closed.Load() {
		n := len(p.slots)
		for i, start := 0, int(rand.Uint32N(uint32(n))); i < n; i++ {
			slot := &p.slots[(start+i)%n]
			if slot.Load() == nil && slot.CompareAndSwap(nil, q) {
				// close may have emptied the slots between the check above
				// and this store; take the query back out if so.
				if p.base.closed.Load() && slot.CompareAndSwap(q, nil) {
					break
				}
				return
			}
		}
	}
	q.state.close()
}

// takeIdle empties the slots and returns the queries they held.
func (p *queryPool[P, R]) takeIdle() []*queryState {
	var idle []*queryState
	for i := range p.slots {
		if q := p.slots[i].Swap(nil); q != nil {
			idle = append(idle, &q.state)
		}
	}
	return idle
}

// capture takes an idle query and captures value into it. On success the
// caller owns the query until it releases the query's owner, after which the
// query may already serve another call.
func (p *queryPool[P, R]) capture(ctx context.Context, value P) (*Query[P, R], error) {
	if ctx == nil {
		return nil, cqltype.TypeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q, err := p.get()
	if err != nil {
		return nil, err
	}
	if err := q.capture(ctx, value); err != nil {
		// A failure after the query turned busy ended its execution, and
		// the query returned through idle. Before that, capture fails only
		// for a closed query, which is dropped, or for the context, which
		// can end between the check above and the capture; that query is
		// still idle and goes back.
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			p.put(q)
		}
		return nil, err
	}
	return q, nil
}

// begin takes an idle query and begins an execution on it without capturing
// parameters. On success the caller owns the query until it releases the
// query's owner.
func (p *queryPool[P, R]) begin(ctx context.Context) (*Query[P, R], error) {
	if ctx == nil {
		return nil, cqltype.TypeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q, err := p.get()
	if err != nil {
		return nil, err
	}
	if err := q.state.begin(ctx); err != nil {
		// begin fails before the query turned busy: for a closed query,
		// which is dropped, or for the context, which leaves it idle.
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			p.put(q)
		}
		return nil, err
	}
	return q, nil
}

// exec executes value with options, which are frozen, so the execution can
// outlive a cancelled call without copying them.
func (p *queryPool[P, R]) exec(ctx context.Context, value P, options CompiledOptions) (model.Result, error) {
	q, err := p.capture(ctx, value)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	result, err := q.state.runtime.exec(ctx, &q.bound, options)
	q.state.owner.InternalRelease()
	return result, err
}

func (p *queryPool[P, R]) each(ctx context.Context, value P, dst *R, fn func(*R) error, options CompiledOptions) (bool, model.Result, error) {
	q, err := p.capture(ctx, value)
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	found, result, err := q.state.runtime.each(ctx, &q.bound, options, dst, fn, &q.scratch)
	q.state.owner.InternalRelease()
	return found, result, err
}

// hold marks a captured pooled query as held open by a handle its caller
// will own, and registers it with the session: Session.Close then stops the
// handle, and a handle dropped without Close is accounted for when its query
// is collected. The query unregisters itself when it becomes idle.
func (q *Query[P, R]) hold() {
	q.state.held = true
	q.state.runtime.registerHeld(&q.state)
}

// run executes value with options on a pooled query and returns rows that
// hold the query until they are closed.
func (p *queryPool[P, R]) run(ctx context.Context, value P, options CompiledOptions) (Rows[R], error) {
	q, err := p.capture(ctx, value)
	if err != nil {
		return Rows[R]{}, err
	}
	q.hold()
	rows, err := Run(ctx, q.state.runtime.Core, &q.bound, options)
	q.state.owner.InternalRelease()
	return rows, err
}

// runPage executes one page of value with options on a pooled query and
// returns a page that holds the query until it is closed.
func (p *queryPool[P, R]) runPage(ctx context.Context, value P, state model.PageState, pageSize int32, options CompiledOptions) (*Page[R], error) {
	q, err := p.capture(ctx, value)
	if err != nil {
		return nil, err
	}
	q.hold()
	page, err := RunPage(ctx, q.state.runtime.Core, &q.bound, state, pageSize, options)
	q.state.owner.InternalRelease()
	return page, err
}

// pool returns p's query pool, created on first use.
func (p *Prepared[P, R]) pool() (*queryPool[P, R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, cqltype.TypeValidation("prepared", "nil handle")
	}
	s := &p.privateStorage
	s.poolOnce.Do(func() {
		s.pool, s.poolErr = newQueryPool(s.InternalState.InternalSession, func(base *queryPoolBase) (*Query[P, R], error) {
			return newPreparedQuery(p, CompiledOptions{}, base)
		})
	})
	return s.pool, s.poolErr
}

// AddToBatch encodes value with the statement's parameters straight into
// batch's storage and appends the statement, as Query.AddToBatch does. It
// takes no pooled query. AddToBatch is safe for concurrent use with other calls
// on p, but a Batch is not: append to one batch from one goroutine at a time.
func (p *Prepared[P, R]) AddToBatch(batch *Batch, value P) error {
	if batch == nil {
		return cqltype.TypeValidation("batch", "nil batch")
	}
	if p == nil || p.privateStorage.InternalState == nil {
		return cqltype.TypeValidation("prepared", "nil handle")
	}
	state := p.privateStorage.InternalState
	s := state.InternalSession
	if s.Closed() {
		return runtimeerrors.ErrClosed
	}
	routing := preparedRouting(state, p.privateStorage.kind)
	return batch.addValue(s, preparedSource{state: state}, state.InternalStatement, &routing, p.privateStorage.params, state.InternalParameters, value, true)
}

// Exec binds value and executes the statement once. The statement must not
// return rows. The CompiledOptions applies to this call only; the zero value uses
// the session's default profile. Exec is safe for concurrent use:
// it runs on a query the statement keeps for one-shot calls, so it allocates
// no per-call execution state.
func (p *Prepared[P, R]) Exec(ctx context.Context, value P, options CompiledOptions) (model.Result, error) {
	pool, err := p.pool()
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	return pool.exec(ctx, value, options)
}

// One binds value, executes the statement once and decodes its first row
// into dst, reporting whether a row existed; without one, dst is cleared.
// Borrowed fields are detached before response storage is released. Options
// are as for Exec. One is safe for concurrent use.
func (p *Prepared[P, R]) One(ctx context.Context, value P, dst *R, options CompiledOptions) (bool, model.Result, error) {
	pool, err := p.pool()
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	found, result, err := pool.each(ctx, value, dst, nil, options)
	if !found && err == nil {
		clearDestination(dst)
	}
	return found, result, err
}

// Each binds value, executes the statement once and decodes every row,
// across all pages, into dst, calling fn after each row. Values that borrow
// response storage are valid only until fn returns. A non-nil error from fn
// stops the iteration and is returned. Options are as for Exec. Each is safe
// for concurrent use.
func (p *Prepared[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error, options CompiledOptions) (model.Result, error) {
	if fn == nil {
		return nilEachCallback()
	}
	pool, err := p.pool()
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	_, result, err := pool.each(ctx, value, dst, fn, options)
	return result, err
}

// All binds value, executes the statement once and returns an iterator over
// every row, across all pages, each decoded into dst. Rows stream page by
// page; they are not loaded into memory. The yielded row is dst and stays
// valid, borrowed fields included, until the next iteration. An error is
// yielded once, as (nil, err), and ends the iteration; stopping the loop early
// releases the execution and its pooled query. Options are as for Exec. All is
// safe for concurrent use with independent destinations.
func (p *Prepared[P, R]) All(ctx context.Context, value P, dst *R, options CompiledOptions) iter.Seq2[*R, error] {
	// All stays small enough to inline, so a range loop over it keeps the
	// iterator and what it captures on the stack.
	return func(yield func(*R, error) bool) { p.all(ctx, value, dst, options, yield) }
}

func (p *Prepared[P, R]) all(ctx context.Context, value P, dst *R, options CompiledOptions, yield func(*R, error) bool) {
	pool, err := p.pool()
	if err != nil {
		yield(nil, err)
		return
	}
	if _, _, err := pool.each(ctx, value, dst, yieldRow(yield), options); err != nil {
		yield(nil, err)
	}
}

// Run binds value, executes the statement once and returns a cursor over
// its rows across all pages, as Query.Run does. The rows hold one of the
// statement's pooled queries until they are closed; Session.Close stops rows
// still open, and rows dropped without Close release their query when the
// garbage collector finds them. Options are as for Exec. Run is safe for
// concurrent use; each Rows is not.
func (p *Prepared[P, R]) Run(ctx context.Context, value P, options CompiledOptions) (Rows[R], error) {
	pool, err := p.pool()
	if err != nil {
		return Rows[R]{}, err
	}
	return pool.run(ctx, value, options)
}

// RunPage binds value and executes one page of the statement, resuming from
// state, as Query.RunPage does with pageSize. The page holds one of the
// statement's pooled queries until it is closed, as Run's rows do. Options are
// as for Exec. RunPage is safe for concurrent use; each Page is not.
func (p *Prepared[P, R]) RunPage(ctx context.Context, value P, state model.PageState, pageSize int32, options CompiledOptions) (*Page[R], error) {
	pool, err := p.pool()
	if err != nil {
		return nil, err
	}
	return pool.runPage(ctx, value, state, pageSize, options)
}

// preparedPoolIdle reports how many idle queries p's pool holds.
func preparedPoolIdle[P, R any](p *Prepared[P, R]) int {
	pool := p.privateStorage.pool
	if pool == nil {
		return 0
	}
	idle := 0
	for i := range pool.slots {
		if pool.slots[i].Load() != nil {
			idle++
		}
	}
	return idle
}

// pooledBusy reports how many pooled one-shot queries of s are busy.
func pooledBusy(s *implCluster.Core) int64 {
	return InternalRuntimeFor(s).pooledBusy.Load()
}

// pooledIdle records that a pooled query became idle, waking Close when it
// was the last busy one.
func (rt *InternalSessionRuntime) pooledIdle() {
	if rt.pooledBusy.Add(-1) != 0 || !rt.Core.Closed() {
		return
	}
	rt.InternalMu.Lock()
	if rt.pooledWake != nil {
		close(rt.pooledWake)
		rt.pooledWake = nil
	}
	rt.InternalMu.Unlock()
}

// waitPooled waits until no pooled query is busy. The session is closed, so
// busy queries only finish: begin refuses new ones.
func (rt *InternalSessionRuntime) waitPooled() {
	for rt.pooledBusy.Load() != 0 {
		rt.InternalMu.Lock()
		if rt.pooledWake == nil {
			rt.pooledWake = make(chan struct{})
		}
		wake := rt.pooledWake
		rt.InternalMu.Unlock()
		// The last idle may have run before the channel existed.
		if rt.pooledBusy.Load() == 0 {
			return
		}
		<-wake
	}
}
