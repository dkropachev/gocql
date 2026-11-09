// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// queryPoolBase is what the session runtime sees of a query pool: every query
// the pool created, so that closing the session stops them. The runtime
// refers to it weakly, so a pool nobody uses is collected with its queries.
type queryPoolBase struct {
	all    []*queryState
	mu     sync.Mutex
	closed atomic.Bool // set under mu
}

// adopt tracks q, a query the pool is creating.
func (b *queryPoolBase) adopt(q *queryState) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return ErrClosed
	}
	b.all = append(b.all, q)
	return nil
}

// forget stops tracking q, a query the pool is closing.
func (b *queryPoolBase) forget(q *queryState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, tracked := range b.all {
		if tracked == q {
			last := len(b.all) - 1
			b.all[i], b.all[last] = b.all[last], nil
			b.all = b.all[:last]
			return
		}
	}
}

// close stops the pool from taking or creating queries and returns those it
// created, for the caller to stop.
func (b *queryPoolBase) close() []*queryState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return nil
	}
	b.closed.Store(true)
	all := b.all
	b.all = nil
	return all
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
		return ErrClosed
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

// InternalQueryPools reports how many query pools the session tracks.
func InternalQueryPools(s *implCluster.Core) int {
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
	if err := InternalRuntimeFor(s).registerPool(s, p.base); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *queryPool[P, R]) get() (*Query[P, R], error) {
	if p.base.closed.Load() {
		return nil, ErrClosed
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
		q.state.idle()
		p.put(q)
	}
	return q, nil
}

// put keeps q, which just became idle, or closes it when every slot is full.
func (p *queryPool[P, R]) put(q *Query[P, R]) {
	if p.base.closed.Load() {
		return
	}
	n := len(p.slots)
	for i, start := 0, int(rand.Uint32N(uint32(n))); i < n; i++ {
		slot := &p.slots[(start+i)%n]
		if slot.Load() == nil && slot.CompareAndSwap(nil, q) {
			return
		}
	}
	p.base.forget(&q.state)
	q.state.close()
}

// capture takes an idle query and captures value into it. On success the
// caller owns the query until it releases the query's owner, after which the
// query may already serve another call.
func (p *queryPool[P, R]) capture(ctx context.Context, value P) (*Query[P, R], error) {
	if ctx == nil {
		return nil, typeValidation("context", "nil context")
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

// exec executes value with options, which are frozen, so the execution can
// outlive a cancelled call without copying them.
func (p *queryPool[P, R]) exec(ctx context.Context, value P, options Options) (Result, error) {
	q, err := p.capture(ctx, value)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	result, err := Exec(ctx, q.state.runtime.Core, &q.bound, options)
	q.state.owner.InternalRelease()
	return result, err
}

func (p *queryPool[P, R]) each(ctx context.Context, value P, dst *R, fn func(*R) error, options Options) (bool, error) {
	q, err := p.capture(ctx, value)
	if err != nil {
		return false, err
	}
	found, _, err := each(ctx, q.state.runtime.Core, &q.bound, options, dst, fn, &q.scratch)
	q.state.owner.InternalRelease()
	return found, err
}

// pool returns p's query pool, created on first use.
func (p *Prepared[P, R]) pool() (*queryPool[P, R], error) {
	s := &p.privateStorage
	s.poolOnce.Do(func() {
		s.pool, s.poolErr = newQueryPool(s.InternalState.InternalSession, func(base *queryPoolBase) (*Query[P, R], error) {
			return newPreparedQuery(p, Options{}, base)
		})
	})
	return s.pool, s.poolErr
}

// Exec binds value and executes the statement once with options, which
// apply to this call only. The statement must not return rows. Exec is safe
// for concurrent use; it reuses the storage of earlier calls, so it is the
// cheapest way to execute a prepared statement.
func (p *Prepared[P, R]) Exec(ctx context.Context, value P, options Options) (Result, error) {
	if p == nil || p.privateStorage.InternalState == nil {
		err := typeValidation("prepared", "nil handle")
		return InternalExecutionErrorResult(err), err
	}
	pool, err := p.pool()
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	return pool.exec(ctx, value, options)
}

// One binds value, executes the statement once with options and decodes its
// first row into dst, reporting whether a row existed. Values that borrow
// response storage are invalid once One returns. One is safe for concurrent
// use.
func (p *Prepared[P, R]) One(ctx context.Context, value P, dst *R, options Options) (bool, error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return false, typeValidation("prepared", "nil handle")
	}
	pool, err := p.pool()
	if err != nil {
		return false, err
	}
	found, err := pool.each(ctx, value, dst, nil, options)
	if !found && err == nil {
		InternalClearDestination(dst)
	}
	return found, err
}

// Each binds value, executes the statement once with options and decodes
// every row, across all pages, into dst, calling fn after each row. Values
// that borrow response storage are valid only until fn returns. A non-nil
// error from fn stops the iteration and is returned. Each is safe for
// concurrent use.
func (p *Prepared[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error, options Options) error {
	if p == nil || p.privateStorage.InternalState == nil {
		return typeValidation("prepared", "nil handle")
	}
	if fn == nil {
		return typeValidation("row callback", "nil callback")
	}
	pool, err := p.pool()
	if err != nil {
		return err
	}
	_, err = pool.each(ctx, value, dst, fn, options)
	return err
}

// InternalPreparedPoolIdle reports how many idle queries p's pool holds.
func InternalPreparedPoolIdle[P, R any](p *Prepared[P, R]) int {
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

// InternalPreparedPoolTracked reports how many queries p's pool tracks.
func InternalPreparedPoolTracked[P, R any](p *Prepared[P, R]) int {
	pool := p.privateStorage.pool
	if pool == nil {
		return 0
	}
	pool.base.mu.Lock()
	defer pool.base.mu.Unlock()
	return len(pool.base.all)
}
