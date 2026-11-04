// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
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
	mu     sync.Mutex
	all    []*queryState
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
func (rt *InternalSessionRuntime) registerPool(s *implCluster.Session, base *queryPoolBase) error {
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
func InternalQueryPools(s *implCluster.Session) int {
	rt := InternalRuntimeFor(s)
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	return len(rt.pools)
}

// queryPool keeps idle queries of one statement for synchronous one-shot
// calls, each with its own options. A query returns to the pool when it
// becomes idle, once its execution drained, not when the call returns.
type queryPool[P, R any] struct {
	base     *queryPoolBase
	mu       sync.Mutex
	free     []*Query[P, R]
	newQuery func(*queryPoolBase) (*Query[P, R], error)
}

func newQueryPool[P, R any](s *implCluster.Session, newQuery func(*queryPoolBase) (*Query[P, R], error)) (*queryPool[P, R], error) {
	p := &queryPool[P, R]{base: new(queryPoolBase), newQuery: newQuery}
	if err := InternalRuntimeFor(s).registerPool(s, p.base); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *queryPool[P, R]) get() (*Query[P, R], error) {
	if p.base.closed.Load() {
		return nil, ErrClosed
	}
	p.mu.Lock()
	if n := len(p.free); n > 0 {
		q := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		p.mu.Unlock()
		return q, nil
	}
	p.mu.Unlock()
	q, err := p.newQuery(p.base)
	if err != nil {
		return nil, err
	}
	q.state.perCall = true
	q.state.onIdle = func() { p.put(q) }
	return q, nil
}

func (p *queryPool[P, R]) put(q *Query[P, R]) {
	if p.base.closed.Load() {
		return
	}
	p.mu.Lock()
	p.free = append(p.free, q)
	p.mu.Unlock()
}

// capture takes an idle query and captures value and options into it. On
// success the caller owns the query until it releases the query's owner,
// after which the query may already serve another call.
func (p *queryPool[P, R]) capture(ctx context.Context, value P, options *Options) (*Query[P, R], Options, error) {
	if ctx == nil {
		return nil, Options{}, typeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, Options{}, err
	}
	// Options are copied for the execution, which may outlive a cancelled
	// call: without a custom payload this does not allocate.
	opts, err := cloneQueryOptions(*options)
	if err != nil {
		return nil, Options{}, err
	}
	q, err := p.get()
	if err != nil {
		return nil, Options{}, err
	}
	// admit fails without making the query busy only when it was closed with
	// the session; the query is dropped. Later failures end the execution,
	// and the query returns through idle.
	if err := q.state.admit(); err != nil {
		return nil, Options{}, err
	}
	if err := q.captureValue(value); err != nil {
		return nil, Options{}, err
	}
	return q, opts, nil
}

func (p *queryPool[P, R]) exec(ctx context.Context, value P, options *Options) (Result, error) {
	q, opts, err := p.capture(ctx, value, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	result, err := Exec(ctx, q.state.runtime.Session, &q.bound, opts)
	q.state.owner.InternalRelease()
	return result, err
}

func (p *queryPool[P, R]) each(ctx context.Context, value P, dst *R, fn func(*R) error, options *Options) (bool, error) {
	q, opts, err := p.capture(ctx, value, options)
	if err != nil {
		return false, err
	}
	found, _, err := each(ctx, q.state.runtime.Session, &q.bound, opts, dst, fn, &q.scratch)
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
	return pool.exec(ctx, value, &options)
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
	found, err := pool.each(ctx, value, dst, nil, &options)
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
	_, err = pool.each(ctx, value, dst, fn, &options)
	return err
}

// InternalPreparedPoolIdle reports how many idle queries p's pool holds.
func InternalPreparedPoolIdle[P, R any](p *Prepared[P, R]) int {
	pool := p.privateStorage.pool
	if pool == nil {
		return 0
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return len(pool.free)
}
