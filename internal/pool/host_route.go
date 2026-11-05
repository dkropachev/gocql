// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"math"
	"sync/atomic"
)

// AnyShard picks the least busy connection of a host instead of one shard's.
const AnyShard int32 = -1

// HostRoute is a host's handle in its session's connection pools. A session
// keeps one per host ID, so routing snapshots can hold it in place of the ID
// and a request picks a connection without looking its host up again. A
// route outlives its host's pool: once the pool is removed, the route has
// none until the host gets a new one.
type HostRoute struct {
	owner *InternalPolicyConnPool // nil for a route made outside a session
	pool  atomic.Pointer[InternalHostConnPool]
	id    UUID
}

// NewHostRoute returns a route of id that belongs to no session; it has no
// pool, and a request resolves it through its session's pools by ID.
func NewHostRoute(id UUID) *HostRoute { return &HostRoute{id: id} }

func (r *HostRoute) ID() UUID { return r.id }

// Bound reports whether the route belongs to a session's pools.
func (r *HostRoute) Bound() bool { return r.owner != nil }

// Pool returns the route's current host pool, or nil.
func (r *HostRoute) Pool() *InternalHostConnPool {
	if p := r.pool.Load(); p != nil {
		return p
	}
	if r.owner == nil {
		return nil
	}
	return r.owner.bindRoute(r)
}

// InternalRoute returns the session's route of a host, made on first use.
func (p *InternalPolicyConnPool) InternalRoute(id UUID) *HostRoute {
	p.InternalMu.RLock()
	r := p.routes[id]
	p.InternalMu.RUnlock()
	if r != nil {
		return r
	}
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if r = p.routes[id]; r == nil {
		if p.routes == nil {
			p.routes = make(map[UUID]*HostRoute)
		}
		r = &HostRoute{owner: p, id: id}
		if pool := p.InternalHostConnPools[id]; pool != nil {
			r.pool.Store(pool)
		}
		p.routes[id] = r
	}
	return r
}

// bindRoute attaches a route that has no pool to its host's current pool.
func (p *InternalPolicyConnPool) bindRoute(r *HostRoute) *InternalHostConnPool {
	p.InternalMu.RLock()
	pool := p.InternalHostConnPools[r.id]
	p.InternalMu.RUnlock()
	if pool != nil {
		r.pool.CompareAndSwap(nil, pool)
	}
	return pool
}

// routeChangedLocked tells a host's route, if it has one, that the host's
// pool is now pool (nil once removed). Must be called with p.InternalMu held.
func (p *InternalPolicyConnPool) routeChangedLocked(id UUID, pool *InternalHostConnPool) {
	if r := p.routes[id]; r != nil {
		r.pool.Store(pool)
	}
}

// typedView is what a typed picker publishes for lock-free picking: its
// connections per shard, which are never modified once published, its
// sharding, and how many connections it misses.
type typedView struct {
	conns     [][]*Conn
	shards    int
	msbIgnore uint64
	count     int
	missing   int
}

// publishLocked publishes the picker's current connections. Must be called
// with p.InternalMu held for writing.
func (p *InternalTypedConnPicker) publishLocked() {
	if p.closed {
		p.view.Store(nil)
		return
	}
	n := p.InternalCount()
	p.view.Store(&typedView{conns: append([][]*Conn(nil), p.InternalConns...), shards: p.InternalNrShards, msbIgnore: p.msbIgnore, count: n, missing: max(0, p.InternalTarget()-n)})
}

func shardOf(token int64, shards int, msbIgnore uint64) int {
	z := uint64(token+math.MinInt64) << msbIgnore
	sum := ((z & 0xffffffff) * uint64(shards) >> 32) + (z>>32)*uint64(shards)
	return int(sum >> 32)
}

// ShardOf returns the shard of token on the route's host, or AnyShard when
// the host has no typed pool.
func (r *HostRoute) ShardOf(token int64) int32 {
	if v := r.view(); v != nil {
		return int32(shardOf(token, v.shards, v.msbIgnore))
	}
	return AnyShard
}

// Usable reports whether the route's host is up and its pool has connections.
func (r *HostRoute) Usable() bool {
	pool := r.Pool()
	if pool == nil || pool.InternalHost == nil || !pool.InternalHost.IsUp() {
		return false
	}
	if t := pool.typedPicker(); t != nil {
		v := t.view.Load()
		return v != nil && v.count > 0
	}
	return pool.Size() > 0
}

func (r *HostRoute) view() *typedView {
	if pool := r.Pool(); pool != nil {
		if t := pool.typedPicker(); t != nil {
			return t.view.Load()
		}
	}
	return nil
}

// typedPicker returns the pool's typed picker, or nil while it has none. A
// pool installs its picker once, replacing the placeholder it starts with.
func (pool *InternalHostConnPool) typedPicker() *InternalTypedConnPicker {
	if t := pool.typed.Load(); t != nil || pool.legacy.Load() {
		return t
	}
	pool.InternalMu.RLock()
	picker := pool.InternalConnPicker
	pool.InternalMu.RUnlock()
	switch t := picker.(type) {
	case *InternalTypedConnPicker:
		pool.typed.Store(t)
		return t
	case InternalNopConnPicker, nil:
	default:
		pool.legacy.Store(true) // the installed picker stays
	}
	return nil
}

// PickRoute returns a connection of a host with a free stream on shard, or
// the least busy one for AnyShard, or nil when it has none: it never moves
// to another shard, which is the routing plan's next route. ok is false
// when the pool has no typed picker and the caller picks the legacy way.
func (pool *InternalHostConnPool) PickRoute(shard int32) (conn *Conn, ok bool) {
	t := pool.typedPicker()
	if t == nil {
		return nil, false
	}
	if pool.InternalHost != nil && !pool.InternalHost.IsUp() {
		return nil, true
	}
	v := t.view.Load()
	if v == nil {
		return nil, true
	}
	if v.missing > 0 {
		pool.InternalScheduleTypedRepair()
	}
	if shard >= 0 {
		if int(shard) >= len(v.conns) {
			return nil, true
		}
		cs := v.conns[shard]
		// A shard usually has one connection: take it when it has a free stream.
		if len(cs) == 1 && cs[0].AvailableStreams() > 0 {
			return cs[0], true
		}
		return t.choose(cs), true
	}
	var best *Conn
	available := 0
	for _, cs := range v.conns {
		if c := t.choose(cs); c != nil {
			if n := c.AvailableStreams(); n > available {
				best, available = c, n
			} else if best == nil {
				best = c
			}
		}
	}
	return best, true
}

// choose returns the connection of cs with the most free streams, or one
// held exclusively that still has a stream, or nil.
func (p *InternalTypedConnPicker) choose(cs []*Conn) *Conn {
	var best, waiting *Conn
	available := 0
	if len(cs) == 0 {
		return nil
	}
	// The shared position only rotates among several connections.
	start := 0
	if len(cs) > 1 {
		start = int(p.InternalPos.Add(1) % uint64(len(cs)))
	}
	for i := range cs {
		c := cs[(start+i)%len(cs)]
		if n := c.AvailableStreams(); n > available {
			best, available = c, n
		} else if waiting == nil && c.InternalExclusiveFlag.Load() && c.InternalStreams.Available() > 0 {
			waiting = c
		}
	}
	if best == nil {
		return waiting
	}
	return best
}
