/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wire

import (
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/syncx"
)

// Lease keeps a Chain's backing storage alive until its final Release. A Lease
// must not be copied.
//
// A lease's state word holds a generation in its upper half and the reference
// count in its lower half; the final release advances the generation. Code
// that owns a reference uses the lease directly. Code that keeps a borrowed
// view without a reference, such as cqltype.Bytes, keeps a Ref instead: it
// names the generation the view belongs to, so a released lease that was
// recycled for a later owner is never mistaken for the view's own.
type Lease struct {
	releaser LeaseReleaser
	chain    Chain
	state    atomic.Uint64 // generation<<32 | references
}

const (
	leaseRefsMask  = 1<<32 - 1
	leaseGenerator = 1 << 32
)

// LeaseReleaser runs once on a lease's final release.
type LeaseReleaser interface{ ReleaseLease() }

// releaseFunc adapts a callback; func values are stored in interfaces directly.
type releaseFunc func()

func (f releaseFunc) ReleaseLease() { f() }

// NewLease creates a lease with one reference. release runs synchronously and
// exactly once on the final Release; it may be nil.
func NewLease(chain Chain, release func()) *Lease {
	l := new(Lease)
	var r LeaseReleaser
	if release != nil {
		r = releaseFunc(release)
	}
	l.init(chain, r)
	return l
}

// init gives the lease one reference in its current generation.
func (l *Lease) init(chain Chain, r LeaseReleaser) {
	l.chain = chain
	l.releaser = r
	perfLease(chain.Len())
	l.state.Store(l.state.Load()&^leaseRefsMask | 1)
}

// cachedLease returns a received buffer to its cache on the final release,
// and itself to a pool: Refs from earlier owners carry their generation.
type cachedLease struct {
	cache *ReceiveCache
	buf   []byte
	Lease
	epoch uint64
}

var cachedLeasePool = syncx.NewPool(func() *cachedLease { return new(cachedLease) })

func (c *cachedLease) ReleaseLease() {
	cache, buf, epoch := c.cache, c.buf, c.epoch
	c.cache, c.buf, c.chain, c.releaser = nil, nil, Chain{}, nil
	cachedLeasePool.Put(c)
	cache.Release(buf, epoch)
}

// NewCachedLease is NewLease whose final release returns buf, acquired with
// epoch, to cache.
func NewCachedLease(chain Chain, cache *ReceiveCache, buf []byte, epoch uint64) *Lease {
	c := cachedLeasePool.Get()
	c.cache, c.buf, c.epoch = cache, buf, epoch
	c.init(chain, c)
	return &c.Lease
}

// InitLease initialises a Lease embedded in a caller's struct with one
// reference; r runs once on the final Release. The embedding struct may be
// reused once that release finished: holders without a reference keep a Ref.
func InitLease(l *Lease, chain Chain, r LeaseReleaser) {
	l.init(chain, r)
}

// Retain acquires another reference, or returns false after the final release.
// Only a holder that already owns a reference may call it; others use Ref.
func (l *Lease) Retain() bool {
	if l == nil {
		return false
	}
	for {
		v := l.state.Load()
		if n := v & leaseRefsMask; n == 0 || n == leaseRefsMask {
			return false
		}
		if l.state.CompareAndSwap(v, v+1) {
			perfAdd(7, 1)
			return true
		}
	}
}

// Alive reports whether references remain. It does not acquire a reference;
// callers accessing storage must already hold a reference for the entire use.
// In particular, a concurrent final Release can invalidate a positive result.
func (l *Lease) Alive() bool { return l != nil && l.state.Load()&leaseRefsMask > 0 }

// Release drops a reference. Calls on nil or an already released lease are
// harmless. Every successful Retain still requires its own Release.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	for {
		v := l.state.Load()
		n := v & leaseRefsMask
		if n == 0 {
			return
		}
		next := v - 1
		if n == 1 {
			next = v&^leaseRefsMask + leaseGenerator // next generation, no references
		}
		if l.state.CompareAndSwap(v, next) {
			perfAdd(8, 1)
			if n == 1 {
				perfLease(-l.chain.Len())
				if l.releaser != nil {
					l.releaser.ReleaseLease()
				}
			}
			return
		}
	}
}

// Ref names one generation of a lease without holding a reference. It is how
// borrowed views remember their storage: once that generation's final release
// happened, the Ref is dead even if the lease serves a later owner.
type Ref struct {
	l   *Lease
	gen uint32
}

// RefOf returns a Ref to l's current generation. The caller must hold a
// reference, so that the generation is the one it observed.
func RefOf(l *Lease) Ref {
	if l == nil {
		return Ref{}
	}
	return Ref{l: l, gen: uint32(l.state.Load() >> 32)}
}

// IsZero reports whether the Ref names no lease.
func (r Ref) IsZero() bool { return r.l == nil }

// Lease returns the referenced lease. Use it only while holding a reference
// obtained through Retain.
func (r Ref) Lease() *Lease { return r.l }

// Alive reports whether the Ref's generation still has references.
func (r Ref) Alive() bool {
	if r.l == nil {
		return false
	}
	v := r.l.state.Load()
	return uint32(v>>32) == r.gen && v&leaseRefsMask > 0
}

// Retain acquires a reference to the Ref's generation, or returns false once
// that generation was released. A successful Retain is paired with
// r.Lease().Release().
func (r Ref) Retain() bool {
	if r.l == nil {
		return false
	}
	for {
		v := r.l.state.Load()
		n := v & leaseRefsMask
		if uint32(v>>32) != r.gen || n == 0 || n == leaseRefsMask {
			return false
		}
		if r.l.state.CompareAndSwap(v, v+1) {
			perfAdd(7, 1)
			return true
		}
	}
}

// Chain returns the borrowed view. The caller must hold a reference throughout
// all uses of the returned view. A nil or fully released lease returns empty.
func (l *Lease) Chain() Chain {
	if !l.Alive() {
		return Chain{}
	}
	return l.chain
}
