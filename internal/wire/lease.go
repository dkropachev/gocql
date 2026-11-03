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
	"math"
	"sync/atomic"
)

// Lease keeps a Chain's backing storage alive until its final Release. A Lease
// must not be copied. Lease objects are never recycled, so an old pointer cannot
// retain a new owner's storage (the pooled-reference ABA problem).
type Lease struct {
	refs     atomic.Int64
	chain    Chain
	releaser LeaseReleaser
}

// LeaseReleaser runs once on a lease's final release.
type LeaseReleaser interface{ ReleaseLease() }

// releaseFunc adapts a callback; func values are stored in interfaces directly.
type releaseFunc func()

func (f releaseFunc) ReleaseLease() { f() }

// NewLease creates a lease with one reference. release runs synchronously and
// exactly once on the final Release; it may be nil.
func NewLease(chain Chain, release func()) *Lease {
	l := &Lease{chain: chain}
	if release != nil {
		l.releaser = releaseFunc(release)
	}
	perfLease(chain.Len())
	l.refs.Store(1)
	return l
}

// cachedLease returns a received buffer to its cache on the final release,
// in one allocation instead of a lease plus a release closure.
type cachedLease struct {
	Lease
	cache *ReceiveCache
	buf   []byte
	epoch uint64
}

func (c *cachedLease) ReleaseLease() { c.cache.Release(c.buf, c.epoch) }

// NewCachedLease is NewLease whose final release returns buf, acquired with
// epoch, to cache.
func NewCachedLease(chain Chain, cache *ReceiveCache, buf []byte, epoch uint64) *Lease {
	c := &cachedLease{cache: cache, buf: buf, epoch: epoch}
	c.chain = chain
	c.releaser = c
	perfLease(chain.Len())
	c.refs.Store(1)
	return &c.Lease
}

// InitLease initialises a Lease embedded in a caller's struct with one
// reference; r runs once on the final Release. Like NewLease, the embedding
// struct must be newly allocated and never recycled.
func InitLease(l *Lease, chain Chain, r LeaseReleaser) {
	l.chain = chain
	l.releaser = r
	perfLease(chain.Len())
	l.refs.Store(1)
}

// Retain acquires another reference, or returns false after the final release.
func (l *Lease) Retain() bool {
	if l == nil {
		return false
	}
	for {
		n := l.refs.Load()
		if n <= 0 || n == math.MaxInt64 {
			return false
		}
		if l.refs.CompareAndSwap(n, n+1) {
			perfAdd(7, 1)
			return true
		}
	}
}

// Alive reports whether references remain. It does not acquire a reference;
// callers accessing storage must already hold a reference for the entire use.
// In particular, a concurrent final Release can invalidate a positive result.
func (l *Lease) Alive() bool { return l != nil && l.refs.Load() > 0 }

// Release drops a reference. Calls on nil or an already released lease are
// harmless. Every successful Retain still requires its own Release.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	for {
		n := l.refs.Load()
		if n <= 0 {
			return
		}
		if l.refs.CompareAndSwap(n, n-1) {
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

// Chain returns the borrowed view. The caller must hold a reference throughout
// all uses of the returned view. A nil or fully released lease returns empty.
func (l *Lease) Chain() Chain {
	if !l.Alive() {
		return Chain{}
	}
	return l.chain
}
