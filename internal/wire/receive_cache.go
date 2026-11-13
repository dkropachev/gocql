// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

// ReceiveCache reuses only fully released receive buffers: raw v4 frame
// bodies and v5 segment payloads. One session owns it, with hard idle
// byte/object limits independent of connection/shard counts. Outstanding page
// bytes are outside it: they are ordinary demand, not idle.
//
// Buffers are kept on size-class free lists, each a small mutex-protected LIFO
// stack. Classes are powers of two from 4 KiB to 64 KiB, then four steps per
// power of two (64, 80, 96, 112, 128, 160, ... KiB) up to 8 MiB, so above
// 64 KiB a class wastes at most 25% of a request; below it a class can be
// twice the request, and every buffer is at least 4 KiB.
// Each class holds at most receiveClassShared buffers past its free entries,
// so idle buffers of one class never take another class's slots; only the
// byte budget is shared. The class index is bit arithmetic: Acquire
// pops from the smallest class that fits and allocates a full class-size
// buffer on a miss; Release accepts only exact class sizes. Requests above
// 8 MiB are plain allocations.
type ReceiveCache struct {
	// idle is the bytes of buffers under the shared budget: every buffer of a
	// class above receiveCacheFreeClassMax, and everything stacked above the
	// first receiveClassFree entries of the smaller classes. One CAS enforces
	// it. The remaining entries are covered by the fixed receiveCacheFreeBytes
	// allowance and cost no atomic.
	idle    atomic.Int64
	epoch   atomic.Uint64
	closed  atomic.Bool
	over    atomic.Uint64 // misses above the largest class
	_       [64]byte      // header off the first class's mutex
	classes [receiveCacheClasses]receiveClass
}

type receiveClass struct {
	mu           sync.Mutex
	stack        [][]byte
	hits, misses uint64
	_            [64]byte // keep neighbouring classes off one cache line
}

const ReceiveCacheMaxBytes = 16 << 20

// ReceiveCacheMaxObjects bounds idle buffers over all classes: up to
// receiveClassShared budgeted buffers per class plus the fixed allowance of
// the small classes.
const ReceiveCacheMaxObjects = receiveCacheClasses*receiveClassShared + receiveCacheFreeObjects

// ReceiveCacheMaxObject is the largest buffer Release retains.
const ReceiveCacheMaxObject = 8 << 20

const (
	receiveCacheMinShift = 12
	receiveCacheMinClass = 1 << receiveCacheMinShift
	// Classes: 4, 8, 16, 32, 64 KiB, then 4 steps in each of the 7 octaves
	// (64 KiB, 128 KiB], ... (4 MiB, 8 MiB].
	receiveCacheClasses = 5 + 7*4
	// The first receiveClassFree buffers of every class up to
	// receiveCacheFreeClassMax (256 KiB) stay outside the shared budget, which
	// leaves room for them within the hard limits. Larger classes (frames over
	// 256 KiB) always go through the atomic budget.
	receiveClassFree         = 2
	receiveCacheFreeClassMax = 12
	// receiveClassShared caps each class's budgeted buffers.
	receiveClassShared      = 64
	receiveCacheFreeObjects = receiveClassFree * (receiveCacheFreeClassMax + 1)
	// 2 * (4+8+16+32+64 + 80..128 + 160..256 KiB).
	receiveCacheFreeBytes = 2 * (124 + 416 + 832) << 10
)

// receiveClassIndex returns the smallest class holding n bytes, n <= 8 MiB.
func receiveClassIndex(n int) int {
	if n <= receiveCacheMinClass {
		return 0
	}
	if n <= 64<<10 {
		return bits.Len(uint(n-1)) - receiveCacheMinShift
	}
	m := uint(n - 1)
	e := bits.Len(m) - 1 // >= 16
	return 5 + (e-16)*4 + int(m>>(e-2))&3
}

// receiveClassSize is the capacity of class idx.
func receiveClassSize(idx int) int {
	if idx < 5 {
		return receiveCacheMinClass << idx
	}
	j := idx - 5
	return (4 + 1 + j&3) << (16 + j>>2 - 2)
}

func (c *ReceiveCache) Acquire(n int) ([]byte, uint64) {
	if c == nil {
		return make([]byte, n), 0
	}
	epoch := c.epoch.Load()
	if n > ReceiveCacheMaxObject {
		c.over.Add(1)
		return make([]byte, n), epoch
	}
	idx := receiveClassIndex(n)
	cl := &c.classes[idx]
	cl.mu.Lock()
	if last := len(cl.stack) - 1; last >= 0 && !c.closed.Load() {
		b := cl.stack[last]
		cl.stack[last] = nil
		cl.stack = cl.stack[:last]
		cl.hits++
		cl.mu.Unlock()
		if last >= receiveClassFreeEntries(idx) {
			c.idle.Add(-int64(cap(b)))
		}
		return b[:n], epoch
	}
	cl.misses++
	cl.mu.Unlock()
	// Full class capacity lets the buffer serve any request of its class.
	return make([]byte, n, receiveClassSize(idx)), epoch
}

// receiveClassFreeEntries is how many buffers of class idx bypass the budget.
func receiveClassFreeEntries(idx int) int {
	if idx > receiveCacheFreeClassMax {
		return 0
	}
	return receiveClassFree
}

// Release returns b, which must have been acquired from a cache of this
// version (its capacity is an exact class size). Other capacities are
// dropped, so the byte bound cannot be exceeded by odd allocations.
func (c *ReceiveCache) Release(b []byte, epoch uint64) {
	size := cap(b)
	if c == nil || size < receiveCacheMinClass || size > ReceiveCacheMaxObject {
		return
	}
	idx := receiveClassIndex(size)
	if receiveClassSize(idx) != size {
		return
	}
	cl := &c.classes[idx]
	cl.mu.Lock()
	// Checked under the class lock: Clear/Close bump the state first and then
	// sweep every class, so a stale buffer cannot slip in behind the sweep.
	if c.closed.Load() || epoch != c.epoch.Load() {
		cl.mu.Unlock()
		return
	}
	if free := receiveClassFreeEntries(idx); len(cl.stack) >= free {
		if len(cl.stack)-free >= receiveClassShared {
			cl.mu.Unlock()
			return
		}
		for {
			v := c.idle.Load()
			if v+int64(size) > ReceiveCacheMaxBytes-receiveCacheFreeBytes {
				cl.mu.Unlock()
				return
			}
			if c.idle.CompareAndSwap(v, v+int64(size)) {
				break
			}
		}
	}
	cl.stack = append(cl.stack, b[:size])
	cl.mu.Unlock()
}

// Clear drops idle storage and invalidates buffers acquired earlier.
func (c *ReceiveCache) Clear() {
	if c == nil {
		return
	}
	c.epoch.Add(1)
	c.drain()
}

func (c *ReceiveCache) Close() {
	if c == nil {
		return
	}
	c.closed.Store(true)
	c.epoch.Add(1)
	c.drain()
}

func (c *ReceiveCache) drain() {
	for i := range c.classes {
		cl := &c.classes[i]
		cl.mu.Lock()
		for j := receiveClassFreeEntries(i); j < len(cl.stack); j++ {
			c.idle.Add(-int64(cap(cl.stack[j])))
		}
		clear(cl.stack)
		cl.stack = nil
		cl.mu.Unlock()
	}
}

func (c *ReceiveCache) Storage() (bytes, objects int) {
	if c == nil {
		return
	}
	for i := range c.classes {
		cl := &c.classes[i]
		cl.mu.Lock()
		for _, b := range cl.stack {
			bytes += cap(b)
		}
		objects += len(cl.stack)
		cl.mu.Unlock()
	}
	return
}

// ReceiveCacheStats reports cumulative reuse and current idle retention. Active
// response pages are excluded; they remain subject to frame/decompression limits.
type ReceiveCacheStats struct {
	Hits, Misses                   uint64
	RetainedBytes, RetainedObjects int
}

func (c *ReceiveCache) Stats() ReceiveCacheStats {
	if c == nil {
		return ReceiveCacheStats{}
	}
	// Stats takes each class lock once, briefly. Counting under the class
	// locks keeps atomics off Acquire and Release; Stats is read only by
	// speculation decisions and metrics snapshots.
	st := ReceiveCacheStats{Misses: c.over.Load()}
	for i := range c.classes {
		cl := &c.classes[i]
		cl.mu.Lock()
		st.Hits += cl.hits
		st.Misses += cl.misses
		for _, b := range cl.stack {
			st.RetainedBytes += cap(b)
		}
		st.RetainedObjects += len(cl.stack)
		cl.mu.Unlock()
	}
	return st
}
