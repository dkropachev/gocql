// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

// ReceiveCache reuses only fully released raw v4 frame allocations. One
// session owns it, with hard idle byte/object limits independent of
// connection/shard counts. Outstanding page bytes are ordinary demand, not idle.
//
// Buffers are kept on power-of-two size-class free lists (4 KiB to 1 MiB), each
// a small mutex-protected LIFO stack. Acquire pops from the class that fits the
// request and Release pushes onto the largest class the buffer can serve, so
// neither scans. Requests above the largest class are plain allocations.
type ReceiveCache struct {
	// idle packs the bytes (low idleObjectShift bits) and count of buffers
	// stacked above each class's first receiveClassFree entries, so one CAS
	// enforces the shared budget. The first entries of a class are covered by
	// the fixed receiveCacheFreeBytes/Objects allowance and cost no atomic.
	idle    atomic.Int64
	epoch   atomic.Uint64
	closed  atomic.Bool
	over    atomic.Uint64 // misses above the largest class
	classes [receiveCacheClasses]receiveClass
}

type receiveClass struct {
	mu           sync.Mutex
	stack        [][]byte
	hits, misses uint64
	_            [64]byte // keep neighbouring classes off one cache line
}

const ReceiveCacheMaxBytes = 16 << 20

// ReceiveCacheMaxObjects bounds idle buffers over all classes: 64 under the
// shared budget plus the fixed per-class allowance.
const ReceiveCacheMaxObjects = receiveCacheSharedObjects + receiveCacheFreeObjects

// ReceiveCacheMaxObject is the largest buffer Release retains.
const ReceiveCacheMaxObject = receiveCacheMinClass << (receiveCacheClasses - 1)

const (
	receiveCacheMinShift       = 12
	receiveCacheMinClass       = 1 << receiveCacheMinShift
	receiveCacheClasses        = 9 // 4 KiB ... 1 MiB
	idleObjectShift            = 40
	idleObjectUnit       int64 = 1 << idleObjectShift
	// Each class keeps its first receiveClassFree buffers outside the shared
	// budget; the budget leaves room for them within the hard limits.
	receiveClassFree          = 2
	receiveCacheSharedObjects = 64
	receiveCacheFreeObjects   = receiveClassFree * receiveCacheClasses
	receiveCacheFreeBytes     = receiveClassFree * (receiveCacheMinClass<<receiveCacheClasses - receiveCacheMinClass)
)

func (c *ReceiveCache) Acquire(n int) ([]byte, uint64) {
	if c == nil {
		return make([]byte, n), 0
	}
	epoch := c.epoch.Load()
	if n > ReceiveCacheMaxObject {
		c.over.Add(1)
		return make([]byte, n), epoch
	}
	idx := 0
	if n > receiveCacheMinClass {
		idx = bits.Len(uint(n-1)) - receiveCacheMinShift
	}
	cl := &c.classes[idx]
	cl.mu.Lock()
	if last := len(cl.stack) - 1; last >= 0 && !c.closed.Load() {
		b := cl.stack[last]
		cl.stack[last] = nil
		cl.stack = cl.stack[:last]
		cl.hits++
		cl.mu.Unlock()
		if last >= receiveClassFree {
			c.idle.Add(-(int64(cap(b)) + idleObjectUnit))
		}
		return b[:n], epoch
	}
	cl.misses++
	cl.mu.Unlock()
	// Full class capacity lets the buffer serve any request of its class.
	return make([]byte, n, receiveCacheMinClass<<idx), epoch
}

func (c *ReceiveCache) Release(b []byte, epoch uint64) {
	size := cap(b)
	if c == nil || size < receiveCacheMinClass || size > ReceiveCacheMaxObject {
		return
	}
	idx := bits.Len(uint(size)) - 1 - receiveCacheMinShift
	cl := &c.classes[idx]
	cl.mu.Lock()
	// Checked under the class lock: Clear/Close bump the state first and then
	// sweep every class, so a stale buffer cannot slip in behind the sweep.
	if c.closed.Load() || epoch != c.epoch.Load() {
		cl.mu.Unlock()
		return
	}
	if len(cl.stack) >= receiveClassFree {
		for {
			v := c.idle.Load()
			if v&(idleObjectUnit-1)+int64(size) > ReceiveCacheMaxBytes-receiveCacheFreeBytes || v>>idleObjectShift >= receiveCacheSharedObjects {
				cl.mu.Unlock()
				return
			}
			if c.idle.CompareAndSwap(v, v+int64(size)+idleObjectUnit) {
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
		for j := receiveClassFree; j < len(cl.stack); j++ {
			c.idle.Add(-(int64(cap(cl.stack[j])) + idleObjectUnit))
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
