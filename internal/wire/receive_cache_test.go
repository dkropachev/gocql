// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import (
	"sync"
	"testing"
)

func CodecTestChain(t *testing.T, b []byte) Chain {
	t.Helper()
	c, e := NewChain(Borrow(b))
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestReceiveCache(t *testing.T) {
	t.Run("SaturationStatistics", func(t *testing.T) {
		cache := new(ReceiveCache)
		for range 100 {
			cache.Release(make([]byte, 4096), 0)
		}
		stats := cache.Stats()
		if stats.RetainedObjects != 64+receiveClassFree || stats.RetainedBytes != (64+receiveClassFree)*4096 {
			t.Fatal(stats)
		}
		for range 64 + receiveClassFree + 1 {
			_, _ = cache.Acquire(4096)
		}
		stats = cache.Stats()
		if stats.Hits != 64+receiveClassFree || stats.Misses != 1 || stats.RetainedObjects != 0 || stats.RetainedBytes != 0 {
			t.Fatal(stats)
		}
	})
	t.Run("Ownership", func(t *testing.T) {
		cache := new(ReceiveCache)
		data, epoch := cache.Acquire(4096)
		data[0] = 71
		chain, _ := NewChain(Borrow(data))
		lease := NewLease(chain, func() { cache.Release(data, epoch) })
		if !lease.Retain() {
			t.Fatal("retain")
		}
		lease.Release()
		if n, _ := cache.Storage(); n != 0 {
			t.Fatal("returned while retained")
		}
		other, _ := cache.Acquire(4096)
		if &other[0] == &data[0] {
			t.Fatal("live page reused")
		}
		lease.Release()
		if n, _ := cache.Storage(); n != 4096 {
			t.Fatal(n)
		}
		reused, nextEpoch := cache.Acquire(4096)
		if &reused[0] != &data[0] || reused[0] != 71 {
			t.Fatal("not actual backing reuse")
		}
		fresh := NewLease(CodecTestChain(t, reused), func() { cache.Release(reused, nextEpoch) })
		if fresh == lease || lease.Retain() {
			t.Fatal("lease ABA")
		}
		fresh.Release()
		cache.Clear()
		if n, c := cache.Storage(); n != 0 || c != 0 {
			t.Fatal("clear", n, c)
		}
		stale, staleEpoch := cache.Acquire(8192)
		cache.Clear()
		cache.Release(stale, staleEpoch)
		if n, _ := cache.Storage(); n != 0 {
			t.Fatal("stale generation returned")
		}
		for i := 0; i < 8; i++ {
			cache.Release(make([]byte, 1<<20), cache.epoch.Load())
		}
		if n, c := cache.Storage(); n > ReceiveCacheMaxBytes || c > ReceiveCacheMaxObjects {
			t.Fatal("object/byte bound", n, c)
		}
		cache.Clear()
		cache.Release(make([]byte, ReceiveCacheMaxObject+1), cache.epoch.Load())
		if n, _ := cache.Storage(); n != 0 {
			t.Fatal("oversized cached")
		}
		for i := 0; i < 20; i++ {
			cache.Release(make([]byte, 1<<20), cache.epoch.Load())
		}
		if n, c := cache.Storage(); n > ReceiveCacheMaxBytes || n < ReceiveCacheMaxBytes-2*(1<<20) || c < 14 {
			t.Fatal("total budget", n, c)
		}
		active, e := cache.Acquire(1 << 20)
		cache.Close()
		cache.Release(active, e)
		if n, c := cache.Storage(); n != 0 || c != 0 {
			t.Fatal("shutdown retained", n, c)
		}
	})
	t.Run("MixedAdmission", func(t *testing.T) {
		cache := new(ReceiveCache)
		for i := 0; i < 4; i++ {
			cache.Release(make([]byte, 32+i), cache.epoch.Load())
		}
		hot, e := cache.Acquire(512 << 10)
		hot[0] = 71
		cache.Release(hot, e)
		reused, e := cache.Acquire(512 << 10)
		if &reused[0] != &hot[0] || reused[0] != 71 {
			t.Fatal("four tiny slots starved hot frame backing")
		}
		cache.Release(reused, e)
		for i := 0; i < 1000; i++ {
			cache.Release(make([]byte, 32+i%4), cache.epoch.Load())
		}
		reused, e = cache.Acquire(512 << 10)
		if &reused[0] != &hot[0] {
			t.Fatal("tiny traffic evicted hot frame")
		}
		cache.Release(reused, e)
		for i := 0; i < 64; i++ {
			other, e := cache.Acquire(1024 + (i%4)*1024)
			cache.Release(other, e)
			large, e := cache.Acquire(512 << 10)
			if &large[0] != &hot[0] {
				t.Fatal("mixed classes lost hot backing")
			}
			cache.Release(large, e)
		}
		bytes, objects := cache.Storage()
		if bytes > ReceiveCacheMaxBytes || objects > ReceiveCacheMaxObjects {
			t.Fatal(bytes, objects)
		}
		cache.Clear()
		stale, e := cache.Acquire(1 << 20)
		cache.Clear()
		cache.Release(stale, e)
		if n, _ := cache.Storage(); n != 0 {
			t.Fatal("epoch stale admission")
		}
		cache.Close()
	})
	// Checks that once a cached lease
	// is recycled for a later frame, a Ref taken from its earlier generation
	// stays dead: it neither reports the new owner alive nor retains it.
	t.Run("LeaseRecycling", func(t *testing.T) {
		cache := new(ReceiveCache)
		var stale []Ref
		seen := map[*Lease]bool{}
		recycled := false
		for i := 0; i < 64 && !recycled; i++ {
			data, epoch := cache.Acquire(64)
			chain, _ := NewChain(Borrow(data))
			lease := NewCachedLease(chain, cache, data, epoch)
			ref := RefOf(lease)
			if !ref.Alive() {
				t.Fatal("fresh generation not alive")
			}
			for _, old := range stale {
				if old.Lease() == lease {
					recycled = true
					if old.Alive() || old.Retain() {
						t.Fatal("stale generation observed a recycled lease's new owner")
					}
				}
			}
			recycled = recycled || seen[lease]
			seen[lease] = true
			lease.Release()
			if ref.Alive() || ref.Retain() {
				t.Fatal("released generation still alive")
			}
			stale = append(stale, ref)
		}
		if !recycled {
			t.Skip("the pool never recycled a lease")
		}
	})
}

func TestReceiveCacheSizeClasses(t *testing.T) {
	t.Run("IdleBudget", func(t *testing.T) {
		cache := new(ReceiveCache)
		for i := 0; i < 1000; i++ {
			cache.Release(make([]byte, 1<<20), 0)
			cache.Release(make([]byte, 4096), 0)
			if n, c := cache.Storage(); n > ReceiveCacheMaxBytes || c > ReceiveCacheMaxObjects {
				t.Fatal("idle bound", n, c)
			}
		}
		if n, _ := cache.Storage(); n == 0 {
			t.Fatal("nothing retained")
		}
	})
	t.Run("SameSizeReuse", func(t *testing.T) {
		cache := new(ReceiveCache)
		for _, n := range []int{100, 4096, 5000, 64 << 10, 1 << 20} {
			b, e := cache.Acquire(n)
			if len(b) != n {
				t.Fatal(len(b), n)
			}
			cache.Release(b, e)
			again, _ := cache.Acquire(n)
			if &again[0] != &b[0] {
				t.Fatalf("size %d not reused", n)
			}
		}
	})
	t.Run("OversizeFallback", func(t *testing.T) {
		cache := new(ReceiveCache)
		n := 3<<20 + 1
		b, e := cache.Acquire(n)
		if len(b) != n {
			t.Fatal(len(b))
		}
		cache.Release(b, e)
		if bytes, objects := cache.Storage(); bytes != 0 || objects != 0 {
			t.Fatal("oversize retained", bytes, objects)
		}
		if again, _ := cache.Acquire(n); &again[0] == &b[0] {
			t.Fatal("oversize reused")
		}
	})
	t.Run("LargerBufferServesSmallerRequest", func(t *testing.T) {
		cache := new(ReceiveCache)
		big := make([]byte, 9000)
		cache.Release(big, 0)
		got, _ := cache.Acquire(8192)
		if &got[0] != &big[0] || len(got) != 8192 {
			t.Fatal("floor class entry not reused")
		}
	})
	t.Run("Concurrent", func(t *testing.T) {
		cache := new(ReceiveCache)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 2000; i++ {
					n := 1 << (8 + (i+g)%13)
					b, e := cache.Acquire(n)
					for j := range b {
						b[j] = byte(g)
					}
					for j := range b {
						if b[j] != byte(g) {
							t.Error("buffer shared between owners")
							return
						}
					}
					if i%500 == 0 {
						cache.Clear()
					}
					cache.Release(b, e)
					if bytes, objects := cache.Storage(); bytes > ReceiveCacheMaxBytes || objects > ReceiveCacheMaxObjects {
						t.Error("bound", bytes, objects)
						return
					}
				}
			}()
		}
		wg.Wait()
		cache.Close()
		if n, c := cache.Storage(); n != 0 || c != 0 || cache.idle.Load() != 0 {
			t.Fatal(n, c, cache.idle.Load())
		}
	})
	t.Run("SharedBudgetBalanced", func(t *testing.T) {
		cache := new(ReceiveCache)
		for round := 0; round < 3; round++ {
			var held [][]byte
			for i := 0; i < 40; i++ {
				b, _ := cache.Acquire(4096 << (i % 4))
				held = append(held, b)
			}
			for _, b := range held {
				cache.Release(b, cache.epoch.Load())
			}
			for range 25 {
				b, _ := cache.Acquire(8192)
				_ = b
			}
		}
		cache.Clear()
		if n, c := cache.Storage(); n != 0 || c != 0 || cache.idle.Load() != 0 {
			t.Fatal(n, c, cache.idle.Load())
		}
	})
}
