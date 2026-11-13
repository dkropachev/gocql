// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "testing"

func BenchmarkReceiveCache(b *testing.B) {
	sizes := [...]int{4 << 10, 64 << 10}
	b.Run("serial", func(b *testing.B) {
		cache := new(ReceiveCache)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf, e := cache.Acquire(sizes[i&1])
			buf[0] = 1
			cache.Release(buf, e)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		cache := new(ReceiveCache)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				buf, e := cache.Acquire(sizes[i&1])
				buf[0] = 1
				cache.Release(buf, e)
				i++
			}
		})
	})
	// A loopback page frame just under 128 KiB, and a default-size page.
	for _, c := range []struct {
		name string
		size int
	}{{"serial131k", 131000}, {"serial1500k", 1500 << 10}} {
		b.Run(c.name, func(b *testing.B) {
			cache := new(ReceiveCache)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf, e := cache.Acquire(c.size)
				buf[0] = 1
				cache.Release(buf, e)
			}
		})
	}
	// Eight goroutines per GOMAXPROCS contending for one class.
	b.Run("parallel8", func(b *testing.B) {
		cache := new(ReceiveCache)
		b.ReportAllocs()
		b.SetParallelism(8)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf, e := cache.Acquire(4 << 10)
				buf[0] = 1
				cache.Release(buf, e)
			}
		})
	})
}
