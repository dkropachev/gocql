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
}
