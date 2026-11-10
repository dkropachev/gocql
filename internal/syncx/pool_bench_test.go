// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package syncx

import (
	"sync"
	"testing"
)

var rawPool = sync.Pool{New: func() any { return &item{} }}

func BenchmarkPoolGetPut(b *testing.B) {
	b.Run("sync", func(b *testing.B) {
		for b.Loop() {
			v := rawPool.Get().(*item)
			rawPool.Put(v)
		}
	})
	p := NewPool(func() *item { return &item{} })
	b.Run("syncx", func(b *testing.B) {
		for b.Loop() {
			p.Put(p.Get())
		}
	})
}
