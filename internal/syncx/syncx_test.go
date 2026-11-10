// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package syncx

import (
	"testing"
)

type item struct{ n int }

func TestPoolNewAndReuse(t *testing.T) {
	news := 0
	p := NewPool(func() *item { news++; return &item{} })
	a := p.Get()
	if a == nil || news != 1 {
		t.Fatalf("Get() = %v after %d News, want a fresh item", a, news)
	}
	a.n = 7
	p.Put(a)
	// sync.Pool may drop items, so only check that Get never returns nil.
	if b := p.Get(); b == nil {
		t.Fatal("Get() = nil")
	}
}

func TestPoolWithoutNewReturnsZero(t *testing.T) {
	var p Pool[*item]
	if got := p.Get(); got != nil {
		t.Fatalf("Get() = %v, want nil", got)
	}
}

func TestPoolPointerDoesNotAllocate(t *testing.T) {
	p := NewPool(func() *item { return &item{} })
	p.Put(p.Get())
	if allocs := testing.AllocsPerRun(1000, func() { p.Put(p.Get()) }); allocs != 0 {
		t.Fatalf("Get/Put allocates %v times, want 0", allocs)
	}
}

func TestPoolChan(t *testing.T) {
	p := NewPool(func() chan int { return make(chan int, 1) })
	c := p.Get()
	c <- 1
	<-c
	p.Put(c)
}

func TestMap(t *testing.T) {
	var m Map[[16]byte, *item]
	k := [16]byte{1}
	if _, ok := m.Load(k); ok {
		t.Fatal("Load on empty map found a value")
	}
	first := &item{n: 1}
	if got, loaded := m.LoadOrStore(k, first); loaded || got != first {
		t.Fatalf("LoadOrStore = %v, %v; want first, false", got, loaded)
	}
	if got, loaded := m.LoadOrStore(k, &item{n: 2}); !loaded || got != first {
		t.Fatalf("second LoadOrStore = %v, %v; want first, true", got, loaded)
	}
	m.Store([16]byte{2}, &item{n: 2})
	sum := 0
	for key, v := range m.All() {
		sum += v.n
		_ = key
	}
	if sum != 3 {
		t.Fatalf("All() sum = %d, want 3", sum)
	}
	m.Delete(k)
	if _, ok := m.Load(k); ok {
		t.Fatal("Load after Delete found a value")
	}
}
