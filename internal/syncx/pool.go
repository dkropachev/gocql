// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package syncx holds typed wrappers over the sync package.
package syncx

import "sync"

// Pool is a sync.Pool of T. T must be pointer-shaped (a pointer, chan, map or
// func); any other T is copied into an interface on every Put and allocates.
// Make one with NewPool.
type Pool[T any] struct{ p sync.Pool }

// NewPool returns a Pool whose Get calls newFn when the pool is empty.
func NewPool[T any](newFn func() T) *Pool[T] {
	p := &Pool[T]{}
	p.p.New = func() any { return newFn() }
	return p
}

// Get returns a pooled value, or a new one; the zero T for a Pool not made by
// NewPool and empty.
func (p *Pool[T]) Get() T {
	v, _ := p.p.Get().(T)
	return v
}

// Put returns v to the pool.
func (p *Pool[T]) Put(v T) { p.p.Put(v) }
