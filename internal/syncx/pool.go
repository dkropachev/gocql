// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package syncx holds typed wrappers over the sync package.
package syncx

import "sync"

// Pool is a sync.Pool of T. T must be pointer-shaped (a pointer, chan, map or
// func); any other T is copied into an interface on every Put and allocates.
type Pool[T any] struct {
	// New, if set, makes a value when the pool is empty.
	New func() T
	p   sync.Pool
}

// Get returns a pooled value, or New(), or the zero T when New is nil.
func (p *Pool[T]) Get() T {
	if v := p.p.Get(); v != nil {
		return v.(T)
	}
	if p.New != nil {
		return p.New()
	}
	var zero T
	return zero
}

// Put returns v to the pool.
func (p *Pool[T]) Put(v T) { p.p.Put(v) }
