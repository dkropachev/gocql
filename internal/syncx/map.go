// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package syncx

import (
	"iter"
	"sync"
)

// Map is a sync.Map from K to V.
type Map[K comparable, V any] struct{ m sync.Map }

// Load returns the value stored for k.
func (m *Map[K, V]) Load(k K) (V, bool) {
	v, ok := m.m.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	x, _ := v.(V) // a nil interface value fails the assertion; zero V is nil
	return x, true
}

// LoadOrStore returns the existing value for k, or stores and returns v.
func (m *Map[K, V]) LoadOrStore(k K, v V) (V, bool) {
	actual, loaded := m.m.LoadOrStore(k, v)
	x, _ := actual.(V)
	return x, loaded
}

// Store sets the value for k.
func (m *Map[K, V]) Store(k K, v V) { m.m.Store(k, v) }

// Delete removes k.
func (m *Map[K, V]) Delete(k K) { m.m.Delete(k) }

// All iterates like sync.Map.Range.
func (m *Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.m.Range(func(k, v any) bool {
			kk, _ := k.(K)
			vv, _ := v.(V)
			return yield(kk, vv)
		})
	}
}
