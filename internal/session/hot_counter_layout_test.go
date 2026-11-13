// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"testing"
	"unsafe"
)

// apart reports whether no cache line can hold bytes of both fields, whatever
// the alignment of the struct that holds them: the gap between them is at
// least one line.
func apart(aOff, aSize, bOff, bSize uintptr) bool {
	if aOff > bOff {
		aOff, aSize, bOff, bSize = bOff, bSize, aOff, aSize
	}
	return bOff >= aOff+aSize+cacheLineSize
}

// TestHotCounterLayout checks that the request id, written by every request,
// does not share a cache line with the read-mostly state every request reads,
// nor with the metrics counters, written by every request too.
func TestHotCounterLayout(t *testing.T) {
	var rt InternalSessionRuntime
	id, idSize := unsafe.Offsetof(rt.requestID), unsafe.Sizeof(rt.requestID)
	for _, field := range []struct {
		name         string
		offset, size uintptr
	}{
		{"config", unsafe.Offsetof(rt.config), unsafe.Sizeof(rt.config)},
		{"defaultOptions", unsafe.Offsetof(rt.defaultOptions), unsafe.Sizeof(rt.defaultOptions)},
		{"attempts", unsafe.Offsetof(rt.attempts), unsafe.Sizeof(rt.attempts)},
		{"inflight", unsafe.Offsetof(rt.inflight), unsafe.Sizeof(rt.inflight)},
	} {
		if !apart(id, idSize, field.offset, field.size) {
			t.Errorf("requestID @%d shares a cache line with %s @%d..%d", id, field.name, field.offset, field.offset+field.size)
		}
	}
}
