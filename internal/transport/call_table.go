// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"iter"
	"sync/atomic"
)

const (
	callChunkBits = 8
	callChunkSize = 1 << callChunkBits
	callChunkMask = callChunkSize - 1
	callChunks    = 32768 >> callChunkBits
)

type callChunk [callChunkSize]atomic.Pointer[InternalCallReq]

// InternalCallTable maps stream IDs to in-flight calls without a lock. Each
// stream has one atomic slot; chunks of 256 slots are allocated on first use
// and published with a CAS, so a connection pays only for the stream range it
// has actually issued.
//
// A slot changes hands only by CAS: register (nil -> call) after the stream
// is allocated, and remove (call -> nil) by whoever takes the call over: the
// reader delivering its response, a caller withdrawing an unsent request, or
// close draining the table. Exactly one of them wins, and the winner owns the
// call's cleanup. Removal by identity keeps a stale owner from removing a
// reused stream's new call.
type InternalCallTable struct {
	chunks [callChunks]atomic.Pointer[callChunk]
}

func InternalNewCallTable() *InternalCallTable { return &InternalCallTable{} }

// slot returns stream's slot, or nil if its chunk was never allocated.
func (t *InternalCallTable) slot(stream int) *atomic.Pointer[InternalCallReq] {
	if t == nil || uint(stream) >= callChunks*callChunkSize {
		return nil
	}
	chunk := t.chunks[stream>>callChunkBits].Load()
	if chunk == nil {
		return nil
	}
	return &chunk[stream&callChunkMask]
}

// slotAlloc returns stream's slot, allocating its chunk if needed.
func (t *InternalCallTable) slotAlloc(stream int) *atomic.Pointer[InternalCallReq] {
	dir := &t.chunks[stream>>callChunkBits]
	chunk := dir.Load()
	if chunk == nil {
		chunk = t.newChunk(dir)
	}
	return &chunk[stream&callChunkMask]
}

//go:noinline
func (t *InternalCallTable) newChunk(dir *atomic.Pointer[callChunk]) *callChunk {
	chunk := new(callChunk)
	if dir.CompareAndSwap(nil, chunk) {
		return chunk
	}
	return dir.Load()
}

// register installs call for stream if the slot is empty.
func (t *InternalCallTable) register(stream int, call *InternalCallReq) bool {
	return t.slotAlloc(stream).CompareAndSwap(nil, call)
}

// remove takes call out of stream's slot if it is still registered there.
func (t *InternalCallTable) remove(stream int, call *InternalCallReq) bool {
	slot := t.slot(stream)
	return slot != nil && slot.CompareAndSwap(call, nil)
}

// drain removes every registered call and yields each one it took.
func (t *InternalCallTable) drain(yield func(*InternalCallReq)) {
	if t == nil {
		return
	}
	for i := range t.chunks {
		chunk := t.chunks[i].Load()
		if chunk == nil {
			continue
		}
		for j := range chunk {
			if chunk[j].Load() == nil {
				continue
			}
			if call := chunk[j].Swap(nil); call != nil {
				yield(call)
			}
		}
	}
}

// Get returns the call registered for stream, or nil.
func (t *InternalCallTable) Get(stream int) *InternalCallReq {
	if slot := t.slot(stream); slot != nil {
		return slot.Load()
	}
	return nil
}

// Set registers call for stream, replacing any existing entry. Connections
// register with a CAS; Set is for tests that arrange a table.
func (t *InternalCallTable) Set(stream int, call *InternalCallReq) {
	if call == nil {
		t.Delete(stream)
		return
	}
	t.slotAlloc(stream).Store(call)
}

// Delete removes and returns the call registered for stream, if any.
func (t *InternalCallTable) Delete(stream int) *InternalCallReq {
	if slot := t.slot(stream); slot != nil {
		return slot.Swap(nil)
	}
	return nil
}

// Len counts the registered calls. It scans the table and is not a snapshot
// while calls register or leave.
func (t *InternalCallTable) Len() int {
	n := 0
	for range t.All() {
		n++
	}
	return n
}

// All yields every registered (stream, call) pair in stream order.
func (t *InternalCallTable) All() iter.Seq2[int, *InternalCallReq] {
	return func(yield func(int, *InternalCallReq) bool) {
		if t == nil {
			return
		}
		for i := range t.chunks {
			chunk := t.chunks[i].Load()
			if chunk == nil {
				continue
			}
			for j := range chunk {
				if call := chunk[j].Load(); call != nil && !yield(i<<callChunkBits|j, call) {
					return
				}
			}
		}
	}
}
