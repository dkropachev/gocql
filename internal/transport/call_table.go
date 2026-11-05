// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import "iter"

const (
	callPageBits = 6
	callPageSize = 1 << callPageBits
	callPageMask = callPageSize - 1
)

type callPage [callPageSize]*InternalCallReq

// InternalCallTable maps stream IDs to in-flight calls. It is guarded by
// Conn.InternalMu. Pages of 64 slots are allocated on first use, so a
// connection pays only for the stream range it has actually issued, and the
// garbage collector scans only those pages.
type InternalCallTable struct {
	pages []*callPage
	n     int
}

func InternalNewCallTable() *InternalCallTable { return &InternalCallTable{} }

// Get returns the call registered for stream, or nil.
func (t *InternalCallTable) Get(stream int) *InternalCallReq {
	if t == nil || stream < 0 {
		return nil
	}
	p := stream >> callPageBits
	if p >= len(t.pages) || t.pages[p] == nil {
		return nil
	}
	return t.pages[p][stream&callPageMask]
}

// Set registers call for stream, replacing any existing entry.
func (t *InternalCallTable) Set(stream int, call *InternalCallReq) {
	if call == nil {
		t.Delete(stream)
		return
	}
	p := stream >> callPageBits
	if p >= len(t.pages) {
		t.pages = append(t.pages, make([]*callPage, p+1-len(t.pages))...)
	}
	page := t.pages[p]
	if page == nil {
		page = new(callPage)
		t.pages[p] = page
	}
	slot := &page[stream&callPageMask]
	if *slot == nil {
		t.n++
	}
	*slot = call
}

// Delete removes and returns the call registered for stream, if any.
func (t *InternalCallTable) Delete(stream int) *InternalCallReq {
	if t == nil || stream < 0 {
		return nil
	}
	p := stream >> callPageBits
	if p >= len(t.pages) || t.pages[p] == nil {
		return nil
	}
	slot := &t.pages[p][stream&callPageMask]
	call := *slot
	if call != nil {
		*slot = nil
		t.n--
	}
	return call
}

// Len reports the number of registered calls.
func (t *InternalCallTable) Len() int {
	if t == nil {
		return 0
	}
	return t.n
}

// All yields every registered (stream, call) pair in stream order.
func (t *InternalCallTable) All() iter.Seq2[int, *InternalCallReq] {
	return func(yield func(int, *InternalCallReq) bool) {
		if t == nil {
			return
		}
		for p, page := range t.pages {
			if page == nil {
				continue
			}
			for i, call := range page {
				if call != nil && !yield(p<<callPageBits|i, call) {
					return
				}
			}
		}
	}
}
