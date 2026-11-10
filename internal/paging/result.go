// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package paging

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"bytes"

	"hash/maphash"
)

// pagingHistory owns references to the same immutable tokens sent on the wire.
// An owner with reusable storage (Each's frame, a query's iterator) lends it
// as InternalInline: the first InternalInlineHistory tokens live there and are
// compared directly, so a short paged read allocates nothing. Later tokens,
// and every token of a history without inline storage, go to a map keyed by
// hash. Hash collisions compare full bytes; count and payload budgets are
// independent. This nongeneric state machine is also used by the incremental
// streaming pager.
type InternalPagingHistoryEntry struct {
	InternalNext  *InternalPagingHistoryEntry
	InternalState model.PageState
}

const InternalInlineHistory = 8

type InternalPagingHistory struct {
	InternalEntries  map[uint64]InternalPagingHistoryEntry
	InternalInline   *[InternalInlineHistory]model.PageState
	InternalSeed     maphash.Seed
	InternalCount    int
	InternalBytes    int64
	InternalMaxCount int
	InternalMaxBytes int64
}

func (h *InternalPagingHistory) InternalAdd(state model.PageState) error {
	if !state.More() {
		return nil
	}
	token := model.PageToken(state)
	perfAdd(0, len(token))
	if h.InternalEntries == nil && h.InternalInline != nil {
		// Without a map, InternalCount is the number of inline tokens.
		for _, held := range h.InternalInline[:h.InternalCount] {
			perfAdd(1, 1)
			if bytes.Equal(model.PageToken(held), token) {
				return runtimeerrors.ErrPagingNoProgress
			}
		}
		if err := h.checkLimits(token); err != nil {
			return err
		}
		if h.InternalCount < len(h.InternalInline) {
			h.InternalInline[h.InternalCount] = state
			h.record(token)
			return nil
		}
		h.InternalSpill()
		h.insert(maphash.Bytes(h.InternalSeed, token), state)
		h.record(token)
		return nil
	}
	h.InternalSpill()
	hash := maphash.Bytes(h.InternalSeed, token)
	if entry, exists := h.InternalEntries[hash]; exists {
		perfAdd(1, 1)
		if bytes.Equal(model.PageToken(entry.InternalState), token) {
			return runtimeerrors.ErrPagingNoProgress
		}
		for p := entry.InternalNext; p != nil; p = p.InternalNext {
			perfAdd(1, 1)
			if bytes.Equal(model.PageToken(p.InternalState), token) {
				return runtimeerrors.ErrPagingNoProgress
			}
		}
	}
	if err := h.checkLimits(token); err != nil {
		return err
	}
	h.insert(hash, state)
	h.record(token)
	return nil
}

// InternalSpill moves the inline tokens into the hashed map; later tokens go
// there too.
func (h *InternalPagingHistory) InternalSpill() {
	if h.InternalEntries != nil {
		return
	}
	h.InternalSeed = maphash.MakeSeed()
	h.InternalEntries = make(map[uint64]InternalPagingHistoryEntry)
	if h.InternalInline == nil {
		return
	}
	for i, held := range h.InternalInline[:h.InternalCount] {
		h.insert(maphash.Bytes(h.InternalSeed, model.PageToken(held)), held)
		h.InternalInline[i] = model.PageState{}
	}
}

func (h *InternalPagingHistory) checkLimits(token []byte) error {
	maxCount, maxBytes := h.InternalMaxCount, h.InternalMaxBytes
	if maxCount == 0 {
		maxCount = 65536
	}
	if maxBytes == 0 {
		maxBytes = 16 << 20
	}
	if h.InternalCount >= maxCount || int64(len(token)) > maxBytes-h.InternalBytes {
		return runtimeerrors.ErrPagingStateLimit
	}
	return nil
}

// insert chains state under hash; the caller has ruled out an equal token.
func (h *InternalPagingHistory) insert(hash uint64, state model.PageState) {
	candidate := InternalPagingHistoryEntry{InternalState: state}
	if previous, exists := h.InternalEntries[hash]; exists {
		candidate.InternalNext = &previous
	}
	h.InternalEntries[hash] = candidate
}

func (h *InternalPagingHistory) record(token []byte) {
	perfAdd(2, 1)
	h.InternalCount++
	h.InternalBytes += int64(len(token))
}

func (h *InternalPagingHistory) InternalClear() {
	if h.InternalEntries == nil && h.InternalInline != nil {
		clear(h.InternalInline[:h.InternalCount])
	}
	h.InternalEntries = nil
	h.InternalCount = 0
	h.InternalBytes = 0
}
