// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"testing"
	"unsafe"
)

// TestHotCounterLayout checks that recording a latency, done by every
// request, writes no cache line that holds the placement epoch every request
// reads, and that consecutive requests write different lines of the ring.
func TestHotCounterLayout(t *testing.T) {
	var p InternalV2PeripheralState
	ring, ringSize := unsafe.Offsetof(p.latency), unsafe.Sizeof(p.latency)
	for _, field := range []struct {
		name   string
		offset uintptr
	}{
		{"InternalPlacementEpoch", unsafe.Offsetof(p.InternalPlacementEpoch)},
		{"InternalSchemaReconciled", unsafe.Offsetof(p.InternalSchemaReconciled)},
		{"schemaGeneration", unsafe.Offsetof(p.schemaGeneration)},
	} {
		if field.offset+8+64 > ring && ring+ringSize+64 > field.offset {
			t.Errorf("latency ring @%d..%d is within 64 bytes of %s @%d", ring, ring+ringSize, field.name, field.offset)
		}
	}
	seen := make(map[uint64]bool, v2LatencySlots)
	for id := uint64(1); id <= v2LatencySlots; id++ {
		slot := v2LatencySlot(id)
		if slot >= v2LatencySlots || seen[slot] {
			t.Fatalf("id %d maps to slot %d, out of range or taken", id, slot)
		}
		seen[slot] = true
		if gap := int(slot) - int(v2LatencySlot(id+1)); gap > -8 && gap < 8 {
			t.Fatalf("ids %d and %d write slots %d and %d, one cache line apart", id, id+1, slot, v2LatencySlot(id+1))
		}
	}
}
