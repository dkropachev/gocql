// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"math/rand/v2"
	"testing"

	"github.com/scylladb/gocql/v2/tablets"
)

// TestTabletRangeIndexMatchesTree applies random overlapping updates to a
// placement and checks after each that its incrementally updated index lists
// the tree's ranges in order, in bounded chunks, and finds the same range as
// the tree for tokens in, between and around them.
func TestTabletRangeIndexMatchesTree(t *testing.T) {
	for _, span := range []int64{4, 64, 4096} {
		rng := rand.New(rand.NewPCG(uint64(span), 1))
		var p InternalTabletPlacement
		for step := 0; step < 4000; step++ {
			// Narrow updates fill hundreds of ranges over several chunks; every
			// 31st replaces up to a few chunks' worth at once.
			first := rng.Int64N(span*512) - span*256
			last := first + 1 + rng.Int64N(span)
			if step%31 == 0 {
				last = first + 1 + rng.Int64N(span*64)
			}
			h, err := tablets.NewTabletInfo("ks", "table", first, last, []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{15: byte(step)}, 0)})
			if err != nil {
				t.Fatal(err)
			}
			p, _ = InternalUpdateTabletPlacement(p, h)
			if !p.currentIndex() {
				t.Fatalf("span %d step %d: index is stale", span, step)
			}
			checkTabletRangeIndex(t, p, rng, span)
		}
		if len(p.index.chunks) < 4 {
			t.Fatalf("span %d: only %d chunks", span, len(p.index.chunks))
		}
		full := InternalIndexTabletPlacement(InternalTabletPlacement{InternalRanges: p.InternalRanges})
		checkTabletRangeIndex(t, full, rng, span)
	}
}

func checkTabletRangeIndex(t *testing.T, p InternalTabletPlacement, rng *rand.Rand, span int64) {
	t.Helper()
	var want []*InternalTabletRange
	InternalPlacementEach(p.InternalRanges, func(_ int64, r *InternalTabletRange) { want = append(want, r) })
	var got []*InternalTabletRange
	if x := p.index; x != nil {
		if len(x.last) != len(x.chunks) {
			t.Fatalf("%d chunk ends for %d chunks", len(x.last), len(x.chunks))
		}
		for i, c := range x.chunks {
			if len(c.ends) == 0 || len(c.ends) > tabletChunkMax || len(c.ends) != len(c.ranges) || x.last[i] != c.ends[len(c.ends)-1] {
				t.Fatalf("chunk %d malformed: %d ends, %d ranges", i, len(c.ends), len(c.ranges))
			}
			for j, r := range c.ranges {
				if c.ends[j] != r.InternalSpan.End {
					t.Fatalf("chunk %d entry %d: end %d for range ending %d", i, j, c.ends[j], r.InternalSpan.End)
				}
			}
			got = append(got, c.ranges...)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("index lists %d ranges, tree %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("range %d differs", i)
		}
	}
	probe := func(token int64) {
		var tree *InternalTabletRange
		if n := InternalTabletRangeFor(p.InternalRanges, token); n != nil {
			tree = n.InternalValue
		}
		if r := p.tabletRangeFor(token); r != tree {
			t.Fatalf("token %d: index found %v, tree %v", token, r, tree)
		}
	}
	for _, r := range want {
		probe(r.InternalSpan.Start)
		probe(r.InternalSpan.Start + 1)
		probe(r.InternalSpan.End)
		probe(r.InternalSpan.End + 1)
	}
	for i := 0; i < 16; i++ {
		probe(rng.Int64N(span*600) - span*300)
	}
}
