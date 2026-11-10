// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/scylladb/gocql/v2/internal/tablets"
)

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

func TestTabletIndex(t *testing.T) {
	t.Run("RangeIndex", func(t *testing.T) {
		// TestTabletIndex/RangeIndex/MatchesTree applies random overlapping updates to a
		// placement and checks after each that its incrementally updated index lists
		// the tree's ranges in order, in bounded chunks, and finds the same range as
		// the tree for tokens in, between and around them.
		t.Run("MatchesTree", func(t *testing.T) {
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
		})
	})

	// TestTabletIndex/GuessLowerBound checks guessLowerBound against lowerBound over evenly
	// spread, clustered and random sorted slices spanning the whole token ring,
	// at, next to and between their elements.
	t.Run("GuessLowerBound", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(7, 7))
		for _, n := range []int{1, 2, 3, 4, 5, 64, 1000} {
			even := make([]int64, n)
			step := uint64(math.MaxUint64) / uint64(n)
			for i := range even {
				even[i] = math.MinInt64 + int64(uint64(i+1)*step-1)
			}
			clustered := make([]int64, n)
			for i := range clustered {
				clustered[i] = int64(i)
			}
			clustered[n-1] = math.MaxInt64
			random := make([]int64, 0, n)
			seen := map[int64]bool{}
			for len(random) < n {
				if v := int64(rng.Uint64()); !seen[v] {
					seen[v] = true
					random = append(random, v)
				}
			}
			slices.Sort(random)
			for _, s := range [][]int64{even, clustered, random} {
				probes := []int64{math.MinInt64, math.MaxInt64, 0}
				for _, e := range s {
					probes = append(probes, e-1, e, e+1, int64(rng.Uint64()))
				}
				for _, v := range probes {
					if got, want := guessLowerBound(s, v), lowerBound(s, v); got != want {
						t.Fatalf("n=%d v=%d: got %d, want %d", n, v, got, want)
					}
				}
			}
		}
	})

	// TestTabletIndex/LookupTabletCached checks that a cached lookup finds what an uncached
	// one does as the placement tree is replaced: by a tablet update, by another
	// table's placement, and by the table's removal.
	t.Run("LookupTabletCached", func(t *testing.T) {
		table := InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "table"}
		other := InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "other"}
		hint := func(first, last int64, host byte) tablets.TabletInfo {
			h, err := tablets.NewTabletInfo("ks", "table", first, last, []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{15: host}, 0)})
			if err != nil {
				t.Fatal(err)
			}
			return h
		}
		var c InternalPlacementCache
		var roots []*InternalTabletPlacementTree
		var p InternalTabletPlacement
		roots = append(roots, nil)
		for i := int64(0); i < 4; i++ {
			p, _ = InternalUpdateTabletPlacement(p, hint(i*100, i*100+100, byte(i)))
			roots = append(roots, InternalPlacementPut(roots[len(roots)-1], table, p, InternalComparePlacementTable))
		}
		roots = append(roots, InternalPlacementPut(roots[len(roots)-1], other, InternalTabletPlacement{}, InternalComparePlacementTable))
		roots = append(roots, placementDelete(roots[len(roots)-1], table, InternalComparePlacementTable))
		// Revisit earlier trees as well, as snapshots taken before an update do.
		order := []int{0, 1, 1, 2, 4, 3, 5, 6, 2, 6}
		for _, i := range order {
			m := NewMetadataSnapshot(SnapshotState{InternalPlacements: roots[i]})
			for _, token := range []int64{-1, 0, 1, 50, 100, 150, 250, 399, 400, 401} {
				info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Value: token, Set: true}}
				if got, want := InternalLookupTabletCached(m, info, &c), InternalLookupTablet(m, info); got != want {
					t.Fatalf("tree %d token %d: cached lookup %+v, uncached %+v", i, token, got, want)
				}
			}
		}
	})
}
