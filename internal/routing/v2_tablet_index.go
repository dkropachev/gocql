// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import "github.com/scylladb/gocql/v2/tablets"

// tabletChunkMax bounds the ranges of one index chunk, and so what an update
// copies: the chunks it touches and the slice of chunk ends.
const tabletChunkMax = 64

// tabletRangeIndex lists a range tree's ranges in token order, in chunks of
// at most tabletChunkMax, so a lookup binary-searches two short contiguous
// slices instead of chasing tree nodes. Like the tree, it is immutable once
// published; an update copies the chunks it changes.
type tabletRangeIndex struct {
	root *InternalTabletRangeTree
	// last holds each chunk's last range end.
	last   []int64
	chunks []*tabletRangeChunk
}

type tabletRangeChunk struct {
	ends   []int64
	ranges []*InternalTabletRange
}

// currentIndex reports whether t's index lists exactly InternalRanges; a
// placement without ranges needs no index.
func (t *InternalTabletPlacement) currentIndex() bool {
	if t.index == nil {
		return t.InternalRanges == nil
	}
	return t.index.root == t.InternalRanges
}

// InternalIndexTabletPlacement returns t with an index of its current ranges.
func InternalIndexTabletPlacement(t InternalTabletPlacement) InternalTabletPlacement {
	if t.currentIndex() {
		return t
	}
	t.index = nil
	if t.InternalRanges == nil {
		return t
	}
	var ends []int64
	var ranges []*InternalTabletRange
	InternalPlacementEach(t.InternalRanges, func(end int64, r *InternalTabletRange) {
		ends = append(ends, end)
		ranges = append(ranges, r)
	})
	x := &tabletRangeIndex{root: t.InternalRanges}
	x.appendChunks(ends, ranges)
	t.index = x
	return t
}

// appendChunks appends ends and ranges to x in chunks of nearly equal size.
func (x *tabletRangeIndex) appendChunks(ends []int64, ranges []*InternalTabletRange) {
	n := (len(ends) + tabletChunkMax - 1) / tabletChunkMax
	for i := 0; i < n; i++ {
		lo, hi := i*len(ends)/n, (i+1)*len(ends)/n
		c := &tabletRangeChunk{ends: ends[lo:hi:hi], ranges: ranges[lo:hi:hi]}
		x.last = append(x.last, c.ends[len(c.ends)-1])
		x.chunks = append(x.chunks, c)
	}
}

// InternalUpdateTabletPlacement is InternalUpdateTabletRange for t's ranges,
// keeping t's index current by copying only the chunks the update changes.
func InternalUpdateTabletPlacement(t InternalTabletPlacement, h tablets.TabletInfo) (InternalTabletPlacement, bool) {
	indexed := t.currentIndex()
	root, changed := InternalUpdateTabletRange(t.InternalRanges, h)
	if !changed {
		return t, false
	}
	old := t.index
	t.InternalRanges = root
	if !indexed {
		return InternalIndexTabletPlacement(t), true
	}
	first, last := h.FirstToken(), h.LastToken()
	r := InternalPlacementGet(root, last, InternalComparePlacementToken).InternalValue
	t.index = old.replace(root, first, last, r)
	return t, true
}

// replace returns x without the ranges InternalUpdateTabletRange removes
// for [first, last), those ending after first and starting before last, and
// with r in their place.
func (x *tabletRangeIndex) replace(root *InternalTabletRangeTree, first, last int64, r *InternalTabletRange) *tabletRangeIndex {
	y := &tabletRangeIndex{root: root}
	if x == nil || len(x.chunks) == 0 {
		y.appendChunks([]int64{last}, []*InternalTabletRange{r})
		return y
	}
	// The first range ending after first is at (ci, j); a range ending
	// after every range goes at the end of the last chunk.
	ci := upperBound(x.last, first)
	var j int
	if ci == len(x.chunks) {
		ci--
		j = len(x.chunks[ci].ends)
	} else {
		j = upperBound(x.chunks[ci].ends, first)
	}
	// Removed ranges run from (ci, j) up to the first range starting at or
	// after last, at (cj, k); cj == len(x.chunks) when none does.
	cj, k := ci, j
	for cj < len(x.chunks) {
		c := x.chunks[cj]
		for k < len(c.ends) && c.ranges[k].InternalSpan.Start < last {
			k++
		}
		if k < len(c.ends) {
			break
		}
		if cj == len(x.chunks)-1 {
			break
		}
		cj, k = cj+1, 0
	}
	head, tail := x.chunks[ci], x.chunks[cj]
	n := j + 1 + len(tail.ends) - k
	ends := make([]int64, 0, n)
	ranges := make([]*InternalTabletRange, 0, n)
	ends = append(append(append(ends, head.ends[:j]...), last), tail.ends[k:]...)
	ranges = append(append(append(ranges, head.ranges[:j]...), r), tail.ranges[k:]...)
	y.last = make([]int64, 0, len(x.last)+2)
	y.chunks = make([]*tabletRangeChunk, 0, len(x.chunks)+2)
	y.last = append(y.last, x.last[:ci]...)
	y.chunks = append(y.chunks, x.chunks[:ci]...)
	y.appendChunks(ends, ranges)
	y.last = append(y.last, x.last[cj+1:]...)
	y.chunks = append(y.chunks, x.chunks[cj+1:]...)
	return y
}

// upperBound returns the index of the first of the sorted s greater than v.
func upperBound(s []int64, v int64) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s[mid] <= v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// lowerBound returns the index of the first of the sorted s not less than v.
func lowerBound(s []int64, v int64) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s[mid] < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// guessLowerBound is lowerBound for s spread about evenly over its span,
// as a table's tablets are: it looks next to where v would sit if s were
// evenly spread, and binary-searches the rest only when v is not there.
func guessLowerBound(s []int64, v int64) int {
	n := len(s)
	if n < 4 {
		return lowerBound(s, v)
	}
	lo, hi := s[0], s[n-1]
	if v <= lo {
		return 0
	}
	if v > hi {
		return n
	}
	// lo < v <= hi, so the result is in [1, n-1]; g is about the last
	// element below v.
	g := int(float64(uint64(v)-uint64(lo)) / float64(uint64(hi)-uint64(lo)) * float64(n-1))
	g = min(max(g, 0), n-2)
	if s[g] < v {
		if s[g+1] >= v {
			return g + 1
		}
		return g + 2 + lowerBound(s[g+2:], v)
	}
	if s[g-1] < v {
		return g
	}
	return lowerBound(s[:g-1], v)
}

// tabletRangeFor is InternalTabletRangeFor over t, using its index when current.
func (t *InternalTabletPlacement) tabletRangeFor(token int64) *InternalTabletRange {
	x := t.index
	if x == nil || x.root != t.InternalRanges {
		if n := InternalTabletRangeFor(t.InternalRanges, token); n != nil {
			return n.InternalValue
		}
		return nil
	}
	ci := guessLowerBound(x.last, token)
	if ci == len(x.chunks) {
		return nil
	}
	c := x.chunks[ci]
	if r := c.ranges[guessLowerBound(c.ends, token)]; token > r.InternalSpan.Start {
		return r
	}
	return nil
}
