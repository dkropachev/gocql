// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package tablets

import (
	"math"
	"testing"

	"github.com/scylladb/gocql/v2/internal/tests"
)

func TestTabletEntries(t *testing.T) {
	t.Parallel()

	t.Run("AddEntry", func(t *testing.T) {
		t.Run("Overlaps", func(t *testing.T) {
			t.Parallel()

			t.Run("Empty", func(t *testing.T) {
				tl := TabletEntryList{}
				tl = tl.addEntry(TabletEntry{
					firstToken: -100, lastToken: 100,
				})

				tests.AssertEqual(t, "length", 1, len(tl))
				tests.AssertEqual(t, "firstToken", int64(-100), tl[0].firstToken)
				tests.AssertEqual(t, "lastToken", int64(100), tl[0].lastToken)
			})

			t.Run("Beginning", func(t *testing.T) {
				tl := TabletEntryList{{
					firstToken: 100, lastToken: 200,
				}}
				tl = tl.addEntry(TabletEntry{
					firstToken: -200, lastToken: -100,
				})

				tests.AssertEqual(t, "length", 2, len(tl))
				tests.AssertTrue(t, "sorted", compareEntryRanges(tl, [][]int64{{-200, -100}, {100, 200}}))
			})

			t.Run("End", func(t *testing.T) {
				tl := TabletEntryList{{
					firstToken: -200, lastToken: -100,
				}}
				tl = tl.addEntry(TabletEntry{
					firstToken: 100, lastToken: 200,
				})

				tests.AssertEqual(t, "length", 2, len(tl))
				tests.AssertTrue(t, "sorted", compareEntryRanges(tl, [][]int64{{-200, -100}, {100, 200}}))
			})

			t.Run("Overlap", func(t *testing.T) {
				tl := TabletEntryList{
					{firstToken: -300, lastToken: -200},
					{firstToken: -200, lastToken: -100},
					{firstToken: -100, lastToken: 0},
					{firstToken: 0, lastToken: 100},
				}
				tl = tl.addEntry(TabletEntry{
					firstToken: -150, lastToken: 50,
				})

				tests.AssertTrue(t, "overlap resolved",
					compareEntryRanges(tl, [][]int64{{-300, -200}, {-150, 50}}))
			})

			t.Run("NewTabletContainedWithinExisting", func(t *testing.T) {
				entries := TabletEntryList{
					{firstToken: -300, lastToken: 300},
				}
				result := entries.addEntry(TabletEntry{firstToken: -100, lastToken: 100})
				if len(result) != 1 {
					t.Errorf("expected 1 tablet after replacement, got %d", len(result))
				}
				if len(result) == 1 {
					tests.AssertEqual(t, "tablet firstToken", int64(-100), result[0].firstToken)
					tests.AssertEqual(t, "tablet lastToken", int64(100), result[0].lastToken)
				}
			})

			t.Run("NewTabletContainsMultiple", func(t *testing.T) {
				entries := TabletEntryList{
					{firstToken: -200, lastToken: -100},
					{firstToken: -100, lastToken: 0},
					{firstToken: 0, lastToken: 100},
				}
				result := entries.addEntry(TabletEntry{firstToken: -300, lastToken: 200})
				if len(result) != 1 {
					t.Errorf("expected consolidation to 1 tablet, got %d", len(result))
				}
				if len(result) == 1 {
					tests.AssertEqual(t, "tablet firstToken", int64(-300), result[0].firstToken)
					tests.AssertEqual(t, "tablet lastToken", int64(200), result[0].lastToken)
				}
			})

			t.Run("MultiplePartialOverlaps", func(t *testing.T) {
				entries := TabletEntryList{
					{firstToken: -300, lastToken: -200},
					{firstToken: -100, lastToken: 0},
					{firstToken: 100, lastToken: 200},
				}
				result := entries.addEntry(TabletEntry{firstToken: -150, lastToken: 150})
				if len(result) != 2 {
					t.Errorf("expected 2 tablets after partial overlap, got %d", len(result))
				}
				if len(result) == 2 {
					tests.AssertEqual(t, "first tablet firstToken", int64(-300), result[0].firstToken)
					tests.AssertEqual(t, "first tablet lastToken", int64(-200), result[0].lastToken)
					tests.AssertEqual(t, "second tablet firstToken", int64(-150), result[1].firstToken)
					tests.AssertEqual(t, "second tablet lastToken", int64(150), result[1].lastToken)
				}
			})
		})

		t.Run("Bounds", func(t *testing.T) {
			t.Parallel()

			t.Run("SingleTokenTablet", func(t *testing.T) {
				tl := TabletEntryList{}
				tl = tl.addEntry(TabletEntry{firstToken: 41, lastToken: 42})
				if len(tl) != 1 || tl[0].firstToken != 41 || tl[0].lastToken != 42 {
					t.Fatalf("expected single (41,42] entry, got %v", tl)
				}

				tl = TabletEntryList{
					{firstToken: -100, lastToken: -50},
					{firstToken: 100, lastToken: 200},
				}
				tl = tl.addEntry(TabletEntry{firstToken: 41, lastToken: 42})
				if len(tl) != 3 {
					t.Fatalf("expected 3 entries, got %d", len(tl))
				}
				if tl[1].firstToken != 41 || tl[1].lastToken != 42 {
					t.Fatalf("expected middle entry (41,42], got [%d,%d]", tl[1].firstToken, tl[1].lastToken)
				}
			})
		})
	})

	t.Run("BulkAddEntries", func(t *testing.T) {
		t.Parallel()

		t.Run("Empty", func(t *testing.T) {
			tl := TabletEntryList{}
			batch := TabletEntryList{
				{firstToken: -200, lastToken: -100},
				{firstToken: -100, lastToken: 0},
				{firstToken: 0, lastToken: 100},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertEqual(t, "length", 3, len(tl))
			tests.AssertTrue(t, "ranges", compareEntryRanges(tl, [][]int64{{-200, -100}, {-100, 0}, {0, 100}}))
		})

		t.Run("Overlap", func(t *testing.T) {
			tl := TabletEntryList{
				{firstToken: -400, lastToken: -300},
				{firstToken: -300, lastToken: -200},
				{firstToken: -200, lastToken: -100},
				{firstToken: 100, lastToken: 200},
			}
			batch := TabletEntryList{
				{firstToken: -350, lastToken: -250},
				{firstToken: -250, lastToken: -150},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertTrue(t, "overlap resolved",
				compareEntryRanges(tl, [][]int64{{-350, -250}, {-250, -150}, {100, 200}}))
		})

		t.Run("IntraBatchOverlappingPair", func(t *testing.T) {
			tl := TabletEntryList{}
			batch := TabletEntryList{
				{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
				{firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertEqual(t, "length", 1, len(tl))
			tests.AssertEqual(t, "firstToken", int64(50), tl[0].firstToken)
			tests.AssertEqual(t, "lastToken", int64(150), tl[0].lastToken)
			tests.AssertEqual(t, "host", testHostUUID("h2"), tl[0].replicas[0].hostId)
		})

		t.Run("IntraBatchOverlappingTriple", func(t *testing.T) {
			tl := TabletEntryList{}
			batch := TabletEntryList{
				{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
				{firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
				{firstToken: 100, lastToken: 200, replicas: []ReplicaInfo{{testHostUUID("h3"), 0}}},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertEqual(t, "length", 1, len(tl))
			tests.AssertEqual(t, "firstToken", int64(100), tl[0].firstToken)
			tests.AssertEqual(t, "lastToken", int64(200), tl[0].lastToken)
		})

		t.Run("IntraBatchOverlappingWithExistingList", func(t *testing.T) {
			tl := TabletEntryList{
				{firstToken: -500, lastToken: -400},
				{firstToken: 500, lastToken: 600},
			}
			batch := TabletEntryList{
				{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
				{firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertEqual(t, "length", 3, len(tl))
			tests.AssertEqual(t, "existing-pre firstToken", int64(-500), tl[0].firstToken)
			tests.AssertEqual(t, "resolved firstToken", int64(50), tl[1].firstToken)
			tests.AssertEqual(t, "resolved lastToken", int64(150), tl[1].lastToken)
			tests.AssertEqual(t, "existing-post firstToken", int64(500), tl[2].firstToken)
		})

		t.Run("NonOverlappingBatchStillWorks", func(t *testing.T) {
			tl := TabletEntryList{}
			batch := TabletEntryList{
				{firstToken: 0, lastToken: 100},
				{firstToken: 100, lastToken: 200},
				{firstToken: 200, lastToken: 300},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertEqual(t, "length", 3, len(tl))
			tests.AssertTrue(t, "ranges", compareEntryRanges(tl, [][]int64{{0, 100}, {100, 200}, {200, 300}}))
		})

		t.Run("BatchWithGapsPreservesExisting", func(t *testing.T) {
			tl := TabletEntryList{
				{firstToken: 200, lastToken: 300, replicas: []ReplicaInfo{{testHostUUID("existing"), 0}}},
			}
			batch := TabletEntryList{
				{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
				{firstToken: 500, lastToken: 600, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
			}
			tl = tl.bulkAddEntries(batch)

			tests.AssertEqual(t, "length", 3, len(tl))
			tests.AssertTrue(t, "ranges", compareEntryRanges(tl, [][]int64{{0, 100}, {200, 300}, {500, 600}}))
			tests.AssertEqual(t, "existing host preserved", testHostUUID("existing"), tl[1].replicas[0].hostId)
		})
	})

	t.Run("FindEntryForToken", func(t *testing.T) {
		t.Parallel()

		t.Run("ExactLastToken", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -100, lastToken: 0},
				{firstToken: 0, lastToken: 100},
			}
			entry, ok := entries.findEntryForToken(0, 0, len(entries))
			if !ok {
				t.Fatal("expected entry for token at exact lastToken boundary")
			}
			if entry.lastToken != 0 {
				t.Fatalf("expected lastToken=0, got %d", entry.lastToken)
			}
		})

		t.Run("ExactFirstToken", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -100, lastToken: 0},
				{firstToken: 0, lastToken: 100},
			}
			if _, ok := entries.findEntryForToken(-100, 0, len(entries)); ok {
				t.Fatal("left-open tablet includes firstToken")
			}
			entry, ok := entries.findEntryForToken(-99, 0, len(entries))
			if !ok || entry.firstToken != -100 {
				t.Fatal("first contained token missing")
			}

		})

		t.Run("BeyondAll", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -100, lastToken: 0},
				{firstToken: 0, lastToken: 100},
			}
			_, ok := entries.findEntryForToken(200, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token beyond all tablets")
			}
		})

		t.Run("BeforeAll", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -100, lastToken: 0},
				{firstToken: 0, lastToken: 100},
			}
			_, ok := entries.findEntryForToken(-200, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token before all tablets")
			}
		})

		t.Run("InGap", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -200, lastToken: -100},
				{firstToken: 100, lastToken: 200},
			}
			_, ok := entries.findEntryForToken(0, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token in gap between non-contiguous tablets")
			}
		})

		t.Run("EmptyList", func(t *testing.T) {
			entries := TabletEntryList{}
			_, ok := entries.findEntryForToken(0, 0, 0)
			if ok {
				t.Fatal("expected nil for empty entry list")
			}
		})

		t.Run("SingleEntry", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -50, lastToken: 50},
			}

			_, ok := entries.findEntryForToken(0, 0, len(entries))
			if !ok {
				t.Fatal("expected entry for token inside single entry")
			}

			_, ok = entries.findEntryForToken(-50, 0, len(entries))
			if ok {
				t.Fatal("left-open single entry includes firstToken")
			}

			_, ok = entries.findEntryForToken(50, 0, len(entries))
			if !ok {
				t.Fatal("expected entry for token at lastToken of single entry")
			}

			_, ok = entries.findEntryForToken(-51, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token before single entry")
			}

			_, ok = entries.findEntryForToken(51, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token after single entry")
			}
		})

		t.Run("InvalidBounds", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("host1"), 0}}},
			}

			testCases := []struct {
				name string
				l, r int
			}{
				{"negative l", -1, 1},
				{"r beyond length", 0, 10},
				{"l > r", 1, 0},
				{"both invalid", -1, 10},
				{"l == r (empty range)", 0, 0},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result, ok := entries.findEntryForToken(50, tc.l, tc.r)
					if ok {
						t.Errorf("expected nil for invalid bounds l=%d r=%d, got %+v", tc.l, tc.r, result)
					}
				})
			}
		})

		t.Run("SingleTokenTablet", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -100, lastToken: -50},
				{firstToken: 41, lastToken: 42},
				{firstToken: 100, lastToken: 200},
			}

			entry, ok := entries.findEntryForToken(42, 0, len(entries))
			if !ok {
				t.Fatal("expected entry for single-token tablet")
			}
			if entry.firstToken != 41 || entry.lastToken != 42 {
				t.Fatalf("expected (41,42], got [%d,%d]", entry.firstToken, entry.lastToken)
			}

			_, ok = entries.findEntryForToken(41, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token just before single-token tablet")
			}

			_, ok = entries.findEntryForToken(43, 0, len(entries))
			if ok {
				t.Fatal("expected nil for token just after single-token tablet")
			}
		})
	})

	t.Run("FindOverlapRange", func(t *testing.T) {
		t.Parallel()

		t.Run("ContiguousBoundary", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("host1"), 0}}},
				{firstToken: 200, lastToken: 300, replicas: []ReplicaInfo{{testHostUUID("host2"), 1}}},
			}

			start, tailStart := entries.findOverlapRange(100, 200)

			if start != 1 {
				t.Errorf("expected start=1 for contiguous boundary, got %d", start)
			}
			if tailStart != 1 {
				t.Errorf("expected tailStart=1 for contiguous boundary, got %d", tailStart)
			}
		})

		t.Run("ExtremeValues", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: math.MinInt64, lastToken: 0, replicas: []ReplicaInfo{{testHostUUID("host1"), 0}}},
				{firstToken: 0, lastToken: math.MaxInt64, replicas: []ReplicaInfo{{testHostUUID("host2"), 1}}},
			}

			start, tailStart := entries.findOverlapRange(math.MinInt64, math.MaxInt64)
			if start != 0 {
				t.Errorf("expected start=0 for full range overlap, got %d", start)
			}
			if tailStart != 2 {
				t.Errorf("expected tailStart=2 for full range overlap, got %d", tailStart)
			}

			start, tailStart = entries.findOverlapRange(math.MinInt64, -100)
			if start != 0 {
				t.Errorf("expected start=0 for MinInt64 range, got %d", start)
			}
			if tailStart != 1 {
				t.Errorf("expected tailStart=1 for MinInt64 range, got %d", tailStart)
			}

			start, tailStart = entries.findOverlapRange(100, math.MaxInt64)
			if start != 1 {
				t.Errorf("expected start=1 for MaxInt64 range, got %d", start)
			}
			if tailStart != 2 {
				t.Errorf("expected tailStart=2 for MaxInt64 range, got %d", tailStart)
			}
		})

		t.Run("SingleEntry", func(t *testing.T) {
			entries := TabletEntryList{
				{firstToken: -100, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("host1"), 0}}},
			}

			start, tailStart := entries.findOverlapRange(-50, 50)
			if start != 0 || tailStart != 1 {
				t.Errorf("expected start=0 tailStart=1 for overlapping range, got start=%d tailStart=%d", start, tailStart)
			}

			start, tailStart = entries.findOverlapRange(-200, 200)
			if start != 0 || tailStart != 1 {
				t.Errorf("expected start=0 tailStart=1 for extended range, got start=%d tailStart=%d", start, tailStart)
			}

			start, tailStart = entries.findOverlapRange(-200, -150)
			if start != 0 || tailStart != 0 {
				t.Errorf("expected start=0 tailStart=0 for range before, got start=%d tailStart=%d", start, tailStart)
			}

			start, tailStart = entries.findOverlapRange(150, 200)
			if start != 1 || tailStart != 1 {
				t.Errorf("expected start=1 tailStart=1 for range after, got start=%d tailStart=%d", start, tailStart)
			}

			start, tailStart = entries.findOverlapRange(-100, -50)
			if start != 0 || tailStart != 1 {
				t.Errorf("expected start=0 tailStart=1 for range sharing firstToken, got start=%d tailStart=%d", start, tailStart)
			}

			start, tailStart = entries.findOverlapRange(100, 200)
			if start != 1 || tailStart != 1 {
				t.Errorf("expected start=1 tailStart=1 for contiguous range at lastToken, got start=%d tailStart=%d", start, tailStart)
			}
		})

		t.Run("SingleTokenTablet", func(t *testing.T) {
			entries := TabletEntryList{}
			start, tailStart := entries.findOverlapRange(42, 42)
			if start != 0 || tailStart != 0 {
				t.Errorf("empty list: expected start=0 tailStart=0, got start=%d tailStart=%d", start, tailStart)
			}

			entries = TabletEntryList{
				{firstToken: 40, lastToken: 50},
			}
			start, tailStart = entries.findOverlapRange(42, 42)
			if start != 0 || tailStart != 1 {
				t.Errorf("contained: expected start=0 tailStart=1, got start=%d tailStart=%d", start, tailStart)
			}

			entries = TabletEntryList{
				{firstToken: 0, lastToken: 42},
			}
			start, tailStart = entries.findOverlapRange(42, 42)
			if start != 1 || tailStart != 1 {
				t.Errorf("adjacent: expected start=1 tailStart=1, got start=%d tailStart=%d", start, tailStart)
			}
		})
	})
}
