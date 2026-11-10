// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package tablets

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkCowTabletList(b *testing.B) {
	// BenchmarkCowTabletList/FindReplicasUnsafeForToken measures the pure lookup+replica-return
	// path for a prepopulated CowTabletList.
	b.Run("FindReplicasUnsafeForToken", func(b *testing.B) {
		for _, numTablets := range []int{1500, 10000} {
			b.Run(fmt.Sprintf("Tablets%d", numTablets), func(b *testing.B) {
				const rf = 3
				const hostsCount = 6
				hosts := GenerateHostUUIDs(hostsCount)
				tl := NewCowTabletList()
				defer tl.Close()

				tl.BulkAddTablets(createTablets("ks", "tbl", hosts, rf, numTablets, int64(numTablets)))
				tl.Flush()
				runtime.GC()
				b.ResetTimer()
				b.ReportAllocs()

				rnd := getThreadSafeRnd()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						token := rnd.Int63()
						replicas := tl.FindReplicasUnsafeForToken("ks", "tbl", token)
						if len(replicas) != rf {
							// Token may fall in a gap; that's fine for benchmarking.
							_ = replicas
						}
					}
				})
			})
		}
	})

	b.Run("Suite", func(b *testing.B) {
		const (
			rf = 3
		)
		b.Run("Parallel-10", func(b *testing.B) {
			runCowTabletListTestSuit(b, "ManyTables", 6, 10, rf, 1500, 5)
			runCowTabletListTestSuit(b, "SingleTable", 6, 10, rf, 1500, 0)
		})

		b.Run("SingleThread", func(b *testing.B) {
			runCowTabletListTestSuit(b, "ManyTables", 6, 1, rf, 1500, 5)
			runCowTabletListTestSuit(b, "SingleTable", 6, 1, rf, 1500, 0)
		})
	})
}
