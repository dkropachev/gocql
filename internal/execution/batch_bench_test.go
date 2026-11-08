// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkBatch(b *testing.B) {
	b.Run("AddToBatch", func(b *testing.B) {
		q := testOwnedQuery(b)
		var batch Batch
		value := []byte("batch entry")
		b.ReportAllocs()
		for b.Loop() {
			if err := q.AddToBatch(&batch, value); err != nil {
				b.Fatal(err)
			}
			if batch.Len() == 64 {
				batch.Reset()
			}
		}
	})

	b.Run("Exec", func(b *testing.B) {
		for _, n := range []int{1, 8} {
			b.Run(fmt.Sprint(n), func(b *testing.B) {
				s := batchBenchSession(b)
				q := batchTestQuery(b, s)
				ctx := context.Background()
				value := []byte("0123456789abcdef0123456789abcdef")
				var batch Batch
				b.ReportAllocs()
				for b.Loop() {
					batch.Reset()
					for i := 0; i < n; i++ {
						if err := q.AddToBatch(&batch, value); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := q.ExecBatch(ctx, UnloggedBatch, &batch, Options{}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
}
