// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
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

	b.Run("PreparedAddToBatch", func(b *testing.B) {
		s, _, peers := runtimeTestSession(b, 1)
		defer s.Close()
		defer peers[0].Close()
		p := oneShotPrepare(b, s, peers[0], make(chan oneShotRequest, 1))
		var batch Batch
		b.ReportAllocs()
		for b.Loop() {
			if err := p.AddToBatch(&batch, 42); err != nil {
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
				session := InternalWrapSession(s)
				ctx := context.Background()
				value := []byte("0123456789abcdef0123456789abcdef")
				batch := Batch{Kind: cqltype.UnloggedBatch}
				b.ReportAllocs()
				for b.Loop() {
					batch.Reset()
					for i := 0; i < n; i++ {
						if err := q.AddToBatch(&batch, value); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := session.ExecBatch(ctx, &batch); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})

	// A reused conditional batch whose response has two rows.
	b.Run("Run", func(b *testing.B) {
		s := batchServeSession(b, runtimeRowsBody(nil, "a", "b"))
		q := batchTestQuery(b, s)
		session := InternalWrapSession(s)
		ctx := context.Background()
		type row struct{ V cqltype.Bytes }
		decoder := codec.Row(codec.Column("value", codec.BlobCodec, func(r *row) *cqltype.Bytes { return &r.V }))
		var batch Batch
		if err := q.AddToBatch(&batch, []byte("0123456789abcdef")); err != nil {
			b.Fatal(err)
		}
		var dst row
		b.ReportAllocs()
		for b.Loop() {
			rows, err := session.RunBatch(ctx, &batch, decoder)
			if err != nil {
				b.Fatal(err)
			}
			for rows.NextInto(&dst) {
			}
			if err := rows.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
