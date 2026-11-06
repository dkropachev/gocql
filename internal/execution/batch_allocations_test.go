//go:build !race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"testing"
)

// A reused batch sent through a query takes the query's inline execution and
// the void-result path: once warm, appending and sending allocate nothing.
func TestBatchExecDoesNotAllocate(t *testing.T) {
	s := batchBenchSession(t)
	q := batchTestQuery(t, s)
	ctx := context.Background()
	value := []byte("0123456789abcdef0123456789abcdef")
	var batch Batch
	send := func() {
		batch.Reset()
		for i := 0; i < 8; i++ {
			if err := q.AddTo(&batch, value); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := q.ExecBatch(ctx, UnloggedBatch, &batch, Options{}); err != nil {
			t.Fatal(err)
		}
	}
	send() // warm the batch, the query and the frame scratch
	if n := testing.AllocsPerRun(50, send); n != 0 {
		t.Fatalf("ExecBatch allocates %v times per send", n)
	}
}
