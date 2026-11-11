//go:build !race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// TestRunBatchAllocs checks that a batch sent again once its rows are closed
// reuses their storage. What remains is registering the held pooled query, so
// that Session.Close stops open rows and dropped rows release their query: the
// three allocations Prepared.Run pays for the same reason. It runs without the
// race detector, whose instrumentation allocates on its own.
func TestRunBatchAllocs(t *testing.T) {
	session, batch := newRunBatch(t)
	type borrowed struct{ V cqltype.Bytes }
	decoder := codec.Row(codec.Column("value", codec.BlobCodec, func(r *borrowed) *cqltype.Bytes { return &r.V }))
	var row borrowed
	send := func() {
		rows, err := session.RunBatch(context.Background(), batch, decoder, CompiledOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for rows.NextInto(&row) {
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	send()
	if n := testing.AllocsPerRun(50, send); n > heldQueryAllocs {
		t.Fatalf("RunBatch allocates %v times per send, want at most %d", n, heldQueryAllocs)
	}
}
