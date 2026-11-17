//go:build !race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

// TestRunBatchAllocs checks that a batch sent again once its rows are closed
// reuses their storage. What remains is registering the held pooled query, so
// that Session.Close stops open rows and dropped rows release their query: the
// three allocations Prepared.Run pays for the same reason. It runs without the
// race detector, whose instrumentation allocates on its own.
func TestRunBatchAllocs(t *testing.T) {
	session, batch := newRunBatch(t)
	type borrowed struct{ V cql.Bytes }
	decoder := cql.Row(cql.Column("value", cql.BlobCodec, func(r *borrowed) *cql.Bytes { return &r.V }))
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

// TestRunConditionalBatchAllocs checks that the applied tally adds nothing
// to what RunBatch allocates for the same response and decoder. The previous
// values are borrowed, so decoding them allocates nothing either.
func TestRunConditionalBatchAllocs(t *testing.T) {
	session, batch := newCASBatch(t, casRowsBody(casBatchRow{true, "a"}, casBatchRow{true, "b"}))
	type borrowed struct{ V cql.Bytes }
	decoder := cql.CASRows(cql.Row(cql.Column("value", cql.BlobCodec, func(r *borrowed) *cql.Bytes { return &r.V })))
	var row cql.CASResult[borrowed]
	plain := func() {
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
	plain()
	baseline := testing.AllocsPerRun(50, plain)
	send := func() {
		rows, err := session.RunConditionalBatch(context.Background(), batch, decoder, CompiledOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if applied, err := rows.Applied(); err != nil || !applied {
			t.Fatal(applied, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	send()
	n := testing.AllocsPerRun(50, send)
	if n > baseline {
		t.Fatalf("RunConditionalBatch allocates %v times per send, RunBatch %v", n, baseline)
	}
	if n > heldQueryAllocs {
		t.Fatalf("RunConditionalBatch allocates %v times per send, want at most %d", n, heldQueryAllocs)
	}
}
