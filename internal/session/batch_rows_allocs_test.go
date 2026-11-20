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
	gateAllocs(t, "RunBatch", heldQueryAllocs, send)
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
	baseline := gateAllocs(t, "RunBatch", heldQueryAllocs, plain)
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
	n := gateAllocs(t, "RunConditionalBatch", heldQueryAllocs, send)
	if n > baseline+allocTolerance {
		t.Errorf("RunConditionalBatch allocates %.3f times per send, RunBatch %.3f", n, baseline)
	}
}
