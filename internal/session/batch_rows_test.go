// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

type batchRow struct{ A, B []byte }

// heldQueryAllocs is what registering a held pooled query allocates: its
// registry entry and its GC cleanup.
const heldQueryAllocs = 3

var (
	batchRowA = codec.Row(codec.Column("value", codec.ByteSliceCodec, func(r *batchRow) *[]byte { return &r.A }))
	batchRowB = codec.Row(codec.Column("value", codec.ByteSliceCodec, func(r *batchRow) *[]byte { return &r.B }))
)

// runBatchRows reads every row of a RunBatch and closes the rows, returning
// the A and B fields of each row joined.
func runBatchRows(t testing.TB, session *Session, batch *Batch, decoder codec.RowDecoder[batchRow]) []string {
	t.Helper()
	rows, err := session.RunBatch(context.Background(), batch, decoder)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	var row batchRow
	for rows.NextInto(&row) {
		got = append(got, string(row.A)+string(row.B))
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	return got
}

// newRunBatch returns a session answering every request with rows "a" and
// "b", and a batch of one member.
func newRunBatch(t *testing.T) (*Session, *Batch) {
	s := batchServeSession(t, runtimeRowsBody(nil, "a", "b"))
	q := batchTestQuery(t, s)
	var batch Batch
	if err := q.AddToBatch(&batch, []byte("v")); err != nil {
		t.Fatal(err)
	}
	return InternalWrapSession(s), &batch
}

func TestRunBatch(t *testing.T) {
	decoder := batchRowA
	newBatch := newRunBatch

	t.Run("Rows", func(t *testing.T) {
		session, batch := newBatch(t)
		if got := runBatchRows(t, session, batch, decoder); len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("rows = %q", got)
		}
	})

	// A handle kept from an earlier execution is stale once the storage is
	// reused, even though the pooled query behind it may differ.
	t.Run("StaleHandle", func(t *testing.T) {
		session, batch := newBatch(t)
		old, err := session.RunBatch(context.Background(), batch, decoder)
		if err != nil {
			t.Fatal(err)
		}
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		runBatchRows(t, session, batch, decoder)
		var row batchRow
		if old.NextInto(&row) || old.Err() != runtimeerrors.ErrStaleRows {
			t.Fatalf("stale handle read rows: err %v", old.Err())
		}
	})

	// Rows still open keep their storage: the next RunBatch gets its own.
	t.Run("OpenRows", func(t *testing.T) {
		session, batch := newBatch(t)
		open, err := session.RunBatch(context.Background(), batch, decoder)
		if err != nil {
			t.Fatal(err)
		}
		if got := runBatchRows(t, session, batch, decoder); len(got) != 2 {
			t.Fatalf("rows = %q", got)
		}
		var row batchRow
		if !open.NextInto(&row) || string(row.A) != "a" {
			t.Fatalf("open rows lost their storage: %q %v", row, open.Err())
		}
		if err := open.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Another decoder with the same R compiles its own plan.
	t.Run("DecoderChange", func(t *testing.T) {
		session, batch := newBatch(t)
		runBatchRows(t, session, batch, decoder)
		var row batchRow
		rows, err := session.RunBatch(context.Background(), batch, batchRowB)
		if err != nil {
			t.Fatal(err)
		}
		if !rows.NextInto(&row) || row.A != nil || string(row.B) != "a" {
			t.Fatalf("row = %+v, %v", row, rows.Err())
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestUnpreparedAddToBatch(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	session := InternalWrapSession(s)
	var batch Batch
	statement := binding.Statement[[]byte, codec.EmptyRecord]{
		Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.t(v) VALUES(?)",
		Params: codec.OneParam(codec.ByteSliceCodec), Rows: codec.EmptyRowDecoder,
		BindMarkers: cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: cqltype.BlobType}),
	}
	if err := session.UnpreparedAddToBatch(&batch, statement, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 || batch.entries[0].statement != statement.CQL || batch.entries[0].source != nil && batch.entries[0].source.prepared() != nil {
		t.Fatalf("entry = %+v", batch.entries[0])
	}
	if got := string(batch.values[batch.entries[0].start:batch.entries[0].end]); got != "\x00\x00\x00\x03abc" {
		t.Fatalf("values = %q", got)
	}
	if batch.routing.Token.Set {
		t.Fatal("an unprepared member gave the batch a token")
	}
	conditional := statement
	conditional.Kind = cqltype.ConditionalStatement
	if err := session.UnpreparedAddToBatch(&batch, conditional, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if batch.routing.Kind != cqltype.ConditionalStatement {
		t.Fatalf("kind = %v", batch.routing.Kind)
	}
	bad := statement
	bad.CQL = " "
	if err := session.UnpreparedAddToBatch(&batch, bad, nil); err == nil || batch.Len() != 2 {
		t.Fatalf("empty CQL: %v, len %d", err, batch.Len())
	}
}
