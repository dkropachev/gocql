// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/routing"
)

type casBatchRow struct {
	applied bool
	value   string
}

// casRowsBody is a v4 ROWS result with columns [applied] and value, one row
// per row.
func casRowsBody(rows ...casBatchRow) []byte {
	b := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint32(b, 2)
	for _, v := range []string{"ks", "tbl"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(v)))
		b = append(b, v...)
	}
	for _, c := range []struct {
		name string
		typ  cql.Type
	}{{"[applied]", cql.TypeBoolean}, {"value", cql.TypeBlob}} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(c.name)))
		b = append(b, c.name...)
		b = binary.BigEndian.AppendUint16(b, uint16(c.typ))
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(rows)))
	for _, row := range rows {
		b = binary.BigEndian.AppendUint32(b, 1)
		if row.applied {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(row.value)))
		b = append(b, row.value...)
	}
	return b
}

var casBatchDecoder = cql.CASRows(batchRowA)

// newCASBatch returns a session answering every request with a RESULT of
// body, and a batch of one conditional member.
func newCASBatch(t testing.TB, body []byte) (*Session, *Batch) {
	t.Helper()
	return newServedBatchOf(t, body, func(s *implCluster.Core, batch *Batch) error {
		return batch.add(s, cqlText{}, "UPDATE ks.t SET v = 1 WHERE k = 1 IF v = 0", &routing.Info{Kind: cql.ConditionalStatement}, false, model.EncodedValues{})
	})
}

func runCASBatch(t testing.TB, session *Session, batch *Batch) CASBatchRows[batchRow] {
	t.Helper()
	rows, err := session.RunConditionalBatch(context.Background(), batch, casBatchDecoder, CompiledOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestRunConditionalBatch(t *testing.T) {
	applied := casRowsBody(casBatchRow{true, "a"}, casBatchRow{true, "b"})
	notApplied := casRowsBody(casBatchRow{true, "a"}, casBatchRow{false, "b"}, casBatchRow{true, "c"})

	for _, tc := range []struct {
		name string
		body []byte
		// read is how many rows NextInto reads before Applied.
		read int
		want bool
	}{
		{"AppliedUnread", applied, 0, true},
		{"AppliedRead", applied, 2, true},
		{"NotAppliedUnread", notApplied, 0, false},
		{"NotAppliedPartlyRead", notApplied, 1, false},
		{"NotAppliedRead", notApplied, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, batch := newCASBatch(t, tc.body)
			rows := runCASBatch(t, session, batch)
			defer rows.Close()
			var row cql.CASResult[batchRow]
			for i := 0; i < tc.read; i++ {
				if !rows.NextInto(&row) {
					t.Fatalf("row %d: %v", i, rows.Err())
				}
			}
			got, err := rows.Applied()
			if err != nil || got != tc.want {
				t.Fatalf("Applied = %v, %v; want %v", got, err, tc.want)
			}
			if rows.NextInto(&row) {
				t.Fatal("a row after Applied")
			}
			if got, err := rows.Applied(); err != nil || got != tc.want {
				t.Fatalf("second Applied = %v, %v", got, err)
			}
		})
	}

	t.Run("ClosedEarly", func(t *testing.T) {
		session, batch := newCASBatch(t, applied)
		rows := runCASBatch(t, session, batch)
		var row cql.CASResult[batchRow]
		if !rows.NextInto(&row) {
			t.Fatal(rows.Err())
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := rows.Applied(); err == nil {
			t.Fatalf("Applied after an early Close = %v", got)
		}
	})

	t.Run("ClosedAfterApplied", func(t *testing.T) {
		session, batch := newCASBatch(t, applied)
		rows := runCASBatch(t, session, batch)
		if _, err := rows.Applied(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := rows.Applied(); err != nil || !got {
			t.Fatalf("Applied after Close = %v, %v", got, err)
		}
	})

	// The next execution reuses the tally: a kept handle is stale and must
	// not report or change it.
	t.Run("StaleHandle", func(t *testing.T) {
		session, batch := newCASBatch(t, notApplied)
		old := runCASBatch(t, session, batch)
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		current := runCASBatch(t, session, batch)
		defer current.Close()
		var row cql.CASResult[batchRow]
		if old.NextInto(&row) {
			t.Fatal("stale handle read a row")
		}
		if _, err := old.Applied(); err != cqlerr.ErrStaleRows {
			t.Fatalf("stale Applied: %v", err)
		}
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := current.Applied(); err != nil || got {
			t.Fatalf("current Applied = %v, %v", got, err)
		}
	})

	// Reading the last row does not end the rows: only a NextInto that
	// returns false does. Closing before that cannot tell.
	t.Run("ClosedAfterLastRow", func(t *testing.T) {
		session, batch := newCASBatch(t, applied)
		rows := runCASBatch(t, session, batch)
		var row cql.CASResult[batchRow]
		for range 2 {
			if !rows.NextInto(&row) {
				t.Fatal(rows.Err())
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := rows.Applied(); err == nil {
			t.Fatalf("Applied after Close before the end = %v", got)
		}
	})

	// A batch known to have no condition is rejected before it is sent.
	t.Run("Ordinary", func(t *testing.T) {
		session, batch := newServedBatch(t, binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid))
		if _, err := session.RunConditionalBatch(context.Background(), batch, casBatchDecoder, CompiledOptions{}); !hasErrorType[*cql.ValidationError](err) {
			t.Fatalf("ordinary batch: %v", err)
		}
		if m := session.Core().Metrics(); m.Requests != 0 {
			t.Fatalf("a rejected batch counted a request: %+v", m)
		}
	})

	// A conditional result always has a row; none cannot report Applied.
	t.Run("NoRows", func(t *testing.T) {
		session, batch := newCASBatch(t, casRowsBody())
		rows := runCASBatch(t, session, batch)
		defer rows.Close()
		if got, err := rows.Applied(); err == nil {
			t.Fatalf("Applied without rows = %v", got)
		}
	})

	// A result without [applied] fails the call: there is nothing to tally.
	t.Run("NotCAS", func(t *testing.T) {
		session, batch := newCASBatch(t, runtimeRowsBody(nil, "a"))
		if _, err := session.RunConditionalBatch(context.Background(), batch, casBatchDecoder, CompiledOptions{}); err == nil {
			t.Fatal("rows without [applied] were accepted")
		}
	})

	t.Run("Zero", func(t *testing.T) {
		var rows CASBatchRows[batchRow]
		if _, err := rows.Applied(); err == nil {
			t.Fatal("zero rows reported Applied")
		}
	})
}

// TestCASBatchRowsAll checks that All yields every row, closes the rows, and
// leaves Applied reporting the outcome.
func TestCASBatchRowsAll(t *testing.T) {
	session, batch := newCASBatch(t, casRowsBody(casBatchRow{true, "a"}, casBatchRow{false, "b"}))
	rows := runCASBatch(t, session, batch)
	var values []string
	for row, err := range rows.All(nil) {
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, string(row.Previous.Value.A))
	}
	if len(values) != 2 || values[0] != "a" || values[1] != "b" {
		t.Fatalf("rows %q", values)
	}
	if applied, err := rows.Applied(); err != nil || applied {
		t.Fatalf("Applied = %v, %v; want false", applied, err)
	}
}
