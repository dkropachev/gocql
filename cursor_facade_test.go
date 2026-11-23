// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
)

// wantColumns fails t unless columns are exactly the named columns of
// ks.tbl with the given types.
func wantColumns(t *testing.T, what string, columns cql.Columns, want ...cql.ColumnSpec) {
	t.Helper()
	if columns.Len() != len(want) {
		t.Fatalf("%s: %d columns, want %d", what, columns.Len(), len(want))
	}
	for i, w := range want {
		got, _ := columns.At(i)
		if got.Keyspace != w.Keyspace || got.Table != w.Table || got.Name != w.Name || !got.Type.Equal(w.Type) {
			t.Fatalf("%s: column %d = %s.%s.%s %s, want %s.%s.%s %s", what, i, got.Keyspace, got.Table, got.Name, got.Type, w.Keyspace, w.Table, w.Name, w.Type)
		}
	}
}

var (
	gridKeyColumn   = cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.IntType}
	gridValueColumn = cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cql.BlobType}
)

// TestCursorColumns checks that Rows, Page and RowStream report the result
// metadata of the page they read, and PreparedStream the metadata it was
// prepared with.
func TestCursorColumns(t *testing.T) {
	f := newLifetimeFixture(t)
	t.Run("Rows", func(t *testing.T) {
		rows, err := f.p.Run(f.ctx, gridKey, f.options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var dst []byte
		if !rows.NextInto(&dst) {
			t.Fatal(rows.Err())
		}
		wantColumns(t, "Rows.Columns", rows.Columns(), gridValueColumn)
	})
	t.Run("Page", func(t *testing.T) {
		page, err := f.p.RunPage(f.ctx, gridKey, cql.StartPage(), 2, f.options)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = page.Close() }()
		wantColumns(t, "Page.Columns", page.Columns(), gridValueColumn)
	})
	t.Run("RowStream", func(t *testing.T) {
		q, err := f.ps.Query(exec.StreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		stream, err := q.Run(f.ctx, gridKey)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		dst := make([]byte, 0, 16)
		if !stream.NextInto(&dst) {
			t.Fatal(stream.Err())
		}
		wantColumns(t, "RowStream.Columns", stream.Columns(), gridValueColumn)
	})
	t.Run("PreparedStream", func(t *testing.T) {
		wantColumns(t, "PreparedStream.Parameters", f.ps.Parameters(), gridKeyColumn)
		wantColumns(t, "PreparedStream.Columns", f.ps.Columns(), gridValueColumn)
		info := f.ps.Info()
		if info.Keyspace != "ks" || info.Table != "tbl" || info.Generation != f.p.Info().Generation {
			t.Fatalf("PreparedStream.Info = %+v, want the statement's table ks.tbl", info)
		}
	})
}

// casBatchRowsBody is a v4 ROWS result of [applied] and a blob value, one
// row per value, none of them applied.
func casBatchRowsBody(values ...string) []byte {
	b := binary.BigEndian.AppendUint32(nil, 2)
	b = binary.BigEndian.AppendUint32(b, 1) // global table spec
	b = binary.BigEndian.AppendUint32(b, 2)
	for _, name := range []string{"ks", "tbl"} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(name)))
		b = append(b, name...)
	}
	for _, column := range []struct {
		name string
		kind cql.Type
	}{{"[applied]", cql.TypeBoolean}, {"value", cql.TypeBlob}} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(column.name)))
		b = append(b, column.name...)
		b = binary.BigEndian.AppendUint16(b, uint16(column.kind))
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(values)))
	for _, value := range values {
		b = binary.BigEndian.AppendUint32(b, 1)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(len(value)))
		b = append(b, value...)
	}
	return b
}

// TestCASBatchRows reads the rows of a conditional batch that did not apply:
// their metadata, an owned Clone and a Retain lease outlive Close, and Info
// reports the execution.
func TestCASBatchRows(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	srv := newGridServer(t, peers[0], func(r gridRequest) []byte {
		if r.batch {
			return typedTestResponse(t, 4, r.stream, casBatchRowsBody("first", "second"))
		}
		return gridError(t, r.stream, 0)
	})
	defer srv.stop(t, peers[0])
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	update, err := s.Prepare(ctx, gridUpdate(cql.ConditionalStatement))
	if err != nil {
		t.Fatal(err)
	}
	var batch Batch
	for range 2 {
		if err := update.AddToBatch(&batch, gridKey); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.RunConditionalBatch(ctx, &batch, cql.CASRows(cql.OneColumn(cql.BlobCodec)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	wantColumns(t, "CASBatchRows.Columns", rows.Columns(),
		cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "[applied]", Type: cql.BooleanType}, gridValueColumn)
	var dst cql.CASResult[cql.Bytes]
	if !rows.NextInto(&dst) {
		t.Fatal(rows.Err())
	}
	if dst.Applied || !dst.Previous.Set || string(dst.Previous.Value.OwnedBytes()) != "first" {
		t.Fatalf("first row = %+v", dst)
	}
	cloned, err := rows.Clone()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := rows.Retain()
	if err != nil {
		t.Fatal(err)
	}
	var second, end cql.CASResult[cql.Bytes]
	if !rows.NextInto(&second) || rows.NextInto(&end) {
		t.Fatal("want exactly two rows", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if info := rows.Info(); info.Rows != 2 || info.Attempts != 1 || info.HostID != (cql.UUID{15: 1}) {
		t.Fatalf("Info = %+v, want two rows from one attempt on the fake node", info)
	}
	if cloned.Applied || !cloned.Previous.Set || string(cloned.Previous.Value.OwnedBytes()) != "first" {
		t.Fatalf("Clone = %+v, want the first row", cloned)
	}
	// The lease keeps the page, and so the borrowed second row, alive after
	// Close; the clone owns its bytes.
	if b, err := second.Previous.Value.Copy(); err != nil || string(b) != "second" {
		t.Fatalf("retained row = %q, %v", b, err)
	}
	lease.Close()
	if _, err := second.Previous.Value.Copy(); !errors.Is(err, cql.ErrReleased) {
		t.Fatalf("borrowed row after the lease closed: %v, want cql.ErrReleased", err)
	}
	if string(cloned.Previous.Value.OwnedBytes()) != "first" {
		t.Fatal("the cloned row borrowed the released page")
	}
	// NextInto reported the end before Close, so Applied still answers.
	if applied, err := rows.Applied(); err != nil || applied {
		t.Fatalf("Applied = %v, %v; want a batch that did not apply", applied, err)
	}
	if sent := srv.sent(); len(sent) != 1 || !sent[0].batch {
		t.Fatalf("sent %+v, want one BATCH", sent)
	}
}
