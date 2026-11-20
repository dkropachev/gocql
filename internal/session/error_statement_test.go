// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// TestErrorStatementBatch checks that a failed batch names its statement
// count and first statement, and never a bound value.
func TestErrorStatementBatch(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
	q := batchTestQuery(t, s)
	var batch Batch
	for range 3 {
		if err := q.AddToBatch(&batch, []byte(redactionSecret)); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		_, stream, body, err := readRequest(peers[0])
		if err != nil {
			return
		}
		if !bytes.Contains(body, []byte(redactionSecret)) {
			t.Error("the batch does not carry the bound value")
		}
		_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeInvalid, nil))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := InternalWrapSession(s).ExecBatch(ctx, &batch, CompiledOptions{})
	failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
	if !ok {
		t.Fatalf("ExecBatch = %v, want an ExecutionError", err)
	}
	const want = `batch of 3 statements, first "INSERT INTO ks.t(v) VALUES(?)"`
	if failure.Statement != "INSERT INTO ks.t(v) VALUES(?)" || failure.BatchStatements != 3 || !strings.Contains(err.Error(), want) {
		t.Errorf("message %q, Statement %q, BatchStatements %d: want %s", err, failure.Statement, failure.BatchStatements, want)
	}
	if text := errorText(err); strings.Contains(text, redactionSecret) {
		t.Errorf("error leaks the bound value: %s", text)
	}
}

// TestErrorStatementPrepared checks that a prepared statement's error names
// the CQL text it was prepared from.
func TestErrorStatementPrepared(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 1)
	defer s.Close()
	rt := InternalRuntimeFor(s)
	rt.config.ErrorStatementBytes = 1000
	columns := cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: cql.BlobType})
	state, _ := preparedState(t, s, columns)
	handle := new(Prepared[cql.EmptyRow, cql.Bytes])
	storage := PreparedData(handle)
	storage.InternalState = state
	storage.params = codecTestMust(cql.EmptyParamsEncoder.Compile(cql.NewColumns()))
	storage.rows = cql.OneColumn(cql.BlobCodec)
	bound := codecTestMust(BindBorrowed(handle, cql.EmptyRow{}))
	defer bound.Close()
	err := newExecutionError(cqlerr.ErrNoConnections, cql.Result{}, cqlerr.NotExecuted, nil)
	rt.describeStatement(err, &bound.privateStorage.InternalRequest)
	if err.Statement != state.InternalStatement || !strings.Contains(err.Error(), `statement "`+state.InternalStatement+`"`) {
		t.Errorf("message %q, Statement %q: want the prepared CQL %q", err, err.Statement, state.InternalStatement)
	}
}

// TestErrorStatementTruncated checks that text over the limit is cut at a
// character boundary and marked, that a statement already named is kept, and
// that errors other than ExecutionError are left alone.
func TestErrorStatementTruncated(t *testing.T) {
	for _, tc := range []struct {
		statement string
		limit     int
		want      string
	}{
		{"SELECT * FROM ks.t", 100, "SELECT * FROM ks.t"},
		{"SELECT * FROM ks.t", 18, "SELECT * FROM ks.t"},
		{"SELECT * FROM ks.t", 6, "SELECT..."},
		{"SELECT 'ж'", 9, "SELECT '..."}, // the limit falls inside ж
		{"SELECT 'ж'", 10, "SELECT 'ж..."},
	} {
		if got := truncateStatement(tc.statement, tc.limit); got != tc.want {
			t.Errorf("truncateStatement(%q, %d) = %q, want %q", tc.statement, tc.limit, got, tc.want)
		}
	}
	rt := &InternalSessionRuntime{}
	rt.config.ErrorStatementBytes = 6
	request := &InternalExecutionRequest{statement: "SELECT * FROM ks.t"}
	err := newExecutionError(cqlerr.ErrNoConnections, cql.Result{}, cqlerr.NotExecuted, nil)
	rt.describeStatement(err, request)
	if err.Statement != "SELECT..." || !strings.HasSuffix(err.Error(), `; statement "SELECT..."`) {
		t.Errorf("message %q, Statement %q", err, err.Statement)
	}
	request.statement = "UPDATE ks.t"
	rt.describeStatement(err, request)
	if err.Statement != "SELECT..." {
		t.Errorf("a named statement was replaced by %q", err.Statement)
	}
	rt.describeStatement(cqlerr.ErrNoConnections, request) // not an ExecutionError: no panic, no change
	rt.config.ErrorStatementBytes = 0
	off := newExecutionError(cqlerr.ErrNoConnections, cql.Result{}, cqlerr.NotExecuted, nil)
	rt.describeStatement(off, request)
	if off.Statement != "" {
		t.Errorf("statement %q added while off", off.Statement)
	}
}
