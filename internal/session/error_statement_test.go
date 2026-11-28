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
	"github.com/scylladb/gocql/v2/internal/binding"
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
	if failure.Statement != "INSERT INTO ks.t(v) VALUES(?)" || failure.BatchSize != 3 || !strings.Contains(err.Error(), want) {
		t.Errorf("message %q, Statement %q, BatchSize %d: want %s", err, failure.Statement, failure.BatchSize, want)
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
	storage.params = codecTestMust(cql.NoParams.Compile(cql.NewColumns()))
	storage.rows = cql.OneColumn(cql.BytesViewCodec)
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

// secretQuery binds redactionSecret to an unprepared statement of kind.
func secretQuery(t *testing.T, kind cql.StatementKind) *Bound[cql.Bytes] {
	t.Helper()
	bound, err := BindUnpreparedOwned(binding.Statement[string, cql.Bytes]{
		Kind:   kind,
		CQL:    "UPDATE ks.tbl SET value = 0x01 WHERE k = ?",
		Params: cql.OneParam(cql.StringCodec),
		Rows:   cql.OneColumn(cql.BytesViewCodec),
	}, cql.NewColumns(cql.ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "k", Type: cql.TextType}), redactionSecret)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bound.Close)
	return bound
}

// checkNamed checks that err is an ExecutionError naming the statement of
// secretQuery and not its bound value.
func checkNamed(t *testing.T, err error) {
	t.Helper()
	failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
	if !ok {
		t.Fatalf("%v: want an ExecutionError", err)
	}
	const want = `statement "UPDATE ks.tbl SET value = 0x01 WHERE k = ?"`
	if failure.Statement != "UPDATE ks.tbl SET value = 0x01 WHERE k = ?" || !strings.Contains(err.Error(), want) {
		t.Errorf("message %q, Statement %q: want %s", err, failure.Statement, want)
	}
	if text := errorText(err); strings.Contains(text, redactionSecret) {
		t.Errorf("error leaks the bound value: %s", text)
	}
}

// TestErrorStatementPaths checks the paths that fill the statement outside
// executePage: refusals before a request is sent and row streams.
func TestErrorStatementPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Run("AdmissionRefused", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		s.Close()
		_, err := RunPage(ctx, s, secretQuery(t, cql.OrdinaryStatement), cql.StartPage(), 0, CompiledOptions{})
		if !errors.Is(err, cqlerr.ErrClosed) {
			t.Fatalf("RunPage on a closed session = %v", err)
		}
		checkNamed(t, err)
	})
	t.Run("ConditionalExec", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		defer s.Close()
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		_, err := Exec(ctx, s, secretQuery(t, cql.ConditionalStatement), CompiledOptions{})
		if !errors.Is(err, cqlerr.ErrConditionalExec) {
			t.Fatalf("Exec of a conditional statement = %v", err)
		}
		checkNamed(t, err)
	})
	t.Run("ConditionalBatchRefused", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		defer s.Close()
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		var batch Batch
		if err := batchTestQuery(t, s).AddToBatch(&batch, []byte(redactionSecret)); err != nil {
			t.Fatal(err)
		}
		_, err := InternalWrapSession(s).RunConditionalBatch(ctx, &batch, cql.CASRows(cql.Row[cql.EmptyRow]()), CompiledOptions{})
		failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
		if !ok || failure.Statement != "INSERT INTO ks.t(v) VALUES(?)" || failure.BatchSize != 1 {
			t.Fatalf("RunConditionalBatch of an ordinary batch = %v", err)
		}
		if text := errorText(err); strings.Contains(text, redactionSecret) {
			t.Errorf("error leaks the bound value: %s", text)
		}
	})
	t.Run("RowStream", func(t *testing.T) {
		s, _, peer := rowStreamSession(t, 4, nil)
		defer s.Close()
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		served := make(chan struct{})
		go func() {
			defer close(served)
			_, stream, body, err := readRequest(peer)
			if err != nil {
				return
			}
			if !bytes.Contains(body, []byte(redactionSecret)) {
				t.Error("the request does not carry the bound value")
			}
			_, _ = peer.Write(serverError(t, stream, cqlerr.CodeInvalid, nil))
		}()
		_, err := RunStream(ctx, s, secretQuery(t, cql.OrdinaryStatement), cql.StreamOneColumn(cql.StreamBlob), StreamOptions{Incremental: true})
		select {
		case <-served:
		case <-ctx.Done():
			t.Fatal("the server never answered")
		}
		if err == nil {
			t.Fatal("stream succeeded")
		}
		checkNamed(t, err)
	})
	// A stream that fails before any attempt, here for want of a
	// connection, reports the failure as an ExecutionError of its own.
	t.Run("RowStreamNoAttempt", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 0)
		defer s.Close()
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		_, err := RunStream(ctx, s, secretQuery(t, cql.OrdinaryStatement), cql.StreamOneColumn(cql.StreamBlob), StreamOptions{Incremental: true})
		if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); !ok || len(failure.Attempts) != 0 {
			t.Fatalf("RunStream without connections = %v, want an ExecutionError without attempts", err)
		}
		checkNamed(t, err)
	})
	t.Run("RunPageRefused", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		defer s.Close()
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		_, err := RunPage(ctx, s, secretQuery(t, cql.OrdinaryStatement), cql.StartPage(), -1, CompiledOptions{})
		if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
			t.Fatalf("RunPage with a negative page size = %v", err)
		}
		checkNamed(t, err)
		// A reusable query's RunPage refuses the same way.
		_, err = batchTestQuery(t, s).RunPage(ctx, []byte(redactionSecret), cql.StartPage(), -1)
		if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); !ok || failure.Statement != "INSERT INTO ks.t(v) VALUES(?)" {
			t.Errorf("Query.RunPage with a negative page size = %v", err)
		}
		if text := errorText(err); strings.Contains(text, redactionSecret) {
			t.Errorf("error leaks the bound value: %s", text)
		}
	})
	t.Run("BatchRefused", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		InternalRuntimeFor(s).config.ErrorStatementBytes = 1000
		var batch Batch
		if err := batchTestQuery(t, s).AddToBatch(&batch, []byte(redactionSecret)); err != nil {
			t.Fatal(err)
		}
		s.Close()
		session := InternalWrapSession(s)
		_, execErr := session.ExecBatch(ctx, &batch, CompiledOptions{})
		_, runErr := session.RunBatch(ctx, &batch, cql.Row[cql.EmptyRow](), CompiledOptions{})
		for name, err := range map[string]error{"ExecBatch": execErr, "RunBatch": runErr} {
			failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
			if !ok || failure.Statement != "INSERT INTO ks.t(v) VALUES(?)" || failure.BatchSize != 1 {
				t.Errorf("%s on a closed session = %v", name, err)
			}
			if text := errorText(err); strings.Contains(text, redactionSecret) {
				t.Errorf("%s: error leaks the bound value: %s", name, text)
			}
		}
	})
}
