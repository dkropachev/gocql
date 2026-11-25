// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// TestStatementDefaults pins how an unclassified statement and a
// statement's Idempotent flag reach executions and batches.
func TestStatementDefaults(t *testing.T) {
	// TestStatementDefaults/InferredKind checks that an unprepared query
	// resolves an unclassified statement from its decoder and CQL, a
	// SELECT reading rows included, and rejects an ordinary one that is
	// conditional before anything is sent.
	t.Run("InferredKind", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		defer s.Close()
		kindOf := func(t *testing.T, err error, request *InternalExecutionRequest) cql.StatementKind {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			return request.routing.Kind
		}
		sel, err := unpreparedQuery(s, binding.Statement[cql.EmptyParams, int32]{CQL: "SELECT v FROM ks.t", Rows: cql.OneColumn(cql.Int32Codec)}, Options{})
		if got := kindOf(t, err, &sel.bound.privateStorage.InternalRequest); got != cql.OrdinaryStatement {
			t.Errorf("SELECT: %v, want ordinary", got)
		}
		sel.Close()
		lwt, err := unpreparedQuery(s, binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "UPDATE ks.t SET v = 1 WHERE k = 1 IF v = 0"}, Options{})
		if got := kindOf(t, err, &lwt.bound.privateStorage.InternalRequest); got != cql.ConditionalStatement {
			t.Errorf("UPDATE IF: %v, want conditional", got)
		}
		lwt.Close()
		cas, err := unpreparedQuery(s, binding.Statement[cql.EmptyParams, cql.CASResult[cql.EmptyRow]]{CQL: "SELECT * FROM ks.t", Rows: cql.CASApplied}, Options{})
		if got := kindOf(t, err, &cas.bound.privateStorage.InternalRequest); got != cql.ConditionalStatement {
			t.Errorf("CASRows: %v, want conditional", got)
		}
		cas.Close()
		_, err = unpreparedQuery(s, binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "INSERT INTO ks.t(k) VALUES(1) IF NOT EXISTS", Kind: cql.OrdinaryStatement}, Options{})
		if !hasErrorType[*cql.ValidationError](err) {
			t.Errorf("ordinary INSERT IF NOT EXISTS: %v, want a ValidationError", err)
		}
		var batch Batch
		err = InternalWrapSession(s).UnpreparedAddToBatch(&batch, binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "DELETE FROM ks.t WHERE k = 1 IF EXISTS", Kind: cql.OrdinaryStatement}, cql.EmptyParams{})
		if !hasErrorType[*cql.ValidationError](err) || batch.Len() != 0 {
			t.Errorf("ordinary DELETE IF EXISTS in a batch: %v, %d members", err, batch.Len())
		}
	})

	// TestStatementDefaults/Unprepared checks that an unprepared query
	// carries the inferred kind and the statement's idempotence, as a batch
	// member too.
	t.Run("Unprepared", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 1)
		defer s.Close()
		for _, idempotent := range []bool{false, true} {
			statement := binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "INSERT INTO ks.t(v) VALUES(1)", Idempotent: idempotent}
			q, err := unpreparedQuery(s, statement, Options{})
			if err != nil {
				t.Fatal(err)
			}
			request := &q.bound.privateStorage.InternalRequest
			if request.routing.Kind != cql.OrdinaryStatement || request.idempotent != idempotent {
				t.Errorf("idempotent %v: kind %v, idempotent %v", idempotent, request.routing.Kind, request.idempotent)
			}
			var batch Batch
			if err := q.AddToBatch(&batch, cql.EmptyParams{}); err != nil {
				t.Fatal(err)
			}
			if batch.routing.Kind != cql.OrdinaryStatement {
				t.Errorf("batch kind %v, want ordinary", batch.routing.Kind)
			}
			q.Close()
		}
	})

	// TestStatementDefaults/Idempotence checks that Options.Idempotent, when
	// set, overrides the statement's flag, and that a batch is idempotent
	// only when every member is.
	t.Run("Idempotence", func(t *testing.T) {
		for _, tc := range []struct {
			statement bool
			option    cql.Option[bool]
			want      bool
		}{
			{false, cql.Option[bool]{}, false},
			{true, cql.Option[bool]{}, true},
			{true, cql.Some(false), false},
			{false, cql.Some(true), true},
		} {
			request := InternalExecutionRequest{idempotent: tc.statement, options: &InternalResolvedOptions{InternalOptions: Options{Idempotent: tc.option}}}
			if got := request.isIdempotent(); got != tc.want {
				t.Errorf("statement %v, option %+v: %v, want %v", tc.statement, tc.option, got, tc.want)
			}
		}

		s, _, _ := runtimeTestSession(t, 1)
		defer s.Close()
		session := InternalWrapSession(s)
		add := func(batch *Batch, idempotent bool) {
			t.Helper()
			statement := binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "INSERT INTO ks.t(v) VALUES(1)", Kind: cql.OrdinaryStatement, Idempotent: idempotent}
			if err := session.UnpreparedAddToBatch(batch, statement, cql.EmptyParams{}); err != nil {
				t.Fatal(err)
			}
		}
		var batch Batch
		add(&batch, true)
		add(&batch, true)
		var request InternalExecutionRequest
		batch.queryExec(&request, &queryState{})
		if !request.idempotent {
			t.Error("batch of idempotent members is not idempotent")
		}
		add(&batch, false)
		batch.queryExec(&request, &queryState{})
		if request.idempotent {
			t.Error("batch with a non-idempotent member is idempotent")
		}
		batch.Reset()
		add(&batch, true)
		batch.queryExec(&request, &queryState{})
		if !request.idempotent {
			t.Error("Reset kept the previous members' idempotence")
		}
	})

	// TestStatementDefaults/Prepared checks, against a server that does not
	// mark conditional statements, that an unclassified prepared statement
	// reading no rows is stamped with a client timestamp, carries its
	// idempotence, and joins a batch as ordinary.
	t.Run("Prepared", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		InternalRuntimeFor(s).config.Timestamps = settings.DriverTimestamps()
		stamped := make(chan bool, 1)
		go func() {
			for {
				op, stream, body, err := readRequest(peers[0])
				if err != nil {
					return
				}
				var response []byte
				if op == byte(frm.OpPrepare) {
					response = preparedBody([]byte("id"), cql.NewColumns(), cql.NewColumns())
				} else {
					n := int(binary.BigEndian.Uint16(body))
					stamped <- body[2+n+2]&0x20 != 0
					response = binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid)
				}
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, response)); err != nil {
					return
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p, err := Prepare(ctx, s, binding.Statement[cql.EmptyParams, cql.EmptyRow]{CQL: "UPDATE ks.t SET v = 1 WHERE k = 1", Idempotent: true}, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if p.Info().Kind != cql.OrdinaryStatement || !p.privateStorage.idempotent {
			t.Fatalf("prepared kind %v, idempotent %v", p.Info().Kind, p.privateStorage.idempotent)
		}
		if _, err := p.Exec(ctx, cql.EmptyParams{}, CompiledOptions{}); err != nil {
			t.Fatal(err)
		}
		if !<-stamped {
			t.Error("EXECUTE carried no client timestamp")
		}
		q, err := p.Query(Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		if request := &q.bound.privateStorage.InternalRequest; request.routing.Kind != cql.OrdinaryStatement || !request.idempotent {
			t.Errorf("query kind %v, idempotent %v", request.routing.Kind, request.idempotent)
		}
		for _, add := range []func(*Batch) error{
			func(b *Batch) error { return p.AddToBatch(b, cql.EmptyParams{}) },
			func(b *Batch) error { return q.AddToBatch(b, cql.EmptyParams{}) },
		} {
			var batch Batch
			if err := add(&batch); err != nil {
				t.Fatal(err)
			}
			if batch.routing.Kind != cql.OrdinaryStatement || batch.notIdempotent {
				t.Errorf("batch kind %v, not idempotent %v", batch.routing.Kind, batch.notIdempotent)
			}
		}
	})
}

// TestUnpreparedMarkers checks that an unprepared statement with parameters
// but no BindMarkers fails before it is sent, unless it binds
// cql.DynamicParams, whose markers come from the values' types.
func TestUnpreparedMarkers(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 1)
	defer s.Close()
	session := InternalWrapSession(s)
	ctx := context.Background()

	missing := binding.Statement[int64, cql.EmptyRow]{CQL: "INSERT INTO ks.t(v) VALUES(?)", Params: cql.OneParam(cql.Int64Codec)}
	if _, err := session.UnpreparedExec(ctx, missing, 1); !hasErrorType[*cql.ValidationError](err) {
		t.Errorf("UnpreparedExec without BindMarkers: %v, want a ValidationError", err)
	} else if !strings.Contains(err.Error(), "BindMarkers") {
		t.Errorf("UnpreparedExec without BindMarkers: %v, want the missing markers named", err)
	}
	var batch Batch
	if err := session.UnpreparedAddToBatch(&batch, missing, 1); !hasErrorType[*cql.ValidationError](err) || batch.Len() != 0 {
		t.Errorf("UnpreparedAddToBatch without BindMarkers: %v, %d members", err, batch.Len())
	}

	named := binding.Statement[[]cql.ValueField, cql.EmptyRow]{CQL: "INSERT INTO ks.t(v) VALUES(:v)", Params: cql.NamedDynamicParams}
	if err := unpreparedMarkers(&named, []cql.ValueField{{Name: "v", Value: cql.NullValue(cql.BigIntType)}}); !hasErrorType[*cql.ValidationError](err) {
		t.Errorf("NamedDynamicParams without BindMarkers: %v, want a ValidationError", err)
	}

	dynamic := binding.Statement[[]cql.Value, cql.EmptyRow]{CQL: "INSERT INTO ks.t(k, v) VALUES(?, ?)", Params: cql.DynamicParams}
	values := []cql.Value{cql.NullValue(cql.BigIntType), cql.NullValue(cql.TextType)}
	if err := unpreparedMarkers(&dynamic, values); err != nil {
		t.Fatal(err)
	}
	first, _ := dynamic.BindMarkers.At(0)
	second, _ := dynamic.BindMarkers.At(1)
	if dynamic.BindMarkers.Len() != 2 || !first.Type.Equal(cql.BigIntType) || !second.Type.Equal(cql.TextType) {
		t.Errorf("derived markers %v", dynamic.BindMarkers)
	}
	if err := session.UnpreparedAddToBatch(&batch, binding.Statement[[]cql.Value, cql.EmptyRow]{CQL: dynamic.CQL, Params: cql.DynamicParams}, values); err != nil || batch.Len() != 1 {
		t.Errorf("UnpreparedAddToBatch with DynamicParams: %v, %d members", err, batch.Len())
	}

	for _, explicit := range []cql.Columns{cql.NewColumns(cql.ColumnSpec{Name: "v", Type: cql.BigIntType})} {
		statement := missing
		statement.BindMarkers = explicit
		if err := unpreparedMarkers(&statement, 1); err != nil || statement.BindMarkers.Len() != 1 {
			t.Errorf("explicit markers: %v", err)
		}
	}
}
