// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// conditionalPrepare prepares a conditional UPDATE on a server that answers
// PREPARE as oneShotServer does, with Cassandra-style result metadata.
func conditionalPrepare(t *testing.T, requests chan oneShotRequest) (*Prepared[int32, []byte], func(applied bool)) {
	t.Helper()
	s, _, peers := runtimeTestSession(t, 1)
	t.Cleanup(func() { s.Close() })
	t.Cleanup(func() { peers[0].Close() })
	oneShotServer(t, peers[0], requests)
	statement := oneShotStatement()
	statement.Kind = cql.ConditionalStatement
	statement.CQL = "UPDATE ks.tbl SET value = 0x00 WHERE k = ? IF EXISTS"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := Prepare(ctx, s, statement, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	answer := func(applied bool) {
		go func() {
			r, ok := <-requests
			if !ok {
				return
			}
			_, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, casRowsBody(casBatchRow{applied, "previous"})))
		}()
	}
	return p, answer
}

// TestApply runs a conditional statement with Prepared.Apply and
// Query.Apply, which report the [applied] column whatever the statement's
// row decoder, and rejects Apply of an ordinary statement before sending.
func TestApply(t *testing.T) {
	requests := make(chan oneShotRequest, 1)
	p, answer := conditionalPrepare(t, requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, want := range []bool{true, false} {
		answer(want)
		applied, result, err := p.Apply(ctx, 1, CompiledOptions{})
		if err != nil || applied != want || result.Attempts != 1 {
			t.Fatalf("Prepared.Apply = %v, %+v, %v; want %v", applied, result, err, want)
		}
	}
	q, err := p.Query(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	answer(true)
	if applied, _, err := q.Apply(ctx, 1); err != nil || !applied {
		t.Fatalf("Query.Apply = %v, %v; want applied", applied, err)
	}

	ordinary := make(chan oneShotRequest, 1)
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	read := oneShotPrepare(t, s, peers[0], ordinary)
	_, _, err = read.Apply(ctx, 1, CompiledOptions{})
	if !errors.As(err, new(*cql.ValidationError)) || cqlerr.ExecutionStatusOf(err) != cqlerr.NotExecuted {
		t.Fatalf("Apply of an ordinary statement = %v, want a ValidationError before sending", err)
	}
	select {
	case r := <-ordinary:
		t.Fatalf("Apply of an ordinary statement sent %+v", r)
	default:
	}
}

// TestOneNilDestinationConditional rejects One with a nil destination on a
// conditional statement before sending: the row it would report exists
// whether or not the write applied.
func TestOneNilDestinationConditional(t *testing.T) {
	requests := make(chan oneShotRequest, 1)
	p, _ := conditionalPrepare(t, requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := p.One(ctx, 1, nil, CompiledOptions{})
	if !errors.As(err, new(*cql.ValidationError)) || cqlerr.ExecutionStatusOf(err) != cqlerr.NotExecuted {
		t.Fatalf("One(nil) on a conditional statement = %v, want a ValidationError before sending", err)
	}
	select {
	case r := <-requests:
		t.Fatalf("One(nil) on a conditional statement sent %+v", r)
	default:
	}
}

// TestExecRejectsResultColumns checks that Exec of a prepared statement
// whose prepare metadata has result columns, as Cassandra and ScyllaDB
// return for a SELECT, fails before anything is sent, through the Prepared
// and a Query alike. A SERIAL read is still sent (see
// TestExecSerialConsistencyReadIsNotRejectedUpFront), and an unprepared
// statement, which has no prepare metadata, is not checked.
func TestExecRejectsResultColumns(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 1)
	p := oneShotPrepare(t, s, peers[0], requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q, err := p.Query(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	for name, exec := range map[string]func() error{
		"Prepared.Exec": func() error { _, err := p.Exec(ctx, 1, CompiledOptions{}); return err },
		"Query.Exec":    func() error { _, err := q.Exec(ctx, 1); return err },
		"Exec at QUORUM": func() error {
			_, err := p.Exec(ctx, 1, testOptions(Options{Consistency: cql.Some(cql.Quorum)}))
			return err
		},
	} {
		err := exec()
		if !errors.As(err, new(*cql.ValidationError)) || cqlerr.ExecutionStatusOf(err) != cqlerr.NotExecuted {
			t.Fatalf("%s of a SELECT = %v, want a ValidationError before sending", name, err)
		}
	}
	select {
	case r := <-requests:
		t.Fatalf("Exec of a SELECT sent %+v", r)
	default:
	}
	if m := s.Metrics(); m.Requests != 0 {
		t.Fatalf("a rejected Exec counted a request: %+v", m)
	}
}
