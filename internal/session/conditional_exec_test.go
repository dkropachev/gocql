// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/model"
)

// An Exec of a statement or batch known to be conditional is rejected before
// anything is sent: its result reports whether it applied, which Exec cannot
// return, so sending it would apply a write the caller cannot inspect.
func TestConditionalExecRejected(t *testing.T) {
	check := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, cqlerr.ErrConditionalExec) {
			t.Fatalf("err = %v, want ErrConditionalExec", err)
		}
		if status := cqlerr.ExecutionStatusOf(err); status != cqlerr.NotExecuted {
			t.Fatalf("status = %v, want NotExecuted", status)
		}
		if kind := cqlerr.KindOf(err); kind != cqlerr.KindInvalid {
			t.Fatalf("kind = %v, want invalid", kind)
		}
	}
	t.Run("Statement", func(t *testing.T) {
		// The session has no connections: a request that was sent would fail
		// with a connection error instead.
		s, _, _ := runtimeTestSession(t, 0)
		b, err := BindUnpreparedBorrowed(binding.Statement[cql.EmptyRow, cql.Bytes]{Kind: cql.ConditionalStatement, CQL: "UPDATE ks.tbl SET v = 1 WHERE k = 1 IF v = 0", Params: cql.EmptyParamsEncoder, Rows: cql.OneColumn(cql.BlobCodec)}, cql.NewColumns(), cql.EmptyRow{})
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		_, err = Exec(context.Background(), s, b, CompiledOptions{})
		check(t, err)
		if m := s.Metrics(); m.Requests != 0 {
			t.Fatalf("a rejected Exec counted a request: %+v", m)
		}
	})
	t.Run("Batch", func(t *testing.T) {
		s, _, _ := runtimeTestSession(t, 0)
		for _, kinds := range [][]cql.StatementKind{
			{cql.ConditionalStatement},
			{cql.OrdinaryStatement, cql.ConditionalStatement},
		} {
			var batch Batch
			for _, kind := range kinds {
				if err := batch.add(s, cqlText{}, "UPDATE t SET v=1", &model.RoutingInfo{Kind: kind}, model.EncodedValues{}); err != nil {
					t.Fatal(err)
				}
			}
			_, err := InternalWrapSession(s).ExecBatch(context.Background(), &batch, CompiledOptions{})
			check(t, err)
		}
	})
}

// SERIAL consistency makes a read a Paxos read that is routed like a
// conditional statement, but its result is ordinary rows: Exec does not
// reject it up front. It is sent, and fails on its result columns.
func TestExecSerialConsistencyReadIsNotRejectedUpFront(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 1)
	p := oneShotPrepare(t, s, peers[0], requests)
	sizes := answerOneShot(t, peers[0], requests, "v")
	opts := testOptions(Options{Consistency: cql.Option[cql.Consistency]{Set: true, Value: cql.Serial}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.Exec(ctx, 1, opts)
	select {
	case <-sizes:
	default:
		t.Fatal("the request was not sent")
	}
	if err == nil {
		t.Fatal("Exec of a row-returning statement succeeded")
	}
	if errors.Is(err, cqlerr.ErrConditionalExec) {
		t.Fatalf("err = %v, rejected up front as conditional", err)
	}
	if !strings.Contains(err.Error(), "result columns require Run") {
		t.Fatalf("err = %v, want the result-columns error", err)
	}
}
