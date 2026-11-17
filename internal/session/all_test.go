// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
)

func TestQueryAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("EveryRowAcrossPages", func(t *testing.T) {
		q, serve := eachTestQuery(t)
		served := serve(runtimeRowsBody([]byte{1}, "a", "b"), runtimeRowsBody(nil, "c"))
		var dst []byte
		var got []string
		for row, err := range q.All(ctx, cql.EmptyRow{}, &dst) {
			if err != nil {
				t.Fatal(err)
			}
			if row != &dst {
				t.Fatal("All yielded a row other than dst")
			}
			got = append(got, string(*row))
		}
		<-served
		if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
			t.Fatalf("rows %q", got)
		}
	})

	t.Run("BreakReleasesExecution", func(t *testing.T) {
		q, serve := eachTestQuery(t)
		rt := q.state.runtime
		served := serve(runtimeRowsBody([]byte{1}, "a", "b"))
		var dst []byte
		rows := 0
		for _, err := range q.All(ctx, cql.EmptyRow{}, &dst) {
			if err != nil {
				t.Fatal(err)
			}
			rows++
			break
		}
		<-served
		if rows != 1 {
			t.Fatalf("rows %d", rows)
		}
		if rt.errors.Load() != 0 || rt.inflight.Load() != 0 {
			t.Fatal("break left an error or an inflight execution")
		}
		// The query is free for its next execution, which the server answers
		// as a new request, not a continuation.
		served = serve(runtimeRowsBody(nil, "next"))
		found, _, err := q.One(ctx, cql.EmptyRow{}, &dst)
		<-served
		if err != nil || !found || string(dst) != "next" {
			t.Fatalf("query reuse: %t %q %v", found, dst, err)
		}
	})

	t.Run("ErrorYieldedOnce", func(t *testing.T) {
		q, serve := eachTestQuery(t)
		q.bound.privateStorage.InternalRows = stopDecodeRows{}
		q.bound.privateStorage.plans = &InternalResultPlanCache[[]byte]{}
		served := serve(runtimeRowsBody(nil, "a"))
		var dst []byte
		var errs []error
		for row, err := range q.All(ctx, cql.EmptyRow{}, &dst) {
			if row != nil {
				t.Fatal("error yielded with a row")
			}
			errs = append(errs, err)
		}
		<-served
		if len(errs) != 1 || !errors.Is(errs[0], StopIteration) {
			t.Fatalf("errors %v", errs)
		}
	})

	t.Run("NilDestination", func(t *testing.T) {
		q, _ := eachTestQuery(t)
		yields := 0
		for row, err := range q.All(ctx, cql.EmptyRow{}, nil) {
			yields++
			if row != nil || err == nil {
				t.Fatalf("nil destination yielded %v, %v", row, err)
			}
		}
		if yields != 1 {
			t.Fatalf("yields %d", yields)
		}
	})
}

func TestPreparedAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	defer peers[0].Close()
	requests := make(chan oneShotRequest, 4)
	p := oneShotPrepare(t, s, peers[0], requests)

	t.Run("Rows", func(t *testing.T) {
		go func() {
			r := <-requests
			_, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "a", "b")))
		}()
		var dst []byte
		var got []string
		for row, err := range p.All(ctx, 1, &dst, CompiledOptions{}) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(*row))
		}
		if len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("rows %q", got)
		}
		waitIdle(t, p, 1)
	})

	t.Run("BreakReturnsQuery", func(t *testing.T) {
		go func() {
			r := <-requests
			_, _ = peers[0].Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, "a", "b")))
		}()
		var dst []byte
		for _, err := range p.All(ctx, 1, &dst, CompiledOptions{}) {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		waitIdle(t, p, 1)
		if pooledBusy(s) != 0 {
			t.Fatal("break left a pooled query busy")
		}
	})
}

func TestUnpreparedAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, conns, peers := runtimeTestSession(t, 1)
	statement := binding.Statement[cql.EmptyRow, []byte]{Kind: cql.OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: cql.EmptyParamsEncoder, Rows: cql.RowInto(func(*[]byte) {}, cql.Column("value", cql.ByteSliceCodec, func(dst *[]byte) *[]byte { return dst }))}
	go func() {
		for _, body := range [][]byte{runtimeRowsBody([]byte{1}, "a"), runtimeRowsBody(nil, "b")} {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	session := InternalWrapSession(s)
	var dst []byte
	var got []string
	for row, err := range session.UnpreparedAll(ctx, statement, cql.EmptyRow{}, &dst) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(*row))
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("rows %q", got)
	}
	rt := InternalRuntimeFor(s)
	rt.InternalMu.Lock()
	open := len(rt.openQueries())
	rt.InternalMu.Unlock()
	if open != 0 {
		t.Fatalf("open queries after UnpreparedAll: %d", open)
	}
}
