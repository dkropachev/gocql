// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func eachTestQuery(t *testing.T) (*Query[Unit, []byte], func(bodies ...[]byte) chan struct{}) {
	t.Helper()
	s, conns, peers := runtimeTestSession(t, 1)
	statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	q := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), Options{}))
	t.Cleanup(q.Close)
	serve := func(bodies ...[]byte) chan struct{} {
		served := make(chan struct{})
		go func() {
			defer close(served)
			for _, body := range bodies {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, body)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		return served
	}
	return q, serve
}

func TestQueryEachDecodesEveryPageAndLeavesQueryIdle(t *testing.T) {
	q, serve := eachTestQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	served := serve(runtimeRowsBody([]byte{1}, "a", "b"), runtimeRowsBody(nil, "c"))
	var got []string
	dst := make([]byte, 0, 8)
	err := q.Each(ctx, Unit{}, &dst, func(row *[]byte) error {
		got = append(got, string(*row))
		return nil
	})
	<-served
	result := q.LastResult()
	if err != nil || strings.Join(got, ",") != "a,b,c" || result.Rows != 3 {
		t.Fatal(got, result.Rows, err)
	}
	// The query is reusable at once: no Close or Wait obligation.
	served = serve(runtimeRowsBody(nil, "d"))
	found, err := q.One(ctx, Unit{}, &dst)
	<-served
	if err != nil || !found || string(dst) != "d" {
		t.Fatal(found, string(dst), err)
	}
}

func TestQueryOneStopsAfterFirstRow(t *testing.T) {
	q, serve := eachTestQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Only one page is served: One must not request the continuation.
	served := serve(runtimeRowsBody([]byte{1}, "first", "second"))
	dst := make([]byte, 0, 8)
	found, err := q.One(ctx, Unit{}, &dst)
	<-served
	if err != nil || !found || string(dst) != "first" {
		t.Fatal(found, string(dst), err)
	}
}

func TestQueryOneNotFoundClearsDestination(t *testing.T) {
	q, serve := eachTestQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	served := serve(runtimeRowsBody(nil))
	dst := []byte("stale")
	found, err := q.One(ctx, Unit{}, &dst)
	<-served
	if err != nil || found || dst != nil {
		t.Fatal(found, dst, err)
	}
}

func TestQueryEachCallbackErrorStops(t *testing.T) {
	q, serve := eachTestQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	served := serve(runtimeRowsBody([]byte{1}, "a", "b"))
	stop := errors.New("stop")
	calls := 0
	dst := make([]byte, 0, 8)
	err := q.Each(ctx, Unit{}, &dst, func(*[]byte) error { calls++; return stop })
	<-served
	if !errors.Is(err, stop) || calls != 1 {
		t.Fatal(calls, err)
	}
	served = serve(runtimeRowsBody(nil, "next"))
	if found, err := q.One(ctx, Unit{}, &dst); err != nil || !found || string(dst) != "next" {
		t.Fatal("query not reusable after a callback error", found, err)
	}
	<-served
}
