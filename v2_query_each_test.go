// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
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

// Each lends the query's response storage to every page. Paging tokens of
// different lengths and per-page row counts must survive that reuse, and a
// token the history already holds must still be detected after the storage
// that carried it was overwritten.
func TestQueryEachReusedStorageKeepsTokensAndDetectsRepeats(t *testing.T) {
	q, serve := eachTestQuery(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	served := serve(runtimeRowsBody([]byte{1}, "a"), runtimeRowsBody([]byte{2, 2, 2}, "b", "c", "d"), runtimeRowsBody([]byte{3}), runtimeRowsBody(nil, "e"))
	var got []string
	dst := make([]byte, 0, 8)
	err := q.Each(ctx, Unit{}, &dst, func(row *[]byte) error {
		got = append(got, string(*row))
		return nil
	})
	<-served
	if err != nil || strings.Join(got, ",") != "a,b,c,d,e" || q.LastResult().Rows != 5 {
		t.Fatal(got, q.LastResult().Rows, err)
	}
	got = got[:0]
	served = serve(runtimeRowsBody([]byte{1}, "a"), runtimeRowsBody([]byte{2, 2}, "b"), runtimeRowsBody([]byte{1}, "c"))
	err = q.Each(ctx, Unit{}, &dst, func(row *[]byte) error {
		got = append(got, string(*row))
		return nil
	})
	<-served
	if !errors.Is(err, ErrPagingNoProgress) || strings.Join(got, ",") != "a,b,c" {
		t.Fatal(got, err)
	}
	served = serve(runtimeRowsBody(nil, "next"))
	if found, err := q.One(ctx, Unit{}, &dst); err != nil || !found || string(dst) != "next" {
		t.Fatal("query not reusable after a paging failure", found, err)
	}
	<-served
}

// An UNPREPARED continuation page is reprepared and re-executed in the
// query's lent response storage, and the released first response does not
// leak into the repaired page.
func TestQueryEachUnpreparedContinuationRepairs(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	implCluster.SessionData(s).InternalCfg.Keyspace = "ks"
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: typ})
	state, old := step03PreparedState(t, s, columns)
	state.InternalParameters = columns
	state.InternalInfo.PartitionKeyIndexes = []int{0}
	handle := new(Prepared[[]byte, Bytes])
	implExecution.PreparedData(handle).InternalState = state
	implExecution.PreparedData(handle).InternalParams = codecTestMust(OneParam(BytesEncoder).Compile(columns))
	implExecution.PreparedData(handle).InternalRows = OneColumn(BlobCodec)
	implExecution.PreparedData(handle).InternalPlans = &resultPlanCache[Bytes]{}
	q := codecTestMust(NewQuery(handle, Options{}))
	defer q.Close()
	serverDone := make(chan error, 1)
	go func() {
		for step := 0; step < 4; step++ {
			op, stream, _, err := step03ReadRequest(peers[0])
			if err != nil {
				serverDone <- err
				return
			}
			switch {
			case step == 2 && op == 9:
				_, err = peers[0].Write(typedTestResponse(t, 4, stream, step03PreparedBody([]byte("new"), columns, columns, 0)))
			case step == 2:
				err = fmt.Errorf("wanted PREPARE got %d", op)
			case step == 0:
				_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("a"), "one", "uno")))
			case step == 1:
				_, err = peers[0].Write(step03ErrorResponse(t, stream, ErrCodeUnprepared, old.InternalId))
			default:
				_, err = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "two")))
			}
			if err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var dst Bytes
	var values []string
	err := q.Each(ctx, []byte("key"), &dst, func(row *Bytes) error {
		values = append(values, string(row.OwnedBytes()))
		return nil
	})
	if err != nil || fmt.Sprint(values) != "[one uno two]" || q.LastResult().Rows != 3 {
		t.Fatal(values, q.LastResult().Rows, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
