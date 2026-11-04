// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"fmt"
	"net"
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

// eachPageTimeoutQuery is eachTestQuery with a page timeout below the
// execution timeout, so pages run under the query's reused page context.
func eachPageTimeoutQuery(t *testing.T) (*Query[Unit, []byte], net.Conn) {
	t.Helper()
	s, _, peers := runtimeTestSession(t, 1)
	statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	options := Options{Timeout: Option[time.Duration]{Set: true, Value: 5 * time.Second}, PageTimeout: Option[time.Duration]{Set: true, Value: 100 * time.Millisecond}}
	q := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), options))
	t.Cleanup(q.Close)
	return q, peers[0]
}

// A page that outlives PageTimeout fails with a deadline while the
// execution timeout still holds, and the next execution's pages get a fresh
// page budget from the same reused context.
func TestQueryEachPageTimeoutPerPage(t *testing.T) {
	q, peer := eachPageTimeoutQuery(t)
	answer := func(body []byte) {
		_, stream, _, err := step03ReadRequest(peer)
		if err == nil {
			_, err = peer.Write(typedTestResponse(t, 4, stream, body))
		}
		if err != nil {
			t.Error(err)
		}
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		answer(runtimeRowsBody([]byte{1}, "a"))
		if _, _, _, err := step03ReadRequest(peer); err != nil { // page 2: never answered
			t.Error(err)
		}
		// The next run: four pages, each within the page budget but
		// together well beyond it.
		for page := byte(2); page <= 4; page++ {
			time.Sleep(60 * time.Millisecond)
			answer(runtimeRowsBody([]byte{page}, string('a'+page)))
		}
		time.Sleep(60 * time.Millisecond)
		answer(runtimeRowsBody(nil, "f"))
	}()
	ctx := context.Background()
	dst := make([]byte, 0, 8)
	started := time.Now()
	err := q.Each(ctx, Unit{}, &dst, func(*[]byte) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatal("page timeout", err, time.Since(started))
	}
	var got string
	err = q.Each(ctx, Unit{}, &dst, func(row *[]byte) error { got += string(*row); return nil })
	<-served
	if err != nil || got != "cdef" {
		t.Fatal("each page should get a fresh budget", got, err)
	}
}

// Cancelling the caller's context interrupts a page running under the
// reused page context.
func TestQueryEachParentCancelReachesPageContext(t *testing.T) {
	q, peer := eachPageTimeoutQuery(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, stream, _, err := step03ReadRequest(peer)
		if err == nil {
			_, err = peer.Write(typedTestResponse(t, 4, stream, runtimeRowsBody([]byte{1}, "a")))
		}
		if err == nil {
			_, _, _, err = step03ReadRequest(peer)
		}
		if err != nil {
			t.Error(err)
		}
		cancel()
	}()
	dst := make([]byte, 0, 8)
	started := time.Now()
	err := q.Each(ctx, Unit{}, &dst, func(*[]byte) error { return nil })
	if !errors.Is(err, context.Canceled) || time.Since(started) > 90*time.Millisecond {
		t.Fatal("parent cancellation did not reach the page", err, time.Since(started))
	}
}

// Cancelling the caller's context interrupts a blocked response wait
// although the execution context registers no watcher on it.
func TestQueryEachParentCancelInterruptsWait(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	q := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), Options{Timeout: Option[time.Duration]{Set: true, Value: 5 * time.Second}}))
	t.Cleanup(q.Close)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if _, _, _, err := step03ReadRequest(peers[0]); err != nil {
			t.Error(err)
		}
		cancel()
	}()
	dst := make([]byte, 0, 8)
	started := time.Now()
	found, err := q.One(ctx, Unit{}, &dst)
	if found || !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
		t.Fatal("parent cancellation did not end the wait", found, err, time.Since(started))
	}
}

type eachRetryAfterDelay struct{}

func (eachRetryAfterDelay) Decide(AttemptError) RetryDecision {
	return RetryDecision{Action: RetrySame, Delay: time.Minute}
}

// Cancelling the caller's context interrupts a retry delay.
func TestQueryEachParentCancelInterruptsRetryDelay(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	profile := defaultProfile()
	profile.Retry = eachRetryAfterDelay{}
	q := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), Options{Profile: codecTestMust(NewProfile(profile)), Timeout: Option[time.Duration]{Set: true, Value: 5 * time.Minute}}))
	t.Cleanup(q.Close)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, stream, _, err := step03ReadRequest(peers[0])
		if err == nil {
			_, err = peers[0].Write(step03ErrorResponse(t, stream, ErrCodeServer, nil))
		}
		if err != nil {
			t.Error(err)
		}
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	dst := make([]byte, 0, 8)
	started := time.Now()
	_, err := q.One(ctx, Unit{}, &dst)
	if !errors.Is(err, context.Canceled) || time.Since(started) > 2*time.Second {
		t.Fatal("parent cancellation did not end the retry delay", err, time.Since(started))
	}
}

func eachPrefetchQuery(t *testing.T) (*Query[Unit, []byte], net.Conn) {
	t.Helper()
	s, _, peers := runtimeTestSession(t, 1)
	statement := Statement[Unit, []byte]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: RowInto(func(*[]byte) {}, Column("value", OwnedBlobCodec, func(dst *[]byte) *[]byte { return dst }))}
	options := Options{Prefetch: Option[int]{Set: true, Value: 1}, Timeout: Option[time.Duration]{Set: true, Value: 5 * time.Second}}
	q := codecTestMust(NewUnpreparedQuery(s, statement, NewColumns(), options))
	t.Cleanup(q.Close)
	return q, peers[0]
}

func eachAnswer(t *testing.T, peer net.Conn, frame func(stream int) []byte) bool {
	_, stream, _, err := step03ReadRequest(peer)
	if err == nil {
		_, err = peer.Write(frame(stream))
	}
	if err != nil {
		t.Error(err)
		return false
	}
	return true
}

// With Prefetch, the next page is requested while the current page's rows
// are delivered, the alternating response slots keep every page intact, and
// the query is reusable afterwards.
func TestQueryEachPrefetchOverlapsPages(t *testing.T) {
	q, peer := eachPrefetchQuery(t)
	requested := make(chan int, 8)
	go func() {
		for page := 1; page <= 5; page++ {
			token := []byte{byte(page)}
			if page == 5 {
				token = nil
			}
			body := runtimeRowsBody(token, fmt.Sprint(page, "a"), fmt.Sprint(page, "b"))
			if !eachAnswer(t, peer, func(stream int) []byte { requested <- page; return typedTestResponse(t, 4, stream, body) }) {
				return
			}
		}
	}()
	var got []string
	dst := make([]byte, 0, 8)
	err := q.Each(context.Background(), Unit{}, &dst, func(row *[]byte) error {
		got = append(got, string(*row))
		if len(got) == 1 {
			// The first row of page 1 waits until page 2 was requested.
			for page := range requested {
				if page == 2 {
					break
				}
			}
		}
		return nil
	})
	if err != nil || strings.Join(got, ",") != "1a,1b,2a,2b,3a,3b,4a,4b,5a,5b" || q.LastResult().Rows != 10 {
		t.Fatal(got, err)
	}
	go eachAnswer(t, peer, func(stream int) []byte { return typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "next")) })
	if found, err := q.One(context.Background(), Unit{}, &dst); err != nil || !found || string(dst) != "next" {
		t.Fatal("query not reusable after a prefetching Each", found, err)
	}
}

// Stopping early while a prefetched page is outstanding cancels and joins
// it before Each returns; a prefetched page's failure is reported only after
// the current page's rows.
func TestQueryEachPrefetchStopAndFailure(t *testing.T) {
	q, peer := eachPrefetchQuery(t)
	abandoned, requested := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(abandoned)
		eachAnswer(t, peer, func(stream int) []byte { return typedTestResponse(t, 4, stream, runtimeRowsBody([]byte{1}, "a", "b")) })
		_, _, _, _ = step03ReadRequest(peer) // page 2: never answered
		close(requested)
	}()
	stop := errors.New("stop")
	dst := make([]byte, 0, 8)
	started := time.Now()
	// The callback stops once page 2 is outstanding at the server.
	err := q.Each(context.Background(), Unit{}, &dst, func(*[]byte) error { <-requested; return stop })
	if !errors.Is(err, stop) || time.Since(started) > 2*time.Second {
		t.Fatal("early stop did not join the outstanding page promptly", err, time.Since(started))
	}
	<-abandoned
	// A failed execution may still drain the abandoned page's accepted
	// write; Wait joins it before the query is reused.
	if err := q.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	go func() {
		eachAnswer(t, peer, func(stream int) []byte { return typedTestResponse(t, 4, stream, runtimeRowsBody([]byte{1}, "a", "b")) })
		eachAnswer(t, peer, func(stream int) []byte { return step03ErrorResponse(t, stream, ErrCodeServer, nil) })
	}()
	var got []string
	err = q.Each(context.Background(), Unit{}, &dst, func(row *[]byte) error { got = append(got, string(*row)); return nil })
	if err == nil || strings.Join(got, ",") != "a,b" {
		t.Fatal("prefetched failure not reported after the page's rows", got, err)
	}
}
