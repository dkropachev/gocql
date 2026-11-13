// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestQueryDrainAdmission(t *testing.T) {
	// An execution that returned (here: cancelled) but still drains makes the
	// next execution wait within its context instead of failing with ErrBusy;
	// calls without a context keep reporting ErrBusy.
	t.Run("NextExecutionWaitsForDrain", func(t *testing.T) {
		s, conns, peers := runtimeTestSession(t, 1)
		entered, release := make(chan struct{}), make(chan struct{})
		var block atomic.Bool
		block.Store(true)
		observer := &execVoidObserver{onAttempt: func(routing.AttemptEvent) {
			if block.CompareAndSwap(true, false) {
				close(entered)
				<-release
			}
		}}
		query := codecTestMust(unpreparedQuery(s, binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k)VALUES(1)", Params: codec.EmptyParamsEncoder, Rows: codec.EmptyRowDecoder}, Options{Observer: observer, Profile: generalPathProfile()}))
		t.Cleanup(query.Close)
		served := make(chan struct{})
		go func() {
			defer close(served)
			for range 2 {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := query.Exec(ctx, codec.EmptyRecord{}); done <- err }()
		<-entered
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		// Routing uses scratch of its own, so a drain does not block it.
		if _, err := query.Routing(codec.EmptyRecord{}); err != nil {
			t.Fatal("Routing during drain:", err)
		}
		// An execution waits for the drain, bounded by its own context.
		short, stopShort := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer stopShort()
		if _, err := query.Exec(short, codec.EmptyRecord{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Exec during drain with an expiring context:", err)
		}
		next := make(chan error, 1)
		go func() {
			joined, finish := context.WithTimeout(context.Background(), 5*time.Second)
			defer finish()
			_, err := query.Exec(joined, codec.EmptyRecord{})
			next <- err
		}()
		select {
		case err := <-next:
			t.Fatal("Exec returned before the previous execution drained:", err)
		case <-time.After(20 * time.Millisecond):
		}
		close(release)
		if err := <-next; err != nil {
			t.Fatal(err)
		}
		<-served
	})

	// Close stops the query and waits for its drain: once it returns, the
	// query is idle and refuses executions.
	t.Run("CloseWaitsForDrain", func(t *testing.T) {
		s, conns, peers := runtimeTestSession(t, 1)
		entered, release := make(chan struct{}), make(chan struct{})
		observer := &execVoidObserver{onAttempt: func(routing.AttemptEvent) { close(entered); <-release }}
		query := codecTestMust(unpreparedQuery(s, binding.Statement[codec.EmptyRecord, codec.EmptyRecord]{Kind: cqltype.OrdinaryStatement, CQL: "INSERT INTO ks.tbl(k)VALUES(1)", Params: codec.EmptyParamsEncoder, Rows: codec.EmptyRowDecoder}, Options{Observer: observer, Profile: generalPathProfile()}))
		go func() {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			if _, err := peers[0].Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, frm.ResultKindVoid})); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := query.Exec(ctx, codec.EmptyRecord{}); done <- err }()
		<-entered
		cancel()
		<-done
		short, stopShort := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer stopShort()
		query.Close()
		if err := query.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Close returned before the drain:", err)
		}
		close(release)
		joined, finish := context.WithTimeout(context.Background(), 5*time.Second)
		defer finish()
		query.Close()
		if err := query.Wait(joined); err != nil {
			t.Fatal(err)
		}
		if _, err := query.Exec(joined, codec.EmptyRecord{}); !errors.Is(err, runtimeerrors.ErrClosed) {
			t.Fatal("Exec after Close:", err)
		}
	})
}
