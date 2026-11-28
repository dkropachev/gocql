// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/execopt"
)

// lifetimeFixture is a session with one fake node answering gridPages, a
// prepared SELECT and the same SELECT prepared for streaming.
type lifetimeFixture struct {
	s       *Session
	srv     *gridServer
	ctx     context.Context
	p       *Prepared[int32, []byte]
	ps      *PreparedStream[int32, []byte]
	options execopt.CompiledOptions
}

func newLifetimeFixture(t *testing.T) *lifetimeFixture {
	t.Helper()
	s, _, peers := runtimeTestSession(t, 1)
	srv := newGridServer(t, peers[0], gridPages(t))
	t.Cleanup(func() { srv.stop(t, peers[0]) })
	t.Cleanup(func() { _ = s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	p, err := s.Prepare(ctx, gridSelect())
	if err != nil {
		t.Fatal(err)
	}
	ps, err := s.PrepareStream(ctx, cql.StreamStatement[int32, []byte]{CQL: gridSelect().CQL, Params: cql.OneParam(cql.Int32Codec), Rows: cql.StreamOneColumn(cql.StreamByteSlice)})
	if err != nil {
		t.Fatal(err)
	}
	options, err := s.CompileOptions(gridOptions)
	if err != nil {
		t.Fatal(err)
	}
	return &lifetimeFixture{s: s, srv: srv, ctx: ctx, p: p, ps: ps, options: options}
}

// closeTwice closes a handle twice and checks that both calls report want.
func closeTwice(t *testing.T, name string, close func() error, want error) {
	t.Helper()
	for i := range 2 {
		if err := close(); !errors.Is(err, want) || (want == nil && err != nil) {
			t.Errorf("%s: Close #%d returned %v, want %v", name, i+1, err, want)
		}
	}
}

// TestHandleLifetimes checks the Close and Wait contract of every execution
// handle: Close is idempotent, and Rows, Page and RowStream report the same
// error each time; Wait after Close returns nil; a closed query refuses work
// with cqlerr.ErrClosed; handles left open across Session.Close,
// Worker.Close or StreamWorker.Close end with cqlerr.ErrClosed instead of
// failing; and Session.Close waits for a running Each or All callback.
func TestHandleLifetimes(t *testing.T) {
	// TestHandleLifetimes/CloseIsIdempotent checks that closing each handle
	// twice is harmless and that a closed Worker or StreamWorker refuses new
	// executions with cqlerr.ErrClosed while Wait returns nil.
	t.Run("CloseIsIdempotent", func(t *testing.T) {
		f := newLifetimeFixture(t)
		var dst []byte
		rows, err := f.p.Run(f.ctx, gridKey, f.options)
		if err != nil {
			t.Fatal(err)
		}
		if !rows.NextInto(&dst) {
			t.Fatal(rows.Err())
		}
		closeTwice(t, "Rows", rows.Close, nil)
		page, err := f.p.RunPage(f.ctx, gridKey, cql.StartPage(), 2, f.options)
		if err != nil {
			t.Fatal(err)
		}
		closeTwice(t, "Page", page.Close, nil)

		q, err := f.p.With(f.s.MustCompileOptions(gridOptions)).Worker()
		if err != nil {
			t.Fatal(err)
		}
		qrows, err := q.Run(f.ctx, gridKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.Run(f.ctx, gridKey); !errors.Is(err, cqlerr.ErrBusy) {
			t.Errorf("Worker.Run with open Rows returned %v, want cqlerr.ErrBusy", err)
		}
		closeTwice(t, "Query Rows", qrows.Close, nil)
		q.Close()
		q.Close()
		for i := range 2 {
			if err := q.Wait(f.ctx); err != nil {
				t.Errorf("Worker.Wait #%d after Close returned %v", i+1, err)
			}
		}
		if _, err := q.Exec(f.ctx, gridKey); !errors.Is(err, cqlerr.ErrClosed) {
			t.Errorf("Worker.Exec after Close returned %v, want cqlerr.ErrClosed", err)
		}

		sq, err := f.ps.Worker(execopt.StreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := sq.Run(f.ctx, gridKey)
		if err != nil {
			t.Fatal(err)
		}
		sdst := make([]byte, 0, 16)
		if !stream.NextInto(&sdst) {
			t.Fatal(stream.Err())
		}
		closeTwice(t, "RowStream", stream.Close, nil)
		sq.Close()
		sq.Close()
		if err := sq.Wait(f.ctx); err != nil {
			t.Errorf("StreamWorker.Wait after Close returned %v", err)
		}
		if _, err := sq.Run(f.ctx, gridKey); !errors.Is(err, cqlerr.ErrClosed) {
			t.Errorf("StreamWorker.Run after Close returned %v, want cqlerr.ErrClosed", err)
		}
	})

	// TestHandleLifetimes/OpenAcrossSessionClose checks that Rows, a Page and
	// a RowStream left open while the session closes report cqlerr.ErrClosed
	// from NextInto, Err and every Close, that open queries' Wait returns nil,
	// and that the session refuses new work.
	t.Run("OpenAcrossSessionClose", func(t *testing.T) {
		f := newLifetimeFixture(t)
		q, err := f.p.With(f.s.MustCompileOptions(gridOptions)).Worker()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := f.p.Run(f.ctx, gridKey, f.options)
		if err != nil {
			t.Fatal(err)
		}
		page, err := f.p.RunPage(f.ctx, gridKey, cql.StartPage(), 2, f.options)
		if err != nil {
			t.Fatal(err)
		}
		qpage, err := q.RunPage(f.ctx, gridKey, cql.StartPage(), 2)
		if err != nil {
			t.Fatal(err)
		}
		sq, err := f.ps.Worker(execopt.StreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := sq.Run(f.ctx, gridKey)
		if err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- f.s.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("Session.Close returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Session.Close waited for open handles")
		}
		if err := f.s.Close(); err != nil {
			t.Errorf("second Session.Close returned %v", err)
		}
		var dst []byte
		sdst := make([]byte, 0, 16)
		for _, h := range []struct {
			name  string
			next  func() bool
			err   func() error
			close func() error
		}{
			{"Rows", func() bool { return rows.NextInto(&dst) }, rows.Err, rows.Close},
			{"Page", func() bool { return page.NextInto(&dst) }, page.Err, page.Close},
			{"Query Page", func() bool { return qpage.NextInto(&dst) }, qpage.Err, qpage.Close},
			{"RowStream", func() bool { return stream.NextInto(&sdst) }, stream.Err, stream.Close},
		} {
			if h.next() {
				t.Errorf("%s: NextInto returned a row after Session.Close", h.name)
			}
			if err := h.err(); !errors.Is(err, cqlerr.ErrClosed) {
				t.Errorf("%s: Err returned %v, want cqlerr.ErrClosed", h.name, err)
			}
			closeTwice(t, h.name, h.close, cqlerr.ErrClosed)
		}
		q.Close()
		if err := q.Wait(f.ctx); err != nil {
			t.Errorf("Worker.Wait after Session.Close returned %v", err)
		}
		sq.Close()
		if err := sq.Wait(f.ctx); err != nil {
			t.Errorf("StreamWorker.Wait after Session.Close returned %v", err)
		}
		if _, err := q.Exec(f.ctx, gridKey); !errors.Is(err, cqlerr.ErrClosed) {
			t.Errorf("Worker.Exec after Session.Close returned %v, want cqlerr.ErrClosed", err)
		}
		if _, err := f.p.Exec(f.ctx, gridKey); !errors.Is(err, cqlerr.ErrClosed) {
			t.Errorf("Prepared.Exec after Session.Close returned %v, want cqlerr.ErrClosed", err)
		}
		if _, err := f.s.Prepare(f.ctx, gridSelect()); !errors.Is(err, cqlerr.ErrClosed) {
			t.Errorf("Session.Prepare after Session.Close returned %v, want cqlerr.ErrClosed", err)
		}
	})

	// TestHandleLifetimes/CloseWaitsForCallback checks that Session.Close
	// does not return while an Each callback or an All loop body runs, since
	// the row it borrows stays valid until it returns, and that the call then
	// ends with cqlerr.ErrClosed without running the callback again.
	t.Run("CloseWaitsForCallback", func(t *testing.T) {
		for _, call := range []string{"Each", "All"} {
			t.Run(call, func(t *testing.T) {
				f := newLifetimeFixture(t)
				entered, release := make(chan struct{}), make(chan struct{})
				calls := 0
				body := func() {
					calls++
					if calls == 1 {
						close(entered)
						<-release
					}
				}
				var dst []byte
				done := make(chan error, 1)
				go func() {
					if call == "Each" {
						_, err := f.p.Each(f.ctx, gridKey, &dst, func(*[]byte) error { body(); return nil }, f.options)
						done <- err
						return
					}
					var err error
					for _, e := range f.p.All(f.ctx, gridKey, &dst, f.options) {
						if e != nil {
							err = e
							break
						}
						body()
					}
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("callback never ran")
				}
				closed := make(chan error, 1)
				go func() { closed <- f.s.Close() }()
				select {
				case <-closed:
					t.Fatal("Session.Close returned while a callback ran")
				case <-time.After(50 * time.Millisecond):
				}
				close(release)
				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("Session.Close returned %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Session.Close did not return after the callback")
				}
				if err := <-done; !errors.Is(err, cqlerr.ErrClosed) {
					t.Errorf("%s returned %v, want cqlerr.ErrClosed", call, err)
				}
				if calls != 1 {
					t.Errorf("callback ran %d times, want 1", calls)
				}
			})
		}
	})

	// TestHandleLifetimes/OpenAcrossQueryClose checks that Rows and a Page of
	// a Worker, and a RowStream of a StreamWorker, left open while the worker
	// closes report cqlerr.ErrClosed, and that Wait then returns nil.
	t.Run("OpenAcrossQueryClose", func(t *testing.T) {
		for _, kind := range []string{"Rows", "Page", "RowStream"} {
			t.Run(kind, func(t *testing.T) {
				f := newLifetimeFixture(t)
				q, err := f.p.With(f.s.MustCompileOptions(gridOptions)).Worker()
				if err != nil {
					t.Fatal(err)
				}
				var next func(*[]byte) bool
				var errOf, closeOf func() error
				waitOf := q.Wait
				closeQuery := q.Close
				switch kind {
				case "RowStream":
					sq, err := f.ps.Worker(execopt.StreamOptions{})
					if err != nil {
						t.Fatal(err)
					}
					stream, err := sq.Run(f.ctx, gridKey)
					if err != nil {
						t.Fatal(err)
					}
					next, errOf, closeOf = stream.NextInto, stream.Err, stream.Close
					waitOf, closeQuery = sq.Wait, sq.Close
				case "Rows":
					rows, err := q.Run(f.ctx, gridKey)
					if err != nil {
						t.Fatal(err)
					}
					next, errOf, closeOf = rows.NextInto, rows.Err, rows.Close
				default:
					page, err := q.RunPage(f.ctx, gridKey, cql.StartPage(), 2)
					if err != nil {
						t.Fatal(err)
					}
					next, errOf, closeOf = page.NextInto, page.Err, page.Close
				}
				closeQuery()
				dst := make([]byte, 0, 16)
				if next(&dst) {
					t.Error("NextInto returned a row after Worker.Close")
				}
				if err := errOf(); !errors.Is(err, cqlerr.ErrClosed) {
					t.Errorf("Err returned %v, want cqlerr.ErrClosed", err)
				}
				closeTwice(t, kind, closeOf, cqlerr.ErrClosed)
				if err := waitOf(f.ctx); err != nil {
					t.Errorf("Wait returned %v", err)
				}
			})
		}
	})
}
