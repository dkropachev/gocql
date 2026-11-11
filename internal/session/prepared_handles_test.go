// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// answerOneShot answers the next EXECUTE with rows, reporting its page size.
func answerOneShot(t *testing.T, peer net.Conn, requests chan oneShotRequest, rows ...string) chan int32 {
	sizes := make(chan int32, 1)
	go func() {
		r, ok := <-requests
		if !ok {
			return
		}
		sizes <- r.pageSize
		_, _ = peer.Write(typedTestResponse(t, 4, r.stream, runtimeRowsBody(nil, rows...)))
	}()
	return sizes
}

// waitPooledIdle waits until s has no busy pooled query, running the
// garbage collector meanwhile.
func waitPooledIdle(t *testing.T, s *cluster.Core) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for pooledBusy(s) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d pooled queries still busy", pooledBusy(s))
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
}

// dropPreparedHandle opens rows, or a page, on p and drops it without Close.
//
//go:noinline
func dropPreparedHandle(t *testing.T, p *Prepared[int32, []byte], page bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if page {
		if _, err := p.RunPage(ctx, 1, model.StartPage(), 0); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := p.Run(ctx, 1); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedHandles(t *testing.T) {
	setup := func(t *testing.T) (*cluster.Core, net.Conn, chan oneShotRequest, *Prepared[int32, []byte]) {
		s, _, peers := runtimeTestSession(t, 1)
		t.Cleanup(func() { _ = peers[0].Close() })
		requests := make(chan oneShotRequest, 4)
		return s, peers[0], requests, oneShotPrepare(t, s, peers[0], requests)
	}

	t.Run("RunReturnsQueryOnClose", func(t *testing.T) {
		s, peer, requests, p := setup(t)
		defer s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		answerOneShot(t, peer, requests, "a", "b")
		rows, err := p.Run(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if pooledBusy(s) != 1 {
			t.Fatalf("open rows hold %d pooled queries", pooledBusy(s))
		}
		var dst []byte
		var got []string
		for rows.NextInto(&dst) {
			got = append(got, string(dst))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Fatalf("rows %q", got)
		}
		waitIdle(t, p, 1)
		if pooledBusy(s) != 0 || liveQueries(InternalRuntimeFor(s)) != 0 {
			t.Fatal("closed rows left their query busy or registered")
		}
		// A closed handle stays inert after its query serves another call.
		answerOneShot(t, peer, requests, "c")
		var other []byte
		if found, _, err := p.One(ctx, 2, &other); err != nil || !found || string(other) != "c" {
			t.Fatalf("reuse: %t %q %v", found, other, err)
		}
		if rows.NextInto(&dst) || rows.Close() != nil {
			t.Fatal("closed rows came back to life")
		}
	})

	t.Run("ConcurrentHandles", func(t *testing.T) {
		s, peer, requests, p := setup(t)
		defer s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		answerOneShot(t, peer, requests, "a")
		first, err := p.Run(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		answerOneShot(t, peer, requests, "b")
		second, err := p.RunPage(ctx, 2, model.StartPage(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if pooledBusy(s) != 2 {
			t.Fatalf("two open handles hold %d pooled queries", pooledBusy(s))
		}
		_ = first.Close()
		_ = second.Close()
		waitIdle(t, p, 2)
	})

	t.Run("RunPageSize", func(t *testing.T) {
		s, peer, requests, p := setup(t)
		defer s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sizes := answerOneShot(t, peer, requests, "a")
		page, err := p.RunPage(ctx, 1, model.StartPage(), 9)
		if err != nil {
			t.Fatal(err)
		}
		if size := <-sizes; size != 9 {
			t.Fatalf("page size on the wire %d", size)
		}
		if page.State().More() {
			t.Fatal("last page reports more")
		}
		_ = page.Close()
		waitIdle(t, p, 1)
	})

	// TestPreparedHandles/SessionCloseStopsOpenHandle checks that Session.Close
	// stops rows a caller still holds instead of waiting for them.
	t.Run("SessionCloseStopsOpenHandle", func(t *testing.T) {
		s, peer, requests, p := setup(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		answerOneShot(t, peer, requests, "a", "b")
		rows, err := p.Run(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		closed := make(chan struct{})
		go func() {
			s.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Session.Close waits for open pooled rows")
		}
		var dst []byte
		for rows.NextInto(&dst) {
		}
		if err := rows.Err(); !errors.Is(err, runtimeerrors.ErrClosed) {
			t.Fatalf("rows after Session.Close: %v", err)
		}
		_ = rows.Close()
	})

	// TestPreparedHandles/DroppedHandle checks that rows or a page dropped
	// without Close release their pooled query once collected, so
	// Session.Close does not wait for them.
	for _, page := range []bool{false, true} {
		name := "DroppedRows"
		if page {
			name = "DroppedPage"
		}
		t.Run(name, func(t *testing.T) {
			s, peer, requests, p := setup(t)
			answerOneShot(t, peer, requests, "a")
			dropPreparedHandle(t, p, page)
			waitPooledIdle(t, s)
			if registeredQueries(InternalRuntimeFor(s)) != 0 {
				t.Fatal("collected query still registered")
			}
			closed := make(chan struct{})
			go func() {
				s.Close()
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Session.Close waits for a dropped handle")
			}
		})
	}
}
