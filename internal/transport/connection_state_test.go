// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// These tests pin what an exclusive stream reservation waits for, and that a
// connection's admission counters return to zero, independent of how shared
// admission and stream release are implemented.

// connSharedOps reads the connection's shared-operation count.
func connSharedOps(c *Conn) int {
	return c.InternalSharedOps()
}

// connCounters reads streams in use, registered calls, orphans and shared ops.
func connCounters(c *Conn) (streams, calls, orphans, shared int) {
	shared = connSharedOps(c)
	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()
	return c.InternalStreams.InUse(), c.InternalCalls.Len(), c.orphans, shared
}

// expectReservationBlocked fails unless a reservation attempt times out.
func expectReservationBlocked(t *testing.T, c *Conn, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	r, err := c.InternalReserveExclusive(ctx)
	if err == nil {
		r.InternalClose(true)
		t.Fatalf("reservation succeeded while %s", what)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reservation while %s: %v, want deadline exceeded", what, err)
	}
	if c.InternalExclusive() {
		t.Fatal("a failed reservation kept the connection exclusive")
	}
}

// expectReservation fails unless a reservation succeeds within a second.
func expectReservation(t *testing.T, c *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.InternalReserveExclusive(ctx)
	if err != nil {
		s, calls, o, shared := connCounters(c)
		t.Fatalf("reservation: %v (streams %d, calls %d, orphans %d, shared %d)", err, s, calls, o, shared)
	}
	if err := r.InternalClose(true); err != nil {
		t.Fatal(err)
	}
}

func TestExclusiveWaitsForQueuedTicket(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	// Occupy the writer with a packet the server does not read yet, so the next
	// request stays queued behind it.
	blocker := make([]byte, 64)
	chain, err := wire.NewChain(wire.Borrow(blocker))
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := c.submitTyped(context.Background(), chain, wire.NewLease(chain, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer ticket.release()
	// Reading the first byte proves the writer is writing the blocker.
	if _, err := io.ReadFull(server, blocker[:1]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		page, err := c.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
		if page != nil {
			page.Owner.Release()
		}
		done <- err
	}()
	// Wait until the request is queued behind the blocker.
	for deadline := time.Now().Add(time.Second); ; {
		w := c.submissionWriter()
		w.mu.Lock()
		queued := len(w.queue)
		w.mu.Unlock()
		if queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request never queued")
		}
		time.Sleep(time.Millisecond)
	}
	// Cancel the queued request: its caller returns and frees the stream, but
	// the writer still holds the ticket and the shared admission with it.
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	expectReservationBlocked(t, c, "a ticket is queued in the writer")
	if _, err := io.ReadFull(server, blocker[1:]); err != nil {
		t.Fatal(err)
	}
	expectReservation(t, c)
}

func TestExclusiveWaitsForDeliveredButNotCleanedUp(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	go func() {
		stream, err := invReadRequest(server)
		if err != nil {
			return
		}
		server.Write(invFrame(4, stream, typedTestRowsBody([]byte("v"))))
	}()
	req := &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One}
	x := typedExec{conn: c}
	page, err := x.run(context.Background(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The response is delivered; the caller has not run its cleanup.
	expectReservationBlocked(t, c, "a delivered call is not cleaned up")
	x.cleanup(req)
	page.Owner.Release()
	expectReservation(t, c)
}

func TestExclusiveWaitsForOrphan(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	requested := make(chan int, 1)
	go func() {
		stream, err := invReadRequest(server)
		if err != nil {
			return
		}
		requested <- stream
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		page, err := c.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
		if page != nil {
			page.Owner.Release()
		}
		done <- err
	}()
	stream := <-requested
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	expectReservationBlocked(t, c, "an orphan's response is outstanding")
	if _, err := server.Write(invFrame(4, stream, typedTestRowsBody(nil))); err != nil {
		t.Fatal(err)
	}
	expectReservation(t, c)
}

// countingObserver records, per allocated stream, how many times it ended.
type countingObserver struct {
	mu       sync.Mutex
	contexts []*countingStream
}

type countingStream struct {
	started, ended atomic.Int32
}

func (o *countingObserver) StreamContext(context.Context) StreamObserverContext {
	s := new(countingStream)
	o.mu.Lock()
	o.contexts = append(o.contexts, s)
	o.mu.Unlock()
	return s
}

func (s *countingStream) StreamStarted(ObservedStream)   { s.started.Add(1) }
func (s *countingStream) StreamAbandoned(ObservedStream) { s.ended.Add(1) }
func (s *countingStream) StreamFinished(ObservedStream)  { s.ended.Add(1) }

// runMixedRequests issues requests from 16 goroutines that succeed, time out,
// or are cancelled, against a server that answers some of them late.
func runMixedRequests(t *testing.T, c *Conn, server net.Conn) {
	t.Helper()
	responses := make(chan int, 4096)
	serverDone := make(chan struct{})
	var pending sync.WaitGroup
	go func() {
		defer close(serverDone)
		for {
			stream, err := invReadRequest(server)
			if err != nil {
				return
			}
			pending.Add(1)
			if rand.IntN(3) == 0 {
				time.AfterFunc(time.Duration(rand.IntN(3000))*time.Microsecond, func() { responses <- stream })
			} else {
				responses <- stream
			}
		}
	}()
	go func() {
		for stream := range responses {
			server.Write(invFrame(4, stream, typedTestRowsBody([]byte("v"))))
			pending.Done()
		}
	}()
	const workers, perWorker = 16, 63
	var wg sync.WaitGroup
	var abandoned atomic.Int32
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				req := &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One}
				ctx := context.Background()
				var cancel context.CancelFunc
				mode := (w + i) % 4
				switch mode {
				case 2:
					req.Timeout = time.Duration(1+rand.IntN(1000)) * time.Microsecond
				case 3:
					ctx, cancel = context.WithCancel(ctx)
					time.AfterFunc(time.Duration(rand.IntN(1500))*time.Microsecond, cancel)
				}
				page, err := c.InternalExecuteTyped(ctx, req)
				if page != nil {
					page.Owner.Release()
				}
				if cancel != nil {
					cancel()
				}
				if mode >= 2 && err != nil {
					abandoned.Add(1)
				}
				if mode < 2 && err != nil {
					t.Error("ordinary request failed:", err)
				}
			}
		})
	}
	wg.Wait()
	t.Logf("%d timed-out or cancelled requests", abandoned.Load())
	// Every request the server read is answered, late ones included.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, calls, o, shared := connCounters(c)
		if s == 0 && calls == 0 && o == 0 && shared == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("counters did not return to zero: streams %d, calls %d, orphans %d, shared %d", s, calls, o, shared)
		}
		time.Sleep(time.Millisecond)
	}
	if c.Closed() {
		t.Fatal("connection closed during the run")
	}
	pending.Wait()
}

func TestConnCountersReturnToZero(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	runMixedRequests(t, c, server)
	expectReservation(t, c)
}

func TestStreamObserverOnceperStream(t *testing.T) {
	c, server := invPair(t, 4)
	observer := new(countingObserver)
	c.InternalStreamObserver = observer
	go c.InternalServe(c.InternalCtx)
	runMixedRequests(t, c, server)
	// A late response's observer runs on the reader after the stream is freed.
	deadline := time.Now().Add(5 * time.Second)
	for {
		observer.mu.Lock()
		contexts := observer.contexts
		observer.mu.Unlock()
		unfinished, extra := 0, 0
		for _, s := range contexts {
			switch n := s.ended.Load(); {
			case n == 0:
				unfinished++
			case n > 1:
				extra++
			}
		}
		if extra != 0 {
			t.Fatalf("%d of %d streams ended more than once", extra, len(contexts))
		}
		if unfinished == 0 {
			if len(contexts) == 0 {
				t.Fatal("no streams observed")
			}
			t.Logf("%d streams each ended once", len(contexts))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d streams never ended", unfinished, len(contexts))
		}
		time.Sleep(time.Millisecond)
	}
}

// The reader frees a live call's stream when it unregisters the call, before
// the caller runs its cleanup.
func TestReaderFreesDeliveredStream(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	go func() {
		stream, err := invReadRequest(server)
		if err != nil {
			return
		}
		server.Write(invFrame(4, stream, typedTestRowsBody([]byte("v"))))
	}()
	req := &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One}
	x := typedExec{conn: c}
	page, err := x.run(context.Background(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s, calls, _, _ := connCounters(c); s != 0 || calls != 0 {
		t.Fatalf("before cleanup: streams %d, calls %d, want 0", s, calls)
	}
	x.cleanup(req)
	page.Owner.Release()
	if s, calls, o, shared := connCounters(c); s != 0 || calls != 0 || o != 0 || shared != 0 {
		t.Fatalf("after cleanup: streams %d, calls %d, orphans %d, shared %d", s, calls, o, shared)
	}
}
