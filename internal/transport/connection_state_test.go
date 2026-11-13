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
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// These tests pin what an exclusive stream reservation waits for, and that a
// connection's admission counters return to zero, independent of how shared
// admission and stream release are implemented.

// receiveWithin returns the next value from ch, failing the test if none
// arrives within five seconds.
func receiveWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// connCounters reads streams in use, registered calls, orphans and shared ops.
func connCounters(c *Conn) (streams, calls, orphans, shared int) {
	shared = c.InternalSharedOps()
	return c.InternalStreams.InUse(), c.InternalCalls.Len(), int(c.orphans.Load()), shared
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
	if err := server.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
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
	if err := receiveWithin(t, done, "the cancelled request"); !errors.Is(err, context.Canceled) {
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

// finishHook runs fn when a stream finishes.
type finishHook struct{ fn func() }

func (h finishHook) StreamContext(context.Context) StreamObserverContext { return h }
func (finishHook) StreamStarted(ObservedStream)                          {}
func (finishHook) StreamAbandoned(ObservedStream)                        {}
func (h finishHook) StreamFinished(ObservedStream)                       { h.fn() }

func TestExclusiveWaitsForCallerCleanup(t *testing.T) {
	c, server := invPair(t, 4)
	finished := 0
	c.InternalStreamObserver = finishHook{fn: func() {
		finished++
		expectReservationBlocked(t, c, "the caller is cleaning up its call")
	}}
	go c.InternalServe(c.InternalCtx)
	go func() {
		stream, err := invReadRequest(server)
		if err != nil {
			return
		}
		server.Write(invFrame(4, stream, typedTestRowsBody([]byte("v"))))
	}()
	page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
	if err != nil {
		t.Fatal(err)
	}
	page.Owner.Release()
	if finished != 1 {
		t.Fatalf("stream finished %d times, want 1", finished)
	}
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
	stream := receiveWithin(t, requested, "the request")
	cancel()
	if err := receiveWithin(t, done, "the cancelled request"); !errors.Is(err, context.Canceled) {
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
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	var pending sync.WaitGroup
	go func() {
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
		for {
			select {
			case stream := <-responses:
				server.Write(invFrame(4, stream, typedTestRowsBody([]byte("v"))))
				pending.Done()
			case <-stop:
				return
			}
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
	answered := make(chan struct{})
	go func() { pending.Wait(); close(answered) }()
	receiveWithin(t, answered, "the server's responses")
}

func TestConnCountersReturnToZero(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	runMixedRequests(t, c, server)
	expectReservation(t, c)
}

func TestStreamObserverOncePerStream(t *testing.T) {
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

// The reader unregisters a live call; the caller frees its stream in its
// cleanup, so a delivered call keeps its stream until then.
func TestCallerFreesDeliveredStream(t *testing.T) {
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
	if s, calls, _, _ := connCounters(c); s != 1 || calls != 0 {
		t.Fatalf("before cleanup: streams %d, calls %d, want 1 and 0", s, calls)
	}
	x.cleanup(req)
	page.Owner.Release()
	if s, calls, o, shared := connCounters(c); s != 0 || calls != 0 || o != 0 || shared != 0 {
		t.Fatalf("after cleanup: streams %d, calls %d, orphans %d, shared %d", s, calls, o, shared)
	}
}

// A typed request on a healthy connection, the reader's delivery included,
// never takes the connection lock: requests complete while a test holds it.
func TestTypedRequestTakesNoConnLock(t *testing.T) {
	c, server := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	go func() {
		for {
			stream, err := invReadRequest(server)
			if err != nil {
				return
			}
			server.Write(invFrame(4, stream, typedTestRowsBody([]byte("v"))))
		}
	}()
	c.InternalMu.Lock()
	done := make(chan error, 1)
	go func() {
		for range 50 {
			page, err := c.InternalExecuteTyped(context.Background(), &model.AttemptRequest{Statement: "SELECT value", Consistency: cqltype.One})
			if err != nil {
				done <- err
				return
			}
			page.Owner.Release()
		}
		done <- nil
	}()
	select {
	case err := <-done:
		c.InternalMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		c.InternalMu.Unlock()
		t.Fatal("typed requests blocked on the connection lock")
	}
	// The writer may drop its ticket reference, and with it the last shared
	// admission, after the caller returned.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, calls, o, shared := connCounters(c)
		if s == 0 && calls == 0 && o == 0 && shared == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("counters: streams %d, calls %d, orphans %d, shared %d", s, calls, o, shared)
		}
		time.Sleep(time.Millisecond)
	}
}

// A reservation already waiting for an orphan's stream is woken by the free
// that follows the orphan's late response.
func TestStreamFreeWakesWaitingReservation(t *testing.T) {
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
	reserved := make(chan error, 1)
	go func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rcancel()
		r, err := c.InternalReserveExclusive(rctx)
		if err == nil {
			err = r.InternalClose(true)
		}
		reserved <- err
	}()
	// Let the reservation set its flag and find the orphan's stream in use.
	deadline := time.Now().Add(5 * time.Second)
	for !c.InternalExclusive() {
		if time.Now().After(deadline) {
			t.Fatal("reservation never started")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	select {
	case err := <-reserved:
		t.Fatalf("reservation finished while the orphan held its stream: %v", err)
	default:
	}
	if _, err := server.Write(invFrame(4, stream, typedTestRowsBody(nil))); err != nil {
		t.Fatal(err)
	}
	if err := <-reserved; err != nil {
		t.Fatalf("reservation: %v", err)
	}
}

// A misplaced call the reader cannot put back is answered by the reader: its
// slot was taken, or close may already have drained the table.
func TestMisplacedCallAnsweredWhenNotPutBack(t *testing.T) {
	expectAnswered := func(t *testing.T, call *InternalCallReq) {
		t.Helper()
		select {
		case resp := <-call.InternalResp:
			if resp.InternalErr == nil || resp.removedFromCalls {
				t.Fatalf("got %+v, want an error without the stream", resp)
			}
		default:
			t.Fatal("misplaced call not answered")
		}
	}
	t.Run("SlotTaken", func(t *testing.T) {
		c, _ := invPair(t, 4)
		call, other := getCallReq(7), getCallReq(5)
		c.InternalCalls.Set(5, other)
		if err := c.misplacedCall(call, 5); err == nil {
			t.Fatal("no error")
		}
		expectAnswered(t, call)
		if c.InternalCalls.Get(5) != other {
			t.Fatal("the slot's own call was replaced")
		}
		c.InternalCalls.Delete(5)
	})
	t.Run("Closed", func(t *testing.T) {
		c, _ := invPair(t, 4)
		call := getCallReq(7)
		c.admission.Or(admissionClosed)
		if err := c.misplacedCall(call, 5); err == nil {
			t.Fatal("no error")
		}
		expectAnswered(t, call)
		if n := c.InternalCalls.Len(); n != 0 {
			t.Fatalf("%d calls left in the table", n)
		}
	})
	t.Run("PutBack", func(t *testing.T) {
		c, _ := invPair(t, 4)
		call := getCallReq(7)
		if err := c.misplacedCall(call, 5); err == nil {
			t.Fatal("no error")
		}
		if c.InternalCalls.Get(5) != call {
			t.Fatal("misplaced call not put back")
		}
		c.InternalCalls.Delete(5)
	})
}

// Registrations and withdrawals racing close: every call registered is either
// withdrawn by its caller or drained and answered by close, a registration
// after close fails with ErrConnectionClosed, and the table ends empty.
// Streams held outside the table push the workers' streams into chunks that
// are not yet published, so chunk publication races close too.
func TestCallTableCloseRace(t *testing.T) {
	closeErr := errors.New("test close")
	published := 0
	var answered atomic.Int32 // calls close took over and answered
	for round := range 200 {
		c, _ := invPair(t, 4)
		held := callChunkSize*(1+round%4) - round%8
		for range held {
			c.InternalStreams.GetStream()
		}
		fresh := held / callChunkSize // no chunk is published yet: held streams are not registered
		const workers = 16
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range workers {
			wg.Go(func() {
				<-start
				for {
					call := getCallReq(0)
					call.typed = true
					if err := c.allocCall(call); err != nil {
						if !errors.Is(err, runtimeerrors.ErrConnectionClosed) {
							t.Errorf("round %d: allocCall: %v", round, err)
						}
						call.finishExec()
						putCallReq(call)
						return
					}
					if c.InternalRemoveCallIfOpen(call) {
						c.releaseStream(call)
						call.finishExec()
						putCallReq(call)
						continue
					}
					// Close owns the call: it answers, then waits for us.
					select {
					case resp := <-call.InternalResp:
						if !errors.Is(resp.InternalErr, closeErr) {
							t.Errorf("round %d: drained call got %v", round, resp.InternalErr)
						}
						answered.Add(1)
					case <-time.After(5 * time.Second):
						t.Errorf("round %d: registered call never answered after close", round)
					}
					call.finishExec()
					return
				}
			})
		}
		close(start)
		if round%3 != 0 {
			// Otherwise close races the first registrations, chunk publication
			// included; rounds with held%256 != 0 also cross a chunk boundary.
			time.Sleep(time.Duration(rand.IntN(200)) * time.Microsecond)
		}
		c.InternalCloseWithError(closeErr)
		wg.Wait()
		if n := c.InternalCalls.Len(); n != 0 {
			t.Fatalf("round %d: %d calls left in the table after close", round, n)
		}
		if c.InternalCalls.chunks[fresh].Load() != nil {
			published++
		}
		if t.Failed() {
			return
		}
	}
	if published == 0 {
		t.Fatal("no round registered into a chunk beyond the held streams")
	}
	if answered.Load() == 0 {
		t.Fatal("close never took over a registered call")
	}
	t.Logf("%d rounds published a chunk while racing close", published)
}

// A reservation takes one connection out of shared use; the session-wide
// receive cache belongs to every other connection too, so it must keep its
// idle buffers.
func TestExclusiveReservationKeepsReceiveCache(t *testing.T) {
	c, _ := invPair(t, 4)
	go c.InternalServe(c.InternalCtx)
	cache := c.InternalCfg.InternalReceiveCache
	buf, epoch := cache.Acquire(8192)
	cache.Release(buf, epoch)
	if n, _ := cache.Storage(); n == 0 {
		t.Fatal("cache did not retain a released buffer")
	}
	expectReservation(t, c)
	if n, objects := cache.Storage(); n == 0 || objects != 1 {
		t.Fatalf("reservation drained the receive cache: %d bytes, %d objects", n, objects)
	}
	reused, _ := cache.Acquire(8192)
	if &reused[0] != &buf[0] {
		t.Fatal("cached buffer not reused after reservation")
	}
}
