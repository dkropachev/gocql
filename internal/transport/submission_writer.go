// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/syncx"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// A ticket has two owners: its waiting caller and the persistent writer. Only
// the last owner recycles it. Cancellation can stop waiting without retiring
// bytes still accepted by the socket writer.
type writeTicket struct {
	ctx   context.Context
	lease *wire.Lease
	// wake is signalled by finish only when the caller is blocked in
	// awaitWritten; completion is otherwise published through state alone.
	wake chan struct{}
	// result is written by the writer before state becomes ticketFinished.
	result InternalWriteResult
	packet wire.Chain
	// request owns a typed request until the writer finishes with it. Its
	// lease is released in finish, before the ticket can be recycled, and no
	// one else holds it, so reusing it with the ticket is safe.
	request typedRequestLease
	refs    atomic.Int32
	phase   atomic.Uint32 // 0 queued, 1 writing, 2 cancelled before transmission
	state   atomic.Uint32 // ticketPending, ticketAwaited or ticketFinished
}

const (
	ticketPending  = iota
	ticketAwaited  // the caller is blocked on wake
	ticketFinished // result is published
)

var ticketPool = syncx.NewPool(func() *writeTicket { return &writeTicket{wake: make(chan struct{}, 1)} })

func (t *writeTicket) release() {
	if t.refs.Add(-1) != 0 {
		return
	}
	t.ctx, t.packet, t.lease, t.result = nil, wire.Chain{}, nil, InternalWriteResult{}
	t.request = typedRequestLease{}
	ticketPool.Put(t)
}
func (t *writeTicket) finish(result InternalWriteResult) {
	t.lease.Release()
	t.lease = nil
	t.result = result
	if t.state.Swap(ticketFinished) == ticketAwaited {
		t.wake <- struct{}{}
	}
	t.release()
}

// pendingWriteError means the writer may still be transmitting. The stream ID
// must become an orphan, and only writer completion may release request storage.
type pendingWriteError struct{ error }

func (e *pendingWriteError) Unwrap() error { return e.error }

// submissionWriter is a connection's persistent writer. Submitters append to
// queue under mu and wake the writer only when it is parked.
type submissionWriter struct {
	conn   *Conn
	wake   chan struct{} // capacity 1; signalled when a parked writer has work
	queue  []*writeTicket
	spare  []*writeTicket // the previous batch's storage, reused as the next queue
	mu     sync.Mutex
	parked bool
	closed bool
}

// transientDoner is implemented by contexts that hand out their Done channel
// differently to waits that end before the current attempt returns.
type transientDoner interface {
	InternalTransientDone() <-chan struct{}
}

// waker is implemented by contexts that send a wake (InternalWake) into a
// channel of their own when cancelled, instead of only closing Done.
type waker interface {
	InternalWakeChannel() chan InternalCallResp
}

// attemptWake returns ctx's wake channel, or nil.
func attemptWake(ctx context.Context) chan InternalCallResp {
	if w, ok := ctx.(waker); ok {
		return w.InternalWakeChannel()
	}
	return nil
}

// parentDoner is implemented by contexts that watch a cancellable parent
// without propagating its cancellation into their own Done channel.
type parentDoner interface {
	InternalParentDone() <-chan struct{}
}

// attemptParentDone returns the Done channel such a context watches, or nil.
// A wait that selects on attemptDone selects on it too.
func attemptParentDone(ctx context.Context) <-chan struct{} {
	if p, ok := ctx.(parentDoner); ok {
		return p.InternalParentDone()
	}
	return nil
}

// attemptDone returns ctx's Done channel for a wait bounded by one attempt.
func attemptDone(ctx context.Context) <-chan struct{} {
	if t, ok := ctx.(transientDoner); ok {
		return t.InternalTransientDone()
	}
	return ctx.Done()
}

func (c *Conn) submissionWriter() *submissionWriter {
	c.typedWriterOnce.Do(func() {
		c.typedWriter = &submissionWriter{conn: c, wake: make(chan struct{}, 1)}
		go c.typedWriter.run()
	})
	return c.typedWriter
}

// submitTyped queues packet for the persistent writer and returns at once;
// the caller then waits only for the response. The returned ticket holds one
// reference for the caller, released with release. An error means the packet
// was never queued: nothing was written.
func (c *Conn) submitTyped(ctx context.Context, packet wire.Chain, lease *wire.Lease) (*writeTicket, error) {
	return c.enqueueTyped(ctx, getTicket(), packet, lease)
}

// getTicket returns an unused ticket, for a caller that builds its request in
// the ticket's embedded lease before enqueueing it.
func getTicket() *writeTicket { return ticketPool.Get() }

// enqueueTyped is submitTyped for a ticket from getTicket.
func (c *Conn) enqueueTyped(ctx context.Context, t *writeTicket, packet wire.Chain, lease *wire.Lease) (*writeTicket, error) {
	w := c.submissionWriter()
	t.refs.Store(2)
	t.phase.Store(0)
	t.state.Store(ticketPending)
	t.ctx, t.packet, t.lease = ctx, packet, lease
	reject := func(err error) (*writeTicket, error) {
		t.finish(InternalWriteResult{InternalErr: err})
		t.release()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return reject(err)
	}
	if c.InternalCtx.Err() != nil {
		return reject(runtimeerrors.ErrConnectionClosed)
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return reject(runtimeerrors.ErrConnectionClosed)
	}
	w.queue = append(w.queue, t)
	wake := w.parked
	w.parked = false
	w.mu.Unlock()
	if wake {
		select {
		case w.wake <- struct{}{}:
		default: // a signal is already pending
		}
	}
	return t, nil
}

// awaitWritten blocks until the writer finished with this ticket, so that a
// caller returning a response has also released the request's input, as when
// it waited for the write first. A response follows its write by a network
// round trip, so the writer has almost always finished already.
func (t *writeTicket) awaitWritten() {
	if t.state.Load() == ticketFinished {
		return
	}
	if t.state.CompareAndSwap(ticketPending, ticketAwaited) {
		<-t.wake
	}
}

// unsent withdraws a ticket the writer has not started, or reports that the
// writer already dropped it unsent. Either way no byte reached the socket.
func (t *writeTicket) unsent() bool {
	return t.phase.CompareAndSwap(0, 2) || t.phase.Load() == 2
}

// written reports how many bytes of this ticket the writer transmitted. The
// writer publishes the result before it closes the connection on a write
// error, so a caller woken by that close always finds it. ok is false while
// the write is still in progress.
func (t *writeTicket) written() (n int64, ok bool) {
	if t.state.Load() != ticketFinished {
		return 0, false
	}
	return t.result.InternalWritten, true
}

// maxBatchTickets bounds the requests in one socket write.
const maxBatchTickets = 16

func (w *submissionWriter) run() {
	spans := make([]wire.Span, 0, 64)
	for {
		batch, ok := w.take()
		if !ok {
			return
		}
		// Each socket write carries at most maxBatchTickets requests and 64
		// KiB; a large first frame is sent alone.
		for start := 0; start < len(batch); {
			end, total := start, int64(0)
			spans = spans[:0]
			for end < len(batch) && end-start < maxBatchTickets {
				t := batch[end]
				if total > 0 && t.packet.Len() > (64<<10)-total {
					break
				}
				end++
				if t.ctx.Err() != nil || w.conn.InternalCtx.Err() != nil {
					t.phase.CompareAndSwap(0, 2)
				}
				if !t.phase.CompareAndSwap(0, 1) {
					err := t.ctx.Err()
					if err == nil {
						err = runtimeerrors.ErrConnectionClosed
					}
					t.finish(InternalWriteResult{InternalErr: err})
					batch[end-1] = nil
					continue
				}
				_ = t.packet.Each(func(s wire.Span) error { spans = append(spans, s); return nil })
				total += t.packet.Len()
			}
			var written int64
			var err error
			if total > 0 {
				if sw, ok := w.conn.InternalW.(spanSliceWriter); ok {
					written, err = sw.InternalWriteSpanSlice(w.conn.InternalCtx, spans, total)
				} else {
					packet, buildErr := wire.NewChain(spans...)
					err = buildErr
					if err == nil {
						written, err = w.conn.InternalW.(InternalSpanContextWriter).InternalWriteSpans(w.conn.InternalCtx, packet)
					}
				}
			}
			remaining := written
			for i := start; i < end; i++ {
				t := batch[i]
				batch[i] = nil
				if t == nil {
					continue
				}
				n := min(max(remaining, 0), t.packet.Len())
				requestErr := err
				if n == t.packet.Len() {
					requestErr = nil
				}
				t.finish(InternalWriteResult{InternalWritten: n, InternalErr: requestErr})
				remaining -= n
			}
			clear(spans)
			// Retain enough descriptors for a full batch; drop only outliers.
			if cap(spans) > maxRetainedWriteSpans {
				spans = nil
			}
			// Publish completions before Close joins the calls waiting on us.
			if err != nil {
				// Close joins every call of the connection, and a call waits
				// for its ticket: finish the rest of this batch and the queued
				// tickets unsent first, and stop accepting more.
				for i := end; i < len(batch); i++ {
					if t := batch[i]; t != nil {
						batch[i] = nil
						t.phase.CompareAndSwap(0, 2)
						t.finish(InternalWriteResult{InternalErr: runtimeerrors.ErrConnectionClosed})
					}
				}
				w.shutdown()
				w.conn.InternalCloseWithError(err)
				return
			}
			start = end
		}
		w.recycle(batch)
	}
}

// take waits for queued tickets and takes all of them. ok is false once the
// connection closed; every ticket still queued then is finished unsent.
//
// Waking this writer puts it next in line on its P, ahead of every runnable
// submitter, so taking the queue at once would find only the request that
// woke it and pay one syscall per request. When more requests are in flight
// than queued, the writer yields once so runnable submitters enqueue first.
// There is no timer and no wait for future requests, and a lone caller never
// yields.
func (w *submissionWriter) take() (batch []*writeTicket, ok bool) {
	w.mu.Lock()
	for len(w.queue) == 0 {
		w.parked = true
		w.mu.Unlock()
		select {
		case <-w.conn.InternalCtx.Done():
			w.shutdown()
			return nil, false
		case <-w.wake:
		}
		w.mu.Lock()
	}
	if n := len(w.queue); n < maxBatchTickets && w.conn.InternalStreams.InUse() > n {
		w.mu.Unlock()
		runtime.Gosched()
		w.mu.Lock()
	}
	batch, w.queue, w.spare = w.queue, w.spare, nil
	w.mu.Unlock()
	return batch, true
}

// recycle keeps a finished batch's storage for the next queue, dropping
// outliers so a burst does not pin a large array.
func (w *submissionWriter) recycle(batch []*writeTicket) {
	if cap(batch) > 4*maxBatchTickets {
		return
	}
	clear(batch)
	w.mu.Lock()
	if w.spare == nil {
		w.spare = batch[:0]
	}
	w.mu.Unlock()
}

// shutdown stops accepting tickets and finishes the queued ones unsent.
func (w *submissionWriter) shutdown() {
	w.mu.Lock()
	w.closed = true
	queued := w.queue
	w.queue, w.spare = nil, nil
	w.mu.Unlock()
	for _, t := range queued {
		t.phase.CompareAndSwap(0, 2)
		t.finish(InternalWriteResult{InternalErr: runtimeerrors.ErrConnectionClosed})
	}
}
