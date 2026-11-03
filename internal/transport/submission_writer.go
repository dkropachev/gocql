// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// A ticket has two owners: its waiting caller and the persistent writer. Only
// the last owner recycles it. Cancellation can stop waiting without retiring
// bytes still accepted by the socket writer.
type writeTicket struct {
	ctx   context.Context
	lease *wire.Lease
	done  chan InternalWriteResult
	// Caller-side cache of the result received from done.
	result      InternalWriteResult
	packet      wire.Chain
	refs        atomic.Int32
	phase       atomic.Uint32 // 0 queued, 1 writing, 2 cancelled before transmission
	resultKnown bool
}

var ticketPool = sync.Pool{New: func() any { return &writeTicket{done: make(chan InternalWriteResult, 1)} }}

func (t *writeTicket) release() {
	if t.refs.Add(-1) != 0 {
		return
	}
	select {
	case <-t.done:
	default:
	}
	t.ctx, t.packet, t.lease = nil, wire.Chain{}, nil
	ticketPool.Put(t)
}
func (t *writeTicket) finish(result InternalWriteResult) {
	t.lease.Release()
	t.lease = nil
	t.done <- result
	t.release()
}

// pendingWriteError means the writer may still be transmitting. The stream ID
// must become an orphan, and only writer completion may release request storage.
type pendingWriteError struct{ error }

func (e *pendingWriteError) Unwrap() error { return e.error }

type submissionWriter struct {
	conn  *Conn
	ready chan *writeTicket
}

// transientDoner is implemented by contexts that hand out their Done channel
// differently to waits that end before the current attempt returns.
type transientDoner interface {
	InternalTransientDone() <-chan struct{}
}

// attemptDone returns ctx's Done channel for a wait bounded by one attempt.
func attemptDone(ctx context.Context) <-chan struct{} {
	if t, ok := ctx.(transientDoner); ok {
		return t.InternalTransientDone()
	}
	return ctx.Done()
}

// submitTyped queues packet for the persistent writer and returns as soon as
// the writer accepted the ticket; the caller then waits only for the response.
// The returned ticket holds one reference for the caller, released with
// release. An error means the packet was never queued: nothing was written.
func (c *Conn) submitTyped(ctx context.Context, packet wire.Chain, lease *wire.Lease) (*writeTicket, error) {
	c.typedWriterOnce.Do(func() {
		c.typedWriter = &submissionWriter{conn: c, ready: make(chan *writeTicket)}
		go c.typedWriter.run()
	})
	t := ticketPool.Get().(*writeTicket)
	t.refs.Store(2)
	t.phase.Store(0)
	t.resultKnown = false
	t.ctx, t.packet, t.lease = ctx, packet, lease
	select {
	case <-attemptDone(ctx):
		t.finish(InternalWriteResult{InternalErr: ctx.Err()})
		t.release()
		return nil, ctx.Err()
	case <-c.InternalCtx.Done():
		t.finish(InternalWriteResult{InternalErr: ErrConnectionClosed})
		t.release()
		return nil, ErrConnectionClosed
	case c.typedWriter.ready <- t:
	}
	return t, nil
}

// awaitWritten blocks until the writer finished with this ticket, so that a
// caller returning a response has also released the request's input, as when
// it waited for the write first. A response follows its write by a network
// round trip, so the writer has almost always finished already.
func (t *writeTicket) awaitWritten() {
	if !t.resultKnown {
		t.result, t.resultKnown = <-t.done, true
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
	if !t.resultKnown {
		select {
		case r := <-t.done:
			t.result, t.resultKnown = r, true
		default:
			return 0, false
		}
	}
	return t.result.InternalWritten, true
}

func (w *submissionWriter) run() {
	var pending [16]*writeTicket
	spans := make([]wire.Span, 0, 64)
	for {
		select {
		case <-w.conn.InternalCtx.Done():
			return
		case first := <-w.ready:
			pending[0] = first
			count, bytes := 1, first.packet.Len()
			// Zero configured coalescing: drain only senders already ready. No
			// timer and no wait for another request. A large first frame stands alone.
			//
			// Waking this writer puts it next in line on its P, ahead of every
			// other runnable submitter, so a plain drain would find only the
			// first request and pay one syscall per request. Yield once when the
			// queue looks empty and another request is in flight: runnable
			// submitters enqueue first. A lone caller never yields.
			yielded := w.conn.InternalStreams.InUse() <= 1
		collect:
			for count < len(pending) && bytes < 64<<10 {
				select {
				case next := <-w.ready:
					// A request that would exceed the byte limit is sent in the
					// next batch; keep it locally without losing its ticket.
					pending[count] = next
					count++
					bytes += next.packet.Len()
					if bytes >= 64<<10 {
						break collect
					}
				default:
					if yielded {
						break collect
					}
					yielded = true
					runtime.Gosched()
				}
			}
			// A batch may include a final frame beyond the threshold above.
			// Split before transmission to keep the strict 64 KiB batch bound.
			for start := 0; start < count; {
				end, total := start, int64(0)
				spans = spans[:0]
				for end < count {
					t := pending[end]
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
							err = ErrConnectionClosed
						}
						t.finish(InternalWriteResult{InternalErr: err})
						pending[end-1] = nil
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
							written, err = w.conn.InternalW.(spanContextWriter).InternalWriteSpans(w.conn.InternalCtx, packet)
						}
					}
				}
				remaining := written
				for i := start; i < end; i++ {
					t := pending[i]
					pending[i] = nil
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
					w.conn.InternalCloseWithError(err)
				}
				start = end
			}
		}
	}
}
