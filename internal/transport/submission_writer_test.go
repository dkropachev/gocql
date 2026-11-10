// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// failingSocket fails every write, after running onWrite, and accepts Close.
type failingSocket struct {
	onWrite func()
	closed  atomic.Bool
}

func (s *failingSocket) InternalWriteSpanSlice(context.Context, []wire.Span, int64) (int64, error) {
	if s.onWrite != nil {
		s.onWrite()
	}
	return 0, errors.New("write timeout")
}
func (s *failingSocket) InternalWriteContext(context.Context, []byte) (int, error) {
	return 0, errors.New("write timeout")
}
func (s *failingSocket) InternalSetWriteTimeout(time.Duration) {}
func (s *failingSocket) SetWriteDeadline(time.Time) error      { return nil }
func (s *failingSocket) Read([]byte) (int, error)              { return 0, io.EOF }
func (s *failingSocket) Close() error                          { s.closed.Store(true); return nil }
func (s *failingSocket) RemoteAddr() net.Addr                  { return nil }
func (s *failingSocket) SetTimeout(time.Duration)              {}
func (s *failingSocket) GetTimeout() time.Duration             { return 0 }
func (s *failingSocket) InternalSetDisarm(bool)                {}

type ignoreConnErrors struct{}

func (ignoreConnErrors) HandleError(*Conn, error, bool) {}

func TestSubmissionWriter(t *testing.T) {
	t.Run("ConnectionLoss", func(t *testing.T) {
		ctx, closeConnection := context.WithCancel(context.Background())
		defer closeConnection()
		conn := &Conn{InternalCtx: ctx}
		// No writer goroutine runs: the ticket stays queued, accepted but not
		// claimed for transmission.
		conn.typedWriterOnce.Do(func() { conn.typedWriter = &submissionWriter{conn: conn, wake: make(chan struct{}, 1)} })
		chain, _ := wire.NewChain(wire.Borrow([]byte("queued request")))
		drained := make(chan struct{})
		lease := wire.NewLease(chain, func() { close(drained) })
		callerTicket, err := conn.submitTyped(context.Background(), chain, lease)
		if err != nil {
			t.Fatal(err)
		}
		conn.typedWriter.mu.Lock()
		ticket := conn.typedWriter.queue[0]
		conn.typedWriter.mu.Unlock()
		closeConnection()
		// The caller withdraws the queued ticket: nothing was transmitted, so the
		// attempt is retryable.
		if !callerTicket.unsent() {
			t.Fatal("queued ticket reported as transmitted")
		}
		if n, known := callerTicket.written(); known && n != 0 {
			t.Fatal("withdrawn ticket reported written bytes", n)
		}
		callerTicket.release()
		select {
		case <-drained:
			t.Fatal("accepted ticket storage drained before writer completion")
		default:
		}
		ticket.finish(InternalWriteResult{InternalErr: ErrConnectionClosed})
		<-drained
	})

	t.Run("Ticket", func(t *testing.T) {
		t.Run("ReportsWriterOutcome", func(t *testing.T) {
			ticket := ticketPool.Get()
			ticket.refs.Store(2)
			ticket.phase.Store(0)
			ticket.state.Store(ticketPending)
			chain, _ := wire.NewChain(wire.Borrow([]byte("request")))
			ticket.packet, ticket.lease = chain, wire.NewLease(chain, nil)
			if _, known := ticket.written(); known {
				t.Fatal("result known before the writer finished")
			}
			if !ticket.phase.CompareAndSwap(0, 1) {
				t.Fatal("writer could not claim the ticket")
			}
			if ticket.unsent() {
				t.Fatal("claimed ticket withdrawn")
			}
			ticket.finish(InternalWriteResult{InternalWritten: 3, InternalErr: errors.New("reset")})
			if n, known := ticket.written(); !known || n != 3 {
				t.Fatal("writer outcome not reported", n, known)
			}
			ticket.release()
		})
		t.Run("AwaitWrittenBlocksUntilFinish", func(t *testing.T) {
			ticket := ticketPool.Get()
			ticket.refs.Store(2)
			ticket.phase.Store(1)
			ticket.state.Store(ticketPending)
			chain, _ := wire.NewChain(wire.Borrow([]byte("request")))
			ticket.packet, ticket.lease = chain, wire.NewLease(chain, nil)
			returned := make(chan struct{})
			go func() {
				ticket.awaitWritten()
				close(returned)
			}()
			for ticket.state.Load() != ticketAwaited {
				runtime.Gosched()
			}
			select {
			case <-returned:
				t.Fatal("awaitWritten returned before the writer finished")
			default:
			}
			ticket.finish(InternalWriteResult{InternalWritten: 7})
			<-returned
			if n, known := ticket.written(); !known || n != 7 {
				t.Fatal("writer outcome not reported", n, known)
			}
			ticket.release()
		})
	})
	// Checks that tickets still
	// queued when the connection closes are finished unsent, and that a submit
	// after shutdown is rejected without being queued.
	t.Run("Shutdown", func(t *testing.T) {
		ctx, closeConnection := context.WithCancel(context.Background())
		conn := &Conn{InternalCtx: ctx}
		conn.typedWriterOnce.Do(func() { conn.typedWriter = &submissionWriter{conn: conn, wake: make(chan struct{}, 1)} })
		w := conn.typedWriter
		var tickets []*writeTicket
		var released atomic.Int32
		for range 3 {
			chain, _ := wire.NewChain(wire.Borrow([]byte("queued")))
			ticket, err := conn.submitTyped(context.Background(), chain, wire.NewLease(chain, func() { released.Add(1) }))
			if err != nil {
				t.Fatal(err)
			}
			tickets = append(tickets, ticket)
		}
		closeConnection()
		w.shutdown() // as run does once it observes the closed connection
		if _, ok := w.take(); ok {
			t.Fatal("take after close reported work")
		}
		if released.Load() != 3 {
			t.Fatal("queued leases not released", released.Load())
		}
		for _, ticket := range tickets {
			if !ticket.unsent() {
				t.Fatal("queued ticket reported as transmitted")
			}
			if _, known := ticket.written(); !known {
				t.Fatal("queued ticket left unfinished")
			}
			ticket.release()
		}
		live := &Conn{InternalCtx: context.Background(), typedWriter: w}
		live.typedWriterOnce.Do(func() {})
		chain, _ := wire.NewChain(wire.Borrow([]byte("late")))
		if _, err := live.submitTyped(context.Background(), chain, wire.NewLease(chain, nil)); !errors.Is(err, ErrConnectionClosed) {
			t.Fatal("submit after shutdown accepted", err)
		}
	})
	// Checks that a failed write
	// finishes every ticket queued behind it, unsent, before the connection
	// closes: close joins the calls, and a call waits for its ticket.
	t.Run("WriteFailure", func(t *testing.T) {
		socket := &failingSocket{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn := &Conn{InternalCtx: ctx, InternalCancel: cancel, InternalW: socket, InternalR: socket, InternalStreams: streams.New(), InternalErrorHandler: ignoreConnErrors{}}
		// The call of a request queued while the failing write is in progress:
		// close joins it, and it ends only once its ticket is finished.
		// InternalGetCallReq already counts the call's one pending exec.
		call := InternalGetCallReq(1)
		conn.InternalCalls = InternalNewCallTable()
		conn.InternalCalls.Set(1, call)
		var late *writeTicket
		lateDone := make(chan struct{})
		socket.onWrite = func() {
			chain, _ := wire.NewChain(wire.Borrow([]byte("late")))
			var err error
			if late, err = conn.submitTyped(context.Background(), chain, wire.NewLease(chain, nil)); err != nil {
				t.Error(err)
			}
			go func() {
				late.awaitWritten()
				call.InternalDone.Done()
				close(lateDone)
			}()
		}
		chain, _ := wire.NewChain(wire.Borrow([]byte("first")))
		first, err := conn.submitTyped(context.Background(), chain, wire.NewLease(chain, nil))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			first.awaitWritten()
			<-lateDone
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("a ticket queued behind a failed write was never finished")
		}
		if n, known := first.written(); !known || n != 0 || first.result.InternalErr == nil {
			t.Fatal("failed write not reported", n, known, first.result.InternalErr)
		}
		if !late.unsent() {
			t.Fatal("ticket queued behind a failed write reported as transmitted")
		}
		if !socket.closed.Load() {
			t.Fatal("connection not closed after the failed write")
		}
		if _, err := conn.submitTyped(context.Background(), chain, wire.NewLease(chain, nil)); !errors.Is(err, ErrConnectionClosed) {
			t.Fatal("submit after a failed write accepted", err)
		}
		first.release()
		late.release()
	})
}
