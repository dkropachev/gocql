// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestQueuedTicketConnectionLossRemainsRetryable(t *testing.T) {
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
}

func TestTicketReportsWriterOutcome(t *testing.T) {
	ticket := ticketPool.Get().(*writeTicket)
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
}

func TestTicketAwaitWrittenBlocksUntilFinish(t *testing.T) {
	ticket := ticketPool.Get().(*writeTicket)
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
}

// TestSubmissionWriterShutdownFinishesQueuedTickets checks that tickets still
// queued when the connection closes are finished unsent, and that a submit
// after shutdown is rejected without being queued.
func TestSubmissionWriterShutdownFinishesQueuedTickets(t *testing.T) {
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
}
