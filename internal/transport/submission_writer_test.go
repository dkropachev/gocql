// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestQueuedTicketConnectionLossRemainsRetryable(t *testing.T) {
	ctx, closeConnection := context.WithCancel(context.Background())
	defer closeConnection()
	conn := &Conn{InternalCtx: ctx}
	// Accept the ticket, but hold it before the writer claims transmission.
	conn.typedWriterOnce.Do(func() { conn.typedWriter = &submissionWriter{conn: conn, ready: make(chan *writeTicket)} })
	accepted := make(chan *writeTicket, 1)
	go func() { accepted <- <-conn.typedWriter.ready }()
	chain, _ := wire.NewChain(wire.Borrow([]byte("queued request")))
	drained := make(chan struct{})
	lease := wire.NewLease(chain, func() { close(drained) })
	submitted := make(chan *writeTicket, 1)
	go func() {
		t, err := conn.submitTyped(context.Background(), chain, lease)
		if err != nil {
			panic(err)
		}
		submitted <- t
	}()
	ticket := <-accepted
	callerTicket := <-submitted
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
	ticket.resultKnown = false
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
