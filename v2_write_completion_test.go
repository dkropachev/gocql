// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// heldSpanWriter writes spans through to the connection, then holds the
// writer until released, as a slow syscall return or preemption would.
type heldSpanWriter struct {
	contextWriter
	release  chan struct{}
	returned atomic.Bool
}

func (w *heldSpanWriter) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	defer w.hold()
	return w.contextWriter.(interface {
		InternalWriteSpans(context.Context, wire.Chain) (int64, error)
	}).InternalWriteSpans(ctx, chain)
}

func (w *heldSpanWriter) InternalWriteSpanSlice(ctx context.Context, spans []wire.Span, total int64) (int64, error) {
	defer w.hold()
	return w.contextWriter.(interface {
		InternalWriteSpanSlice(context.Context, []wire.Span, int64) (int64, error)
	}).InternalWriteSpanSlice(ctx, spans, total)
}

func (w *heldSpanWriter) hold() {
	<-w.release
	w.returned.Store(true)
}

// A response can arrive before the writer has finished with its request. The
// attempt must still return only after the writer released the request input,
// or a reusable query would report ErrBusy on its next execution.
func TestTypedAttemptReturnsAfterWriterReleasesInput(t *testing.T) {
	c, peer := typedTestConn(t, 4, nil)
	writer := &heldSpanWriter{contextWriter: c.InternalW, release: make(chan struct{})}
	c.InternalW = writer
	go c.InternalServe(c.InternalCtx)
	chain, _ := wire.NewChain(wire.Borrow([]byte("value")))
	var inputReleased atomic.Int32
	input := wire.NewLease(chain, func() { inputReleased.Add(1) })
	go func() {
		stream := typedTestReadRequest(t, c, peer)
		if _, err := peer.Write(typedTestResponse(t, 4, stream, []byte{0, 0, 0, 1})); err != nil {
			t.Error(err)
		}
	}()
	returned := make(chan error, 1)
	go func() {
		page, err := c.InternalExecuteTyped(context.Background(), &attemptRequest{Statement: "q", Consistency: One, Lease: input})
		if page != nil {
			page.Owner.Release()
		}
		returned <- err
	}()
	select {
	case err := <-returned:
		t.Fatal("attempt returned while its writer still held the request", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(writer.release)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("attempt did not return after the writer finished")
	}
	if !writer.returned.Load() {
		t.Fatal("attempt returned before the writer finished")
	}
	input.Release()
	if inputReleased.Load() != 1 {
		t.Fatal("request input still retained after the attempt returned")
	}
}
