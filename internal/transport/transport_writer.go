// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"io"
	"net"

	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// spanContextWriter consumes immutable span descriptors while serializing the
// entire request. Completion, including cancellation, is a lifetime boundary.
type spanContextWriter interface {
	InternalWriteSpans(context.Context, wire.Chain) (int64, error)
}

// spanSliceWriter is implemented by writers that accept a span slice directly,
// sparing the submission writer a Chain allocation per batch.
type spanSliceWriter interface {
	InternalWriteSpanSlice(ctx context.Context, spans []wire.Span, total int64) (int64, error)
}

// TCP and Unix sockets use writev. Wrappers (including crypto/tls) receive
// ordered spans without a driver gather buffer. TLS record assembly is owned by
// the standard library. Checking every fallback Write prevents a short nil write
// from accidentally skipping the unwritten suffix between spans.
func writeSpanBuffers(w io.Writer, buffers *net.Buffers) (int64, error) {
	switch w.(type) {
	case *net.TCPConn, *net.UnixConn:
		// WriteTo consumes *buffers; a caller-owned header keeps it off the heap.
		return buffers.WriteTo(w)
	}
	spans := *buffers
	var total int64
	for _, span := range spans {
		n, err := w.Write(span)
		if n < 0 || n > len(span) {
			return total, io.ErrShortWrite
		}
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n != len(span) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// maxRetainedWriteSpans bounds idle writer descriptor storage per connection.
const maxRetainedWriteSpans = 512

func (w *InternalDeadlineContextWriter) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	return w.writeSpans(ctx, chain, nil, chain.Len())
}

// InternalWriteSpanSlice writes ordered spans whose lengths sum to total,
// without first building a Chain over them.
func (w *InternalDeadlineContextWriter) InternalWriteSpanSlice(ctx context.Context, slice []wire.Span, total int64) (int64, error) {
	return w.writeSpans(ctx, wire.Chain{}, slice, total)
}

// writeSpans writes chain, or slice when chain is empty.
func (w *InternalDeadlineContextWriter) writeSpans(ctx context.Context, chain wire.Chain, slice []wire.Span, total int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-w.InternalQuit:
		return 0, runtimeerrors.ErrConnectionClosed
	case w.InternalSemaphore <- struct{}{}:
	}
	defer func() { <-w.InternalSemaphore }()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.InternalW.SetWriteDeadline(effectiveWriteDeadline(w.InternalTimeout.Load())); err != nil {
		return 0, err
	}

	spans := w.spanScratch[:0]
	defer func() {
		clear(spans)
		// Bound idle descriptor storage independently of request fragmentation.
		// The write semaphore remains held until all payload references are gone.
		// A submission batch carries up to 16 requests, so retain enough
		// descriptors for one batch: 512 spans, 12 KiB per connection.
		if cap(spans) <= maxRetainedWriteSpans {
			w.spanScratch = spans[:0]
		} else {
			w.spanScratch = nil
		}
	}()
	if slice != nil {
		for _, s := range slice {
			spans = append(spans, s.Bytes())
		}
	} else {
		_ = chain.Each(func(s wire.Span) error { spans = append(spans, s.Bytes()); return nil })
	}
	w.writeBuffers = spans
	n, err := writeSpanBuffers(w.InternalW, &w.writeBuffers)
	w.writeBuffers = nil
	if err == nil && n != total {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *InternalWriteCoalescer) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resultChan := writeResultChanPool.Get()
	req := InternalWriteRequest{ctx: ctx, chain: chain, resultChan: resultChan}
	select {
	case <-ctx.Done():
		writeResultChanPool.Put(resultChan)
		return 0, ctx.Err()
	case <-w.InternalQuit:
		writeResultChanPool.Put(resultChan)
		return 0, runtimeerrors.ErrConnectionClosed
	case w.InternalWriteCh <- req:
	}
	if w.InternalTestEnqueuedHook != nil {
		w.InternalTestEnqueuedHook()
	}
	// Cancellation after admission must join the writer before releasing input.
	result := <-resultChan
	writeResultChanPool.Put(resultChan)
	return result.written, result.InternalErr
}
