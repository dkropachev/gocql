// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// Worker owns reusable parameters and execution state for one goroutine. It
// permits one active execution and must not be copied. Options are fixed at
// creation. Close the worker when done, including when execution fails.
// Prepared.Exec, One, Each and All manage their own state and are convenient
// for concurrent request handlers.
//
// Prepared.Worker makes a Worker. A zero Worker's executions return a
// cql.ValidationError, and its Close does nothing.
type Worker[P, R any] implSession.Query[P, R]

func (w *Worker[P, R]) internal() *implSession.Query[P, R] { return (*implSession.Query[P, R])(w) }

// Exec executes a statement with no result columns, capturing value once for
// routing, retries and paging. A statement whose prepared metadata has
// result columns, or that is conditional, is rejected before it is sent, as
// for Prepared.Exec. Overlapping calls return cqlerr.ErrBusy.
func (w *Worker[P, R]) Exec(ctx context.Context, value P) (cql.Result, error) {
	return w.internal().Exec(ctx, value)
}

// Apply executes a conditional statement and reports whether its condition
// applied, as Prepared.Apply does. Overlapping calls return cqlerr.ErrBusy.
func (w *Worker[P, R]) Apply(ctx context.Context, value P) (bool, cql.Result, error) {
	return w.internal().Apply(ctx, value)
}

// One decodes the first row into dst, ignoring further rows. It reports whether
// a row exists and clears dst when none exists. Borrowed fields are detached
// before return; built-in owning fields keep their destination buffers. With
// a nil dst, One only reports whether a row exists; on a conditional
// statement it is rejected before sending, as for Prepared.One: use Apply.
func (w *Worker[P, R]) One(ctx context.Context, value P, dst *R) (bool, cql.Result, error) {
	return w.internal().One(ctx, value, dst)
}

// Each visits every row across all pages, decoding into dst and calling fn.
// Borrowed fields expire when fn returns. Return StopIteration to stop
// successfully; any other callback error stops iteration and is returned. There is no execution cursor to close.
// dst is decoded into for every row and passed to fn, the same pointer each
// time: copy a row to keep it. A nil dst decodes each row into a new R, which
// fn may keep. With cql.ReuseBuffers, or a RowInto reset that keeps slices,
// []byte fields reuse their capacity, so a slice kept from one call of fn
// aliases the next row's data: keep bytes.Clone(row.Field).
func (w *Worker[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error) (cql.Result, error) {
	return w.internal().Each(ctx, value, dst, fn)
}

// All iterates every row across all pages, decoding each into dst:
//
//	for row, err := range w.All(ctx, value, &dst) {
//		if err != nil {
//			return err
//		}
//		use(row)
//	}
//
// Rows stream page by page; they are not loaded into memory. The yielded row
// is dst, the same pointer every iteration, so append(rows, *row) keeps a
// copy while append(ptrs, row) keeps one row many times. With a nil dst, each
// row is decoded into a new R that the loop may keep. Borrowed fields are
// valid only until the next iteration either way. An error is yielded once as
// (nil, err) and ends the loop. Breaking out of the loop releases the
// execution. There is no cursor to close and no Result; use Each or Run when
// the Result matters. dst is decoded into as for Each.
func (w *Worker[P, R]) All(ctx context.Context, value P, dst *R) iter.Seq2[*R, error] {
	return w.internal().All(ctx, value, dst)
}

// Run returns a cursor that fetches continuation pages as needed. Close the
// cursor before reusing the worker. Borrowed fields expire on advancement or
// Close unless a retained PageLease pins their page.
func (w *Worker[P, R]) Run(ctx context.Context, value P) (Rows[R], error) {
	rows, err := w.internal().Run(ctx, value)
	return Rows[R](rows), err
}

// RunPage executes one page, resuming from state. cql.StartPage() begins the first
// page. Page.State remains available after Close; close the page before reusing w.
// pageSize is this page's size: zero uses the worker's resolved page size, so
// a page size that changes per request needs no new options. With
// cql.Unpaged the page is the whole result: the response is bounded by the
// server and by the configured frame and decompression limits. A negative
// pageSize, or one above cql.Unpaged, is rejected with a cql.ValidationError.
func (w *Worker[P, R]) RunPage(ctx context.Context, value P, state cql.PageState, pageSize int) (*Page[R], error) {
	size, err := pageSize32(pageSize)
	if err != nil {
		return nil, err
	}
	page, err := w.internal().RunPage(ctx, value, cql.PageState(state), size)
	return (*Page[R])(page), err
}

// AddToBatch encodes value straight into storage batch owns. It uses none of
// the worker's execution state, so it works while the worker runs, drains or has
// an open handle; a closed worker or session returns cqlerr.ErrClosed. A batch
// that holds members of another session, or already holds
// MaxBatchStatements, is rejected with a *cql.ValidationError and left
// unchanged. Neither the worker nor batch may be used concurrently with this
// call.
func (w *Worker[P, R]) AddToBatch(batch *Batch, value P) error {
	return w.internal().AddToBatch((*implSession.Batch)(batch), value)
}

// Close stops the worker permanently and returns at once. It may be called
// from any goroutine: a call running at that moment is cancelled and returns
// an error matching context.Canceled, with cqlerr.ExecutionStatusOf reporting
// whether the request may have reached the server; open Rows or a Page report
// cqlerr.ErrClosed, and later calls return cqlerr.ErrClosed. Accepted writes and observer callbacks drain
// in the background; Wait joins them and Session.Close joins any that remain.
// A worker dropped without Close is closed when the garbage collector finds it
// unreachable, but call Close: collection timing is unspecified. Close
// returns nil: a failure of the drain is reported to the call that started
// it, and Wait reports a drain still under way. The error result makes the
// Worker an io.Closer, like every other handle the driver returns.
func (w *Worker[P, R]) Close() error {
	w.internal().Close()
	return nil
}

// Wait waits until the worker's last execution has drained: accepted writes,
// attempts in flight and observer callbacks. It returns ctx.Err() if ctx ends
// first. Wait after Close when code that follows must not race those
// callbacks, for example before tearing down state an observe.Hooks uses.
func (w *Worker[P, R]) Wait(ctx context.Context) error { return w.internal().Wait(ctx) }

// StreamWorker owns reusable state for streaming into seeded caller storage.
// Close it when done and close each Run cursor before starting another run.
// Page buffering is the default; exec.StreamOptions.Incremental reserves a connection
// for bounded incremental receive. It must not be copied or used concurrently.
//
// PreparedStream.Worker makes a StreamWorker. A zero StreamWorker's Run returns a
// cql.ValidationError, and its Close does nothing.
type StreamWorker[P, R any] implSession.StreamQuery[P, R]

// Run starts a stream. Only successful NextInto calls publish complete rows.
// Output belongs to the caller and survives cursor Close; reusing a destination
// overwrites its contents. Close the returned cursor on early termination.
func (w *StreamWorker[P, R]) Run(ctx context.Context, value P) (*RowStream[R], error) {
	stream, err := (*implSession.StreamQuery[P, R])(w).Run(ctx, value)
	return (*RowStream[R])(stream), err
}

// Close stops the worker permanently and returns at once, as Worker.Close
// does, and returns nil. An open RowStream reports cqlerr.ErrClosed.
func (w *StreamWorker[P, R]) Close() error {
	(*implSession.StreamQuery[P, R])(w).Close()
	return nil
}

// Wait waits until the worker's last execution has drained, as Worker.Wait
// does.
func (w *StreamWorker[P, R]) Wait(ctx context.Context) error {
	return (*implSession.StreamQuery[P, R])(w).Wait(ctx)
}
