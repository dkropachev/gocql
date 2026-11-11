// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/model"

	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/routing"
)

// Query owns reusable parameters and execution state for one worker. It permits
// one active execution and must not be copied. Options are fixed at creation.
// Close the query when done, including when execution fails: the session keeps
// it registered until Close. Prepared.Exec, One and Each manage their own state
// and are convenient for concurrent request handlers.
type Query[P, R any] implSession.Query[P, R]

func (q *Query[P, R]) internal() *implSession.Query[P, R] { return (*implSession.Query[P, R])(q) }

// Exec executes a statement with no result columns, capturing value once for
// routing, retries and paging. Overlapping calls return cqlerr.ErrBusy.
func (q *Query[P, R]) Exec(ctx context.Context, value P) (Result, error) {
	return q.internal().Exec(ctx, value)
}

// One decodes the first row into dst, ignoring further rows. It reports whether
// a row exists and clears dst when none exists. Borrowed fields are detached
// before return; built-in owning fields keep their destination buffers.
func (q *Query[P, R]) One(ctx context.Context, value P, dst *R) (bool, Result, error) {
	return q.internal().One(ctx, value, dst)
}

// Each visits every row across all pages, decoding into dst and calling fn.
// Borrowed fields expire when fn returns. Return StopIteration to stop
// successfully; any other callback error stops iteration and is returned. There is no execution cursor to close.
func (q *Query[P, R]) Each(ctx context.Context, value P, dst *R, fn func(*R) error) (Result, error) {
	return q.internal().Each(ctx, value, dst, fn)
}

// Run returns a cursor that fetches continuation pages as needed. Close the
// cursor before reusing the query. Borrowed fields expire on advancement or
// Close unless a retained PageLease pins their page.
func (q *Query[P, R]) Run(ctx context.Context, value P) (Rows[R], error) {
	rows, err := q.internal().Run(ctx, value)
	return Rows[R](rows), err
}

// RunPage executes one page, resuming from state. StartPage begins the first
// page. Page.State remains available after Close; close the page before reusing q.
func (q *Query[P, R]) RunPage(ctx context.Context, value P, state PageState) (*Page[R], error) {
	page, err := q.internal().RunPage(ctx, value, model.PageState(state))
	return (*Page[R])(page), err
}

// RunUnpaged requests one response without paging. The response is bounded by
// the configured frame and decompression limits. Close its page when done.
func (q *Query[P, R]) RunUnpaged(ctx context.Context, value P) (*Page[R], error) {
	page, err := q.internal().RunUnpaged(ctx, value)
	return (*Page[R])(page), err
}

// Routing reports routing information derived from value. It requires an idle
// query and does not execute the statement.
func (q *Query[P, R]) Routing(value P) (routing.Info, error) { return q.internal().Routing(value) }

// AddToBatch encodes and copies value into batch. It requires an idle query;
// neither the query nor batch may be used concurrently with this call.
func (q *Query[P, R]) AddToBatch(batch *Batch, value P) error {
	return q.internal().AddToBatch((*implSession.Batch)(batch), value)
}

// Close permanently stops the query and any active cursor, then waits for
// accepted writes and callbacks to drain. If ctx expires, cleanup continues
// under the session and Session.Close joins it. Use a separate bounded cleanup
// context when a cancelled request context cannot wait for the drain.
func (q *Query[P, R]) Close(ctx context.Context) error { return q.internal().Close(ctx) }

// StreamQuery owns reusable state for streaming into seeded caller storage.
// Close it when done and close each Run cursor before starting another run.
// Page buffering is the default; StreamOptions.Incremental reserves a connection
// for bounded incremental receive. It must not be copied or used concurrently.
type StreamQuery[P, R any] implSession.StreamQuery[P, R]

// Run starts a stream. Only successful NextInto calls publish complete rows.
// Output belongs to the caller and survives cursor Close; reusing a destination
// overwrites its contents. Close the returned cursor on early termination.
func (q *StreamQuery[P, R]) Run(ctx context.Context, value P) (*RowStream[R], error) {
	stream, err := (*implSession.StreamQuery[P, R])(q).Run(ctx, value)
	return (*RowStream[R])(stream), err
}

// Close stops admission, cancels any active cursor and waits for drain, as
// Query.Close does.
func (q *StreamQuery[P, R]) Close(ctx context.Context) error {
	return (*implSession.StreamQuery[P, R])(q).Close(ctx)
}
