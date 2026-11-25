// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// PageLease pins response storage independently of cursor advancement and
// session shutdown. Close it once all borrowed views it protects are unused.
// Do not copy a lease or access protected views concurrently with its final Close.
//
// Rows.Retain, Page.Retain and CASBatchRows.Retain make leases; Close on a zero
// PageLease does nothing.
type PageLease implSession.PageLease

// Close releases the retained page. It is idempotent and accepts a nil receiver.
func (l *PageLease) Close() { (*implSession.PageLease)(l).Close() }

// Rows iterates one execution across automatically fetched pages. Close it on
// early termination and check Err after iteration. Borrowed fields expire on
// advancement or Close unless Retain pins the page. A stale handle cannot affect
// a later execution and reports cqlerr.ErrStaleRows. The zero Rows is empty.
// Rows must not be used concurrently.
type Rows[R any] implSession.Rows[R]

// Retain keeps the current row's physical page alive until the lease closes.
// With no current row it returns cqlerr.ErrNoCurrentRow.
func (r Rows[R]) Retain() (*PageLease, error) {
	lease, err := (implSession.Rows[R])(r).Retain()
	return (*PageLease)(lease), err
}

// NextInto decodes the next row into dst, reporting whether one exists.
func (r Rows[R]) NextInto(dst *R) bool { return (implSession.Rows[R])(r).NextInto(dst) }

// All returns an iterator over the remaining rows, fetching pages as
// NextInto does. Each row is decoded into dst, and the yielded pointer is dst
// itself, reused for the next row: copy the row to keep it. With a nil dst,
// each row is decoded into a new R, which the caller may keep. Borrowed
// fields stay valid until the next iteration. An error is yielded once, as
// (nil, err), and ends the iteration. The rows are closed when the loop
// ends, early or not.
func (r Rows[R]) All(dst *R) iter.Seq2[*R, error] { return (implSession.Rows[R])(r).All(dst) }

// Err returns the terminal iteration error, if any.
func (r Rows[R]) Err() error { return (implSession.Rows[R])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent and
// returns the same error each time: the error that ended iteration, if any,
// or cqlerr.ErrClosed when Worker.Close or Session.Close stopped the execution
// first.
func (r Rows[R]) Close() error { return (implSession.Rows[R])(r).Close() }

// Columns returns the current page's actual immutable result metadata.
func (r Rows[R]) Columns() cql.Columns { return (implSession.Rows[R])(r).Columns() }

// Clone returns an owned copy of the current row, recursively detaching views.
// With no current row it returns cqlerr.ErrNoCurrentRow.
func (r Rows[R]) Clone() (R, error) { return (implSession.Rows[R])(r).Clone() }

// Info returns execution information accumulated so far.
func (r Rows[R]) Info() cql.Result { return (implSession.Rows[R])(r).Info() }

// CASBatchRows are the rows of a conditional batch sent with
// Session.RunConditionalBatch. Each row reports whether the batch applied, and
// Applied reports it over all of them, which is the same rule on Cassandra and
// ScyllaDB. Which rows the server returns differs: on ScyllaDB row i belongs
// to statement i, and its Previous comes from the row the server found for
// that statement, while on Cassandra rows belong to rows of the partition;
// see the server differences documentation. Ownership and staleness are as
// for Rows. CASBatchRows must not be used concurrently.
//
// The zero CASBatchRows has no rows, and its Applied returns a
// cql.ValidationError.
type CASBatchRows[X any] implSession.CASBatchRows[X]

// All returns an iterator over the remaining rows, as Rows.All does. Applied
// still reports the outcome once the loop ran to the end.
func (r CASBatchRows[X]) All(dst *cql.CASResult[X]) iter.Seq2[*cql.CASResult[X], error] {
	return (implSession.CASBatchRows[X])(r).All(dst)
}

// NextInto decodes the next row into dst, reporting whether one exists.
func (r CASBatchRows[X]) NextInto(dst *cql.CASResult[X]) bool {
	return (implSession.CASBatchRows[X])(r).NextInto(dst)
}

// Applied reports whether the batch applied: whether every row reports it.
// It reads the rows NextInto has not, so it may be called with or without
// iterating first. It returns an error instead when iteration failed, when
// the rows were closed before NextInto or Applied reported their end (even
// right after the last row), when the result had no rows, when the handle is
// stale, and for the zero CASBatchRows.
func (r CASBatchRows[X]) Applied() (bool, error) {
	return (implSession.CASBatchRows[X])(r).Applied()
}

// Err returns the terminal iteration error, if any.
func (r CASBatchRows[X]) Err() error { return (implSession.CASBatchRows[X])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent and
// returns the same error each time: the error that ended iteration, if any,
// or cqlerr.ErrClosed when Session.Close stopped the execution first.
func (r CASBatchRows[X]) Close() error { return (implSession.CASBatchRows[X])(r).Close() }

// Columns returns the actual immutable result metadata.
func (r CASBatchRows[X]) Columns() cql.Columns { return (implSession.CASBatchRows[X])(r).Columns() }

// Retain keeps the current row's physical page alive until the lease closes.
// With no current row it returns cqlerr.ErrNoCurrentRow.
func (r CASBatchRows[X]) Retain() (*PageLease, error) {
	lease, err := (implSession.CASBatchRows[X])(r).Retain()
	return (*PageLease)(lease), err
}

// Clone returns an owned copy of the current row, recursively detaching views.
// With no current row it returns cqlerr.ErrNoCurrentRow.
func (r CASBatchRows[X]) Clone() (cql.CASResult[X], error) {
	return (implSession.CASBatchRows[X])(r).Clone()
}

// Info returns execution information accumulated so far.
func (r CASBatchRows[X]) Info() cql.Result { return (implSession.CASBatchRows[X])(r).Info() }

// Page iterates exactly one response page. Borrowed fields expire on
// advancement or Close unless Retain pins the page. State remains available
// after Close to resume explicit paging. A Page must not be copied or used concurrently.
//
// The zero Page has no rows, and its State is complete.
type Page[R any] implSession.Page[R]

// Retain keeps the current row's physical page alive until the lease closes.
// With no current row it returns cqlerr.ErrNoCurrentRow.
func (r *Page[R]) Retain() (*PageLease, error) {
	lease, err := (*implSession.Page[R])(r).Retain()
	return (*PageLease)(lease), err
}

// NextInto decodes the next row into dst, reporting whether one exists.
func (r *Page[R]) NextInto(dst *R) bool { return (*implSession.Page[R])(r).NextInto(dst) }

// All returns an iterator over the page's remaining rows, as Rows.All does:
// dst is reused for every row, and a nil dst decodes each into a new R. The
// page is closed when the loop ends; State remains available to resume
// from.
func (r *Page[R]) All(dst *R) iter.Seq2[*R, error] { return (*implSession.Page[R])(r).All(dst) }

// Err returns the terminal iteration error, if any.
func (r *Page[R]) Err() error { return (*implSession.Page[R])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent and
// returns the same error each time: the error that ended iteration, if any,
// or cqlerr.ErrClosed when Worker.Close or Session.Close stopped the execution
// first.
func (r *Page[R]) Close() error { return (*implSession.Page[R])(r).Close() }

// Columns returns the current page's actual immutable result metadata.
func (r *Page[R]) Columns() cql.Columns { return (*implSession.Page[R])(r).Columns() }

// Clone returns an owned copy of the current row, recursively detaching views.
// With no current row it returns cqlerr.ErrNoCurrentRow.
func (r *Page[R]) Clone() (R, error) { return (*implSession.Page[R])(r).Clone() }

// Info returns execution information accumulated so far.
func (r *Page[R]) Info() cql.Result { return (*implSession.Page[R])(r).Info() }

// State returns the paging state, which remains available after Close.
func (r *Page[R]) State() cql.PageState { return cql.PageState((*implSession.Page[R])(r).State()) }

// RowStream decodes into seeded caller-owned storage. Only a true NextInto
// publishes a complete row; errors can leave partial destination contents. Buffer
// capacity must be provisioned before decoding. Output survives Close and is
// overwritten when its destination is reused. Concurrent NextInto is rejected.
//
// The zero RowStream has no rows.
type RowStream[R any] implSession.RowStream[R]

// NextInto decodes the next row into dst, reporting whether one exists.
func (r *RowStream[R]) NextInto(dst *R) bool { return (*implSession.RowStream[R])(r).NextInto(dst) }

// Err returns the terminal iteration error, if any.
func (r *RowStream[R]) Err() error { return (*implSession.RowStream[R])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent and
// returns the same error each time: the error that ended iteration, if any,
// or cqlerr.ErrClosed when StreamWorker.Close or Session.Close stopped the
// execution first. It cancels and joins any in-progress destination access
// before returning.
func (r *RowStream[R]) Close() error { return (*implSession.RowStream[R])(r).Close() }

// Columns returns the current page's actual immutable result metadata.
func (r *RowStream[R]) Columns() cql.Columns { return (*implSession.RowStream[R])(r).Columns() }

// Info reports delivered rows, pages and execution progress.
func (r *RowStream[R]) Info() StreamInfo { return StreamInfo((*implSession.RowStream[R])(r).Info()) }
