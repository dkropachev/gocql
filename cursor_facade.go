// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// PageLease pins response storage independently of cursor advancement and
// session shutdown. Close it once all borrowed views it protects are unused.
// Do not copy a lease or access protected views concurrently with its final Close.
type PageLease implExecution.PageLease

// Close releases the retained page. It is idempotent and accepts a nil receiver.
func (l *PageLease) Close() { (*implExecution.PageLease)(l).Close() }

// Rows iterates one execution across automatically fetched pages. Close it on
// early termination and check Err after iteration. Borrowed fields expire on
// advancement or Close unless Retain pins the page. A stale handle cannot affect
// a later execution and reports cqlerr.ErrStaleRows. The zero Rows is empty.
// Rows must not be used concurrently.
type Rows[R any] implExecution.Rows[R]

// Retain keeps the current row's physical page alive until the lease closes.
func (r Rows[R]) Retain() (*PageLease, error) {
	lease, err := (implExecution.Rows[R])(r).Retain()
	return (*PageLease)(lease), err
}

// NextInto decodes the next row into dst, reporting whether one exists.
func (r Rows[R]) NextInto(dst *R) bool { return (implExecution.Rows[R])(r).NextInto(dst) }

// Err returns the terminal iteration error, if any.
func (r Rows[R]) Err() error { return (implExecution.Rows[R])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent.
func (r Rows[R]) Close() error { return (implExecution.Rows[R])(r).Close() }

// Columns returns the current page's actual immutable result metadata.
func (r Rows[R]) Columns() cql.Columns { return (implExecution.Rows[R])(r).Columns() }

// Clone returns an owned copy of the current row, recursively detaching views.
func (r Rows[R]) Clone() (R, error) { return (implExecution.Rows[R])(r).Clone() }

// Info returns execution information accumulated so far.
func (r Rows[R]) Info() Result { return (implExecution.Rows[R])(r).Info() }

// Page iterates exactly one response page. Borrowed fields expire on
// advancement or Close unless Retain pins the page. Save State before Close to
// resume explicit paging. A Page must not be copied or used concurrently.
type Page[R any] implExecution.Page[R]

// Retain keeps the current row's physical page alive until the lease closes.
func (r *Page[R]) Retain() (*PageLease, error) {
	lease, err := (*implExecution.Page[R])(r).Retain()
	return (*PageLease)(lease), err
}

// NextInto decodes the next row into dst, reporting whether one exists.
func (r *Page[R]) NextInto(dst *R) bool { return (*implExecution.Page[R])(r).NextInto(dst) }

// Err returns the terminal iteration error, if any.
func (r *Page[R]) Err() error { return (*implExecution.Page[R])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent.
func (r *Page[R]) Close() error { return (*implExecution.Page[R])(r).Close() }

// Columns returns the current page's actual immutable result metadata.
func (r *Page[R]) Columns() cql.Columns { return (*implExecution.Page[R])(r).Columns() }

// Clone returns an owned copy of the current row, recursively detaching views.
func (r *Page[R]) Clone() (R, error) { return (*implExecution.Page[R])(r).Clone() }

// Info returns execution information accumulated so far.
func (r *Page[R]) Info() Result { return (*implExecution.Page[R])(r).Info() }

// State returns the continuation state; save it before closing the page.
func (r *Page[R]) State() PageState { return (*implExecution.Page[R])(r).State() }

// RowStream decodes into seeded caller-owned storage. Only a true NextInto
// publishes a complete row; errors can leave partial destination contents. Buffer
// capacity must be provisioned before decoding. Output survives Close and is
// overwritten when its destination is reused. Concurrent NextInto is rejected.
type RowStream[R any] implExecution.RowStream[R]

// NextInto decodes the next row into dst, reporting whether one exists.
func (r *RowStream[R]) NextInto(dst *R) bool { return (*implExecution.RowStream[R])(r).NextInto(dst) }

// Err returns the terminal iteration error, if any.
func (r *RowStream[R]) Err() error { return (*implExecution.RowStream[R])(r).Err() }

// Close stops iteration and releases this execution. It is idempotent.
// It cancels and joins any in-progress destination access before returning.
func (r *RowStream[R]) Close() error { return (*implExecution.RowStream[R])(r).Close() }

// Columns returns the current page's actual immutable result metadata.
func (r *RowStream[R]) Columns() cql.Columns { return (*implExecution.RowStream[R])(r).Columns() }

// Info reports delivered rows, pages and execution progress.
func (r *RowStream[R]) Info() StreamInfo { return (*implExecution.RowStream[R])(r).Info() }
