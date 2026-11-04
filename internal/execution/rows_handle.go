// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "errors"

// ErrStaleRows is reported by a Rows handle whose query has started a later
// execution. Its storage now belongs to that execution.
var ErrStaleRows = errors.New("gocql: rows belong to an earlier execution of the query")

// Rows iterates the rows of one execution across its pages.
//
// Rows is a small value. A reusable Query keeps the iterator's storage and
// reuses it for its next execution, which can begin only after these rows
// are closed or the execution is stopped. A handle kept from an earlier
// execution is stale: it reports no rows, Err returns ErrStaleRows, and
// Close does nothing. The zero Rows has no rows.
//
// Rows is not safe for concurrent use.
type Rows[R any] struct {
	impl *rowsImpl[R]
	gen  uint64
}

// newRowsHandle returns a handle for impl's current generation.
func newRowsHandle[R any](impl *rowsImpl[R]) Rows[R] {
	return Rows[R]{impl: impl, gen: impl.gen.Load()}
}

// live returns the iterator if the handle belongs to its current execution.
func (r Rows[R]) live() *rowsImpl[R] {
	if r.impl != nil && r.impl.gen.Load() == r.gen {
		return r.impl
	}
	return nil
}

func (r Rows[R]) stale() bool { return r.impl != nil && r.impl.gen.Load() != r.gen }

// Next advances to the next row, fetching continuation pages as needed.
func (r Rows[R]) Next() bool {
	if impl := r.live(); impl != nil {
		return impl.Next()
	}
	return false
}

// NextInto decodes the next row into dst.
func (r Rows[R]) NextInto(dst *R) bool {
	if impl := r.live(); impl != nil {
		return impl.NextInto(dst)
	}
	if r.impl == nil {
		InternalClearDestination(dst)
	}
	return false
}

// Row returns the current row. It panics with ErrNoRows without one.
func (r Rows[R]) Row() R {
	if impl := r.live(); impl != nil {
		return impl.Row()
	}
	panic(ErrNoRows)
}

// Err returns the error that ended iteration, if any.
func (r Rows[R]) Err() error {
	if impl := r.live(); impl != nil {
		return impl.Err()
	}
	if r.stale() {
		return ErrStaleRows
	}
	return nil
}

// Close stops iteration and releases the execution. It is idempotent, and
// does nothing for a stale handle.
func (r Rows[R]) Close() error {
	if impl := r.live(); impl != nil {
		return impl.Close()
	}
	return nil
}

// Retain keeps the current row's page alive past advancement.
func (r Rows[R]) Retain() (*PageLease, error) {
	if impl := r.live(); impl != nil {
		return impl.Retain()
	}
	return nil, ErrNoRows
}

// Clone returns an owned copy of the current row.
func (r Rows[R]) Clone() (R, error) {
	if impl := r.live(); impl != nil {
		return impl.Clone()
	}
	var zero R
	return zero, ErrNoRows
}

// Info returns the execution's result information so far.
func (r Rows[R]) Info() Result {
	if impl := r.live(); impl != nil {
		return impl.Info()
	}
	return Result{}
}

// Columns returns the current page's actual physical result metadata.
func (r Rows[R]) Columns() Columns {
	if impl := r.live(); impl != nil {
		return impl.Columns()
	}
	return Columns{}
}

// rowsHolder is a Rows iterator with its first page, allocated together. A
// reusable query embeds one.
type rowsHolder[R any] struct {
	page Page[R]
	rows rowsImpl[R]
}
