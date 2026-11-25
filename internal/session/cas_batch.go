// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// casTally is what a CASBatchRows has learned from the rows read so far. A
// Batch keeps the tally of its latest RunConditionalBatch, tied to the rows
// storage owner, and resets it when the next one reuses that storage, so
// tracking it allocates nothing once warm and costs RunBatch nothing. An
// earlier handle on other storage keeps its own tally through its pointer.
type casTally[X any] struct {
	owner *batchRows[cql.CASResult[X]]
	// scratch is the destination Applied decodes the rows the caller did not
	// read into.
	scratch cql.CASResult[X]
	// rows reports that a row was read; notApplied, that one had Applied
	// false.
	rows, notApplied bool
	// done reports that the rows were read to their end; closed, that
	// they were closed before it.
	done, closed bool
}

// casTallyFor returns b's tally for rows storage r, reset for a new
// execution. A handle of an earlier execution on r is stale by then, so it
// no longer reads or writes the tally.
func (b *Batch) casTallyFor[X any](r *batchRows[cql.CASResult[X]]) *casTally[X] {
	t, ok := b.cas.(*casTally[X])
	if !ok || t.owner != r {
		t = &casTally[X]{owner: r}
		b.cas = t
		return t
	}
	*t = casTally[X]{owner: r}
	return t
}

// CASBatchRows are the rows of a conditional batch, each reporting whether
// the batch applied, and whether it applied over all of them.
//
// Like Rows, CASBatchRows is a small value whose storage the Batch reuses
// for its next execution once these rows are closed; a handle kept from an
// earlier execution is stale. It is not safe for concurrent use.
type CASBatchRows[X any] struct {
	tally *casTally[X]
	rows  Rows[cql.CASResult[X]]
}

// All iterates the remaining rows as Rows.All does.
func (r CASBatchRows[X]) All(dst *cql.CASResult[X]) iter.Seq2[*cql.CASResult[X], error] {
	return allRows(dst, r.NextInto, r.Close)
}

// NextInto decodes the next row into dst and reports whether there was one.
func (r CASBatchRows[X]) NextInto(dst *cql.CASResult[X]) bool {
	if r.rows.NextInto(dst) {
		r.tally.rows = true
		if !dst.Applied {
			r.tally.notApplied = true
		}
		return true
	}
	if r.tally != nil && !r.rows.stale() && !r.tally.closed {
		r.tally.done = true
	}
	return false
}

// Applied reports whether the batch applied: on Cassandra and ScyllaDB alike,
// it applied if and only if every row reports it. It reads the rows NextInto
// has not, so it can be called without iterating, or after. It returns an
// error instead when iteration failed, when the rows were closed before
// NextInto or Applied reported their end (even right after the last row),
// when the result had no rows, which no conditional batch returns, when the
// handle is stale, and for the zero CASBatchRows.
func (r CASBatchRows[X]) Applied() (bool, error) {
	if r.tally == nil {
		return false, cqltype.TypeValidation("CASBatchRows", "zero rows handle")
	}
	if !r.rows.stale() && !r.tally.done && !r.tally.closed {
		for r.NextInto(&r.tally.scratch) {
		}
		// The scratch row may borrow the response page; drop it.
		var zero cql.CASResult[X]
		r.tally.scratch = zero
	}
	if err := r.rows.Err(); err != nil {
		return false, err
	}
	if !r.tally.done {
		return false, cqltype.TypeValidation("CASBatchRows", "rows closed before Applied read them")
	}
	if !r.tally.rows {
		return false, cqltype.TypeValidation("CASBatchRows", "conditional batch returned no rows")
	}
	return !r.tally.notApplied, nil
}

// Err returns the error that ended iteration, if any.
func (r CASBatchRows[X]) Err() error { return r.rows.Err() }

// Close releases the execution. It is idempotent, and does nothing for a
// stale handle.
func (r CASBatchRows[X]) Close() error {
	if r.tally != nil && r.rows.live() != nil && !r.tally.done {
		r.tally.closed = true
	}
	return r.rows.Close()
}

// Columns returns the result metadata of the rows.
func (r CASBatchRows[X]) Columns() cql.Columns { return r.rows.Columns() }

// Clone returns an owned copy of the row the last NextInto decoded.
func (r CASBatchRows[X]) Clone() (cql.CASResult[X], error) { return r.rows.Clone() }

// Retain keeps the current row's page alive past advancement.
func (r CASBatchRows[X]) Retain() (*PageLease, error) { return r.rows.Retain() }

// Info returns the execution's result information so far.
func (r CASBatchRows[X]) Info() cql.Result { return r.rows.Info() }

// RunConditionalBatch sends batch as RunBatch does and returns its rows as
// CASBatchRows, whose Applied reports whether the batch applied. decoder is
// a CASRows decoder, typically the Rows of a member's statement. A batch
// whose members are all known not to be conditional is rejected before it is
// sent.
func (s *Session) RunConditionalBatch[X any](ctx context.Context, batch *Batch, decoder cql.RowDecoder[cql.CASResult[X]], options CompiledOptions) (CASBatchRows[X], error) {
	if s == nil {
		return CASBatchRows[X]{}, notSent(cqltype.TypeValidation("session", "nil session"))
	}
	if decoder == nil {
		return CASBatchRows[X]{}, notSent(cqltype.TypeValidation("batch rows", "nil decoder"))
	}
	core := s.Core()
	if err := validateBatch(core, batch); err != nil {
		err = notSent(err)
		runtimeOf(core).describeBatch(err, batch)
		return CASBatchRows[X]{}, err
	}
	// A batch known to hold no condition gets a VOID result, which has no
	// [applied] to report: reject it before sending, as ExecBatch rejects a
	// conditional one.
	if batch.routing.Kind == cql.OrdinaryStatement {
		err := notSent(cqltype.TypeValidation("RunConditionalBatch", "batch has no conditional statement; send it with ExecBatch"))
		runtimeOf(core).describeBatch(err, batch)
		return CASBatchRows[X]{}, err
	}
	r, rows, err := sendBatchRows(ctx, core, batch, decoder, options)
	if err != nil {
		return CASBatchRows[X]{}, err
	}
	return CASBatchRows[X]{tally: batch.casTallyFor(r), rows: rows}, nil
}
