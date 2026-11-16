// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// casTally is what a CASBatchRows has learned from the rows read so far.
// The Batch keeps it with the execution's storage, so that tracking it
// allocates nothing. scratch is the destination Applied decodes the rows
// the caller did not read into; R is always a CASResult.
type casTally[R any] struct {
	scratch R
	// notApplied reports a row with Applied false.
	notApplied bool
	// done reports that the rows were read to their end; closed, that
	// they were closed before it.
	done, closed bool
}

func (t *casTally[R]) reset() {
	var zero R
	t.scratch, t.notApplied, t.done, t.closed = zero, false, false, false
}

// CASBatchRows are the rows of a conditional batch, each reporting whether
// the batch applied, and whether it applied over all of them.
//
// Like Rows, CASBatchRows is a small value whose storage the Batch reuses
// for its next execution once these rows are closed; a handle kept from an
// earlier execution is stale. It is not safe for concurrent use.
type CASBatchRows[X any] struct {
	tally *casTally[codec.CASResult[X]]
	rows  Rows[codec.CASResult[X]]
}

// NextInto decodes the next row into dst and reports whether there was one.
func (r CASBatchRows[X]) NextInto(dst *codec.CASResult[X]) bool {
	if r.rows.NextInto(dst) {
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
// has not, so it can be called without iterating, or after. Rows closed
// before they were read to their end cannot tell, and return an error.
func (r CASBatchRows[X]) Applied() (bool, error) {
	if r.tally == nil {
		return false, cqltype.TypeValidation("CASBatchRows", "zero rows handle")
	}
	if !r.rows.stale() && !r.tally.done && !r.tally.closed {
		for r.NextInto(&r.tally.scratch) {
		}
	}
	if err := r.rows.Err(); err != nil {
		return false, err
	}
	if !r.tally.done {
		return false, cqltype.TypeValidation("CASBatchRows", "rows closed before Applied read them")
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
func (r CASBatchRows[X]) Columns() cqltype.Columns { return r.rows.Columns() }

// Clone returns an owned copy of the row the last NextInto decoded.
func (r CASBatchRows[X]) Clone() (codec.CASResult[X], error) { return r.rows.Clone() }

// Retain keeps the current row's page alive past advancement.
func (r CASBatchRows[X]) Retain() (*PageLease, error) { return r.rows.Retain() }

// Info returns the execution's result information so far.
func (r CASBatchRows[X]) Info() model.Result { return r.rows.Info() }

// RunConditionalBatch sends batch as RunBatch does and returns its rows as
// CASBatchRows, whose Applied reports whether the batch applied. decoder is
// a CASRows decoder, typically the Rows of a member's statement.
func (s *Session) RunConditionalBatch[X any](ctx context.Context, batch *Batch, decoder codec.RowDecoder[codec.CASResult[X]], options CompiledOptions) (CASBatchRows[X], error) {
	if s == nil {
		return CASBatchRows[X]{}, cqltype.TypeValidation("session", "nil session")
	}
	if decoder == nil {
		return CASBatchRows[X]{}, cqltype.TypeValidation("batch rows", "nil decoder")
	}
	r, rows, err := sendBatchRows(ctx, s.Core(), batch, decoder, options)
	if err != nil {
		return CASBatchRows[X]{}, err
	}
	return CASBatchRows[X]{tally: &r.cas, rows: rows}, nil
}
