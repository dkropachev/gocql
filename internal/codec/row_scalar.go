// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"io"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// Only the driver's built-in scalar codecs can implement this private hook.
// User codecs continue to receive their independent immutable ValueReader.
type rowValueIntoDecoder[T any] interface {
	decodeColumnInto(*RowReader, *T) error
}

func (c scalarCodec[T]) decodeColumnInto(r *RowReader, dst *T) error {
	if c.decodeRow == nil {
		value, err := r.nextValue()
		if err == nil {
			var decoded T
			decoded, err = c.Decode(value)
			if err == nil {
				*dst = decoded
			}
		}
		return err
	}
	value, err := c.decodeRow(r, c.kind, c.alternative, c.compiled)
	if err == nil {
		*dst = value
	}
	return err
}

// scalarColumn validates a full cell transactionally, leaving its present body
// on the row cursor for direct consumption. On a semantic error it skips the
// body, matching nextValue followed by scalarCodec.Decode. A zero width admits
// an arbitrary blob length; scalar widths require an exact match.
//
// The fast path accepts only a cell that already passes every check: a
// column of the codec's kind, a live lease, and a present body of the
// required width whose length prefix ends inside the cursor's head span. It
// consumes nothing until all hold; any other cell goes to scalarColumnSlow,
// which reproduces every error and cursor position. A zero DataType has kind
// TypeCustom, which no scalar codec uses, so the kind check also covers
// ValidateDataType.
func (r *RowReader) scalarColumn(kind, alternative cqltype.Type, compiled bool, width int64) (int64, error) {
	if r != nil && r.cursor != nil {
		if column, ok := r.columns.At(r.index); ok {
			k := column.Type.Kind()
			if prefix, ok := r.cursor.PeekHeadUint32(); ok && (k == kind || alternative != 0 && k == alternative) && (r.lease.IsZero() || r.lease.Alive()) {
				length := int64(int32(prefix))
				if length >= 0 && length <= r.cursor.Remaining()-4 && (width == 0 || length == width) {
					if !compiled {
						perfCheckType()
					}
					r.cursor.AdvanceHead(4)
					r.index++
					return length, nil
				}
			}
		}
	}
	return r.scalarColumnSlow(kind, alternative, compiled, width)
}

// scalarColumnSlow is scalarColumn for every cell the fast path declines:
// errors, nulls, missing cells and prefixes that reach a span boundary.
//
//go:noinline
func (r *RowReader) scalarColumnSlow(kind, alternative cqltype.Type, compiled bool, width int64) (int64, error) {
	if r == nil || r.cursor == nil {
		return 0, fmt.Errorf("gocql: nil row reader")
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return 0, fmt.Errorf("gocql: too many columns: index %d", r.index)
	}
	missing := r.allowMissing && r.cursor.Remaining() == 0
	var length int32 = -1
	if !missing {
		if err := cqltype.ValidateDataType(column.Type); err != nil {
			return 0, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, err)
		}
		if !r.lease.IsZero() && !r.lease.Alive() {
			return 0, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, cqltype.ErrReleased)
		}
		checkpoint := *r.cursor
		prefix, err := r.cursor.Uint32()
		length = int32(prefix)
		if err == nil {
			if length < -1 {
				err = fmt.Errorf("invalid cell length %d", length)
			} else if int64(length) > r.cursor.Remaining() {
				err = io.ErrUnexpectedEOF
			}
		}
		if err != nil {
			*r.cursor = checkpoint
			return 0, fmt.Errorf("gocql: column %d (%s): %w", r.index, column.Name, &cqltype.ProtocolError{Stage: "cell", Err: err})
		}
	}
	r.index++
	fail := func(err error) (int64, error) {
		if length >= 0 {
			_ = r.cursor.Skip(int64(length)) // the full length was checked above
		}
		return 0, err
	}
	// Standalone codecs retain their CheckType instrumentation contract.
	if !compiled {
		perfCheckType()
	}
	if err := checkScalarType(column.Type, kind, alternative); err != nil {
		return fail(err)
	}
	if length == -1 {
		return 0, fmt.Errorf("gocql: nonnullable %s cannot decode cell state %d", column.Type.Kind(), cqltype.Null)
	}
	if width > 0 && int64(length) != width {
		return fail(fmt.Errorf("gocql: %s requires %d bytes, got %d", column.Type.Kind(), width, length))
	}
	return int64(length), nil
}
