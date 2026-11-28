// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"io"
	"strconv"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
)

// BoundValues is a view of the values bound to a statement, as the driver
// encoded them for the request: RequestStart.Values, and
// BatchStatement.Values for each statement of a batch.
//
// It is borrowed: it reads the request's own storage, which the driver
// releases or reuses for another request once the request ends. It is valid
// until the request's end function, the one the Request hook returned,
// returns, or until the Request hook returns when it returns none. Read or
// Format what you need before then; a cql.ValueReader that At returned, and
// a cql.Value decoded from it, are borrowed the same way, so detach a value
// you keep with cql.Value.Clone.
//
// Bound values often hold personal data or secrets. Logging or exporting
// them is the application's decision: Format takes a redact function that
// keeps chosen values out of its output.
//
// The zero BoundValues holds no value.
type BoundValues struct {
	src     any
	at      func(src any, i int) cql.ValueReader
	columns cql.Columns
	n       int
}

func newBoundValues(columns cql.Columns, n int, src any, at func(src any, i int) cql.ValueReader) BoundValues {
	return BoundValues{columns: columns, n: n, src: src, at: at}
}

func init() { link.Set(link.NewBoundValues, newBoundValues) }

// Len returns the number of values.
func (v BoundValues) Len() int { return v.n }

// At returns the column of the bind marker the i-th value is bound to,
// whose Name is empty for a marker without one, and a reader over the
// value. Decode it with a codec, such as cql.DynamicCodec.Decode for a
// cql.Value. At panics if i is out of range.
func (v BoundValues) At(i int) (cql.ColumnSpec, cql.ValueReader) {
	if i < 0 || i >= v.n {
		panic("observe: BoundValues.At index " + strconv.Itoa(i) + " out of range [0, " + strconv.Itoa(v.n) + ")")
	}
	column, _ := v.columns.At(i)
	return column, v.at(v.src, i)
}

// Format writes the values to w as a comma-separated list of name=value
// pairs, in order, each value a CQL literal as cql.Value.String prints it,
// such as id=42, name='Ann'. A marker without a name is written as its
// 0-based position. A value for which redact returns true is written as
// <redacted>, without being decoded; a nil redact redacts none. A value that
// does not decode is written as <error: ...>. Format returns the first error
// w returned.
func (v BoundValues) Format(w io.Writer, redact func(cql.ColumnSpec) bool) error {
	buf := make([]byte, 0, 64)
	for i := range v.n {
		column, reader := v.At(i)
		buf = buf[:0]
		if i > 0 {
			buf = append(buf, ", "...)
		}
		if column.Name != "" {
			buf = append(buf, column.Name...)
		} else {
			buf = strconv.AppendInt(buf, int64(i), 10)
		}
		buf = append(buf, '=')
		switch {
		case redact != nil && redact(column):
			buf = append(buf, "<redacted>"...)
		case reader.State() == cql.CellNull:
			buf = append(buf, "null"...)
		case reader.State() == cql.CellUnset:
			buf = append(buf, "unset"...)
		case reader.State() == cql.CellEmpty:
			buf = append(buf, "empty"...)
		default:
			value, err := cql.DynamicCodec.Decode(reader)
			if err != nil {
				buf = append(buf, "<error: "...)
				buf = append(buf, err.Error()...)
				buf = append(buf, '>')
			} else {
				buf = append(buf, value.String()...)
			}
		}
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}
