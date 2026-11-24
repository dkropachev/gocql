// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// CASResult is a row of a conditional statement's result: Applied reports
// whether the statement applied, and Previous holds the existing row the server
// returned with it, if any. Its zero value did not apply and holds no row.
type CASResult[R any] struct {
	// Previous holds the existing row the server returned, if any.
	Previous Option[R]
	// Applied reports whether the statement applied.
	Applied bool
}
type casRows[R any] struct{ decoder RowDecoder[R] }
type compiledCASRow[R any] struct {
	previous CompiledRow[R]
	columns  Columns
}

// lenientRowDecoder is a field-mapped decoder that can compile a plan which
// skips columns it has no field for and leaves fields without a column zero.
type lenientRowDecoder[R any] interface {
	compileLenient(columns Columns) (CompiledRow[R], error)
}

// CASRows decodes the [applied] column and the conditional result with decoder.
func CASRows[R any](decoder RowDecoder[R]) RowDecoder[CASResult[R]] {
	return casRows[R]{decoder: decoder}
}
func (c casRows[R]) Compile(columns Columns) (CompiledRow[CASResult[R]], error) {
	first, ok := columns.At(0)
	if !ok || first.Name != "[applied]" {
		return nil, typeValidation("CAS result", "first column must be [applied]")
	}
	if err := BoolCodec.CheckType(first.Type); err != nil {
		return nil, columnError(CodecStageCompile, 0, first.Name, err)
	}
	_, previous, err := splitColumns(columns, 1)
	if err != nil {
		return nil, err
	}
	p := compiledCASRow[R]{columns: previous}
	if previous.Len() > 0 {
		if c.decoder == nil {
			return nil, typeValidation("CAS result", "nil previous-value decoder")
		}
		// The previous row's columns differ between Cassandra and ScyllaDB
		// and between outcomes, so a field-mapped decoder compiles leniently.
		if lenient, ok := c.decoder.(lenientRowDecoder[R]); ok {
			p.previous, err = lenient.compileLenient(previous)
		} else {
			p.previous, err = compileRow(c.decoder, previous)
		}
		if err != nil {
			return nil, rebaseColumnError(err, 1)
		}
	}
	return p, nil
}
func (p compiledCASRow[R]) DecodeInto(reader *RowReader, dst *CASResult[R]) error {
	if dst == nil {
		return typeValidation("destination", "nil CAS destination")
	}
	*dst = CASResult[R]{}
	at := reader.index
	if err := readColumnInto(reader, BoolCodec, &dst.Applied); err != nil {
		column, _ := reader.columns.At(at)
		return columnError(CodecStageDecode, at, column.Name, err)
	}
	if p.previous == nil {
		return nil
	}
	if skipNullPrevious(reader, p.columns.Len()) {
		return nil
	}
	columns, index := reader.columns, reader.index
	reader.columns, reader.index = p.columns, 0
	err := p.previous.DecodeInto(reader, &dst.Previous.Value)
	if err == nil {
		err = finishRowReader(reader)
	}
	reader.columns, reader.index = columns, index
	if err != nil {
		return rebaseColumnError(err, index)
	}
	reader.index += p.columns.Len()
	dst.Previous.Set = true
	return nil
}
func (c casRows[R]) Clone(value CASResult[R]) CASResult[R] {
	if value.Previous.Set {
		value.Previous.Value = c.decoder.Clone(value.Previous.Value)
	}
	return value
}

// skipNullPrevious consumes the previous-row cells of a conditional result
// when all of them are null, and reports whether it did. ScyllaDB sends the
// previous row's columns whatever the outcome; all null, they mean there was
// no previous row, which a decoder with non-nullable fields could not decode.
func skipNullPrevious(reader *RowReader, n int) bool {
	if reader.cursor == nil || reader.cursor.Remaining() == 0 {
		return false
	}
	scan := *reader.cursor
	for range n {
		prefix, err := scan.Uint32()
		if err != nil || int32(prefix) != -1 {
			return false
		}
	}
	*reader.cursor = scan
	reader.index += n
	return true
}
