// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"math"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// ValueWriter writes one length-framed cell. The enclosing plan owns completion;
// an encoder writes only its body or one explicit null/unset marker.
//
// The driver passes one to an encoder; a zero ValueWriter rejects writes with a
// ValidationError.
type ValueWriter struct {
	err                         error
	builder                     *wire.Builder
	typ                         DataType
	mark                        wire.Mark
	start                       int64
	state                       CellState
	allowUnset, wrote, finished bool
	ownBorrowed                 bool
}

func newValueWriter(builder *wire.Builder, typ DataType, allowUnset bool) *ValueWriter {
	w := new(ValueWriter)
	initValueWriter(w, builder, typ, allowUnset)
	return w
}

func initValueWriter(w *ValueWriter, builder *wire.Builder, typ DataType, allowUnset bool) {
	*w = ValueWriter{builder: builder, typ: typ, allowUnset: allowUnset}
	if builder == nil {
		w.err = typeValidation("writer", "nil builder")
		return
	}
	w.mark = builder.Mark()
	w.start = builder.Len()
	if err := validateDataType(typ); err != nil {
		w.err = err
		return
	}
	w.err = builder.Uint32(0)
}
func newOwnedValueWriter(builder *wire.Builder, typ DataType, allowUnset bool) *ValueWriter {
	w := newValueWriter(builder, typ, allowUnset)
	w.ownBorrowed = true
	return w
}

// Type returns the type of the cell being written.
func (w *ValueWriter) Type() DataType { return w.typ }
func (w *ValueWriter) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return w.err
}
func (w *ValueWriter) ready() error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return w.fail(typeValidation("writer", "cell is already finished"))
	}
	if w.builder == nil {
		return w.fail(typeValidation("writer", "uninitialized writer"))
	}
	return nil
}
func (w *ValueWriter) payload() error {
	if err := w.ready(); err != nil {
		return err
	}
	if w.state != Present {
		return w.fail(typeValidation("value", "payload cannot be combined with null or unset"))
	}
	w.wrote = true
	return nil
}
func (w *ValueWriter) setState(state CellState) error {
	if err := w.ready(); err != nil {
		return err
	}
	if w.wrote || w.state != Present {
		return w.fail(typeValidation("value", "cell state was already written"))
	}
	if state == Unset && !w.allowUnset {
		return w.fail(typeValidation("value", "unset is not permitted in this cell"))
	}
	w.state = state
	return nil
}

// Empty marks the cell as an empty value: present, with a zero-length body.
// It fails for a type that does not support empty values, or after any
// other state or payload was written.
func (w *ValueWriter) Empty() error { return w.emptyValue() }

// Null marks the cell as null. It fails after a state or payload was
// written.
func (w *ValueWriter) Null() error { return w.setState(Null) }

// Unset marks the cell as unset, so the server leaves the parameter's column
// unchanged. It fails where unset is not permitted, such as in a nested value,
// and after a state or payload was written.
func (w *ValueWriter) Unset() error { return w.setState(Unset) }
func (w *ValueWriter) emptyValue() error {
	if !emptySupported(w.typ) {
		return w.fail(typeValidation("type", "%s does not support empty values", w.typ))
	}
	return w.setState(EmptyCell)
}

// BorrowBytes appends b to the cell's body without copying it, unless the
// writer copies borrowed input, as EncodeValue's does. The caller must keep b
// unchanged until the request or encoded value using it is no longer in use.
func (w *ValueWriter) BorrowBytes(b []byte) error {
	if err := w.payload(); err != nil {
		return err
	}
	if w.ownBorrowed {
		b = append([]byte(nil), b...)
	}
	return w.fail(w.builder.Borrow(wire.Borrow(b)))
}

// BorrowText appends s to the cell's body without copying it, unless the
// writer copies borrowed input, as EncodeValue's does.
func (w *ValueWriter) BorrowText(s string) error {
	if err := w.payload(); err != nil {
		return err
	}
	if w.ownBorrowed {
		return w.fail(w.builder.Borrow(wire.Borrow([]byte(s))))
	}
	return w.fail(w.builder.Borrow(wire.BorrowString(s)))
}
func (w *ValueWriter) borrowView(b Bytes) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(eachBytes(b, func(s wire.Span) error {
		if w.ownBorrowed {
			s = wire.Borrow(append([]byte(nil), s.Bytes()...))
		}
		return w.builder.Borrow(s)
	}))
}
func (w *ValueWriter) scalarBytes(n int) ([]byte, error) {
	if e := w.payload(); e != nil {
		return nil, e
	}
	p, e := w.builder.ScalarBytes(n)
	if e != nil {
		return nil, w.fail(e)
	}
	return p, nil
}

// Uint8 appends v to the cell's body.
func (w *ValueWriter) Uint8(v uint8) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint8(v))
}

// Uint16 appends v to the cell's body in big-endian order.
func (w *ValueWriter) Uint16(v uint16) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint16(v))
}

// Uint32 appends v to the cell's body in big-endian order.
func (w *ValueWriter) Uint32(v uint32) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint32(v))
}

// Uint64 appends v to the cell's body in big-endian order.
func (w *ValueWriter) Uint64(v uint64) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint64(v))
}

// Child appends a nested length-framed cell of type typ, written by encode.
// If encode fails, the nested cell is rolled back and its error becomes the
// writer's error.
func (w *ValueWriter) Child(typ DataType, encode func(*ValueWriter) error) error {
	if err := w.payload(); err != nil {
		return err
	}
	if encode == nil {
		return w.fail(typeValidation("encoder", "nil child encoder"))
	}
	child := newValueWriter(w.builder, typ, false)
	child.ownBorrowed = w.ownBorrowed
	if child.err != nil {
		abortValueWriter(child)
		return w.fail(child.err)
	}
	if err := encode(child); err != nil {
		abortValueWriter(child)
		return w.fail(err)
	}
	return w.fail(finishValueWriter(child))
}
func abortValueWriter(w *ValueWriter) {
	if w == nil || w.finished {
		return
	}
	if w.builder != nil {
		w.builder.Rollback(w.mark)
	}
	w.finished = true
}
func finishValueWriter(w *ValueWriter) error {
	if w == nil {
		return typeValidation("writer", "nil writer")
	}
	if err := w.ready(); err != nil {
		abortValueWriter(w)
		return err
	}
	length := w.builder.Len() - w.start - 4
	if length < 0 || length > math.MaxInt32 {
		w.fail(typeValidation("value", "cell length %d is outside signed 32-bit range", length))
	}
	var prefix uint32
	switch w.state {
	case Present:
		prefix = uint32(length)
	case EmptyCell:
		prefix = 0
	case Null:
		prefix = math.MaxUint32
	case Unset:
		prefix = math.MaxUint32 - 1
	default:
		w.fail(typeValidation("value", "invalid cell state %v", w.state))
	}
	if w.state != Present && length != 0 {
		w.fail(typeValidation("value", "null or unset cell contains a payload"))
	}
	if w.err == nil {
		w.fail(w.builder.PatchUint32(w.mark, prefix))
	}
	if w.err != nil {
		abortValueWriter(w)
		return w.err
	}
	w.finished = true
	return nil
}

// ValueReader is an immutable snapshot. Each Cursor starts at the beginning.
//
// The driver passes one to a decoder. The zero ValueReader is an empty
// present cell of the invalid zero DataType: its Cursor holds no bytes, so a
// read fails with a ProtocolError.
type ValueReader struct {
	err   error
	typ   DataType
	lease wire.Ref
	body  wire.Chain
	state CellState
}

// NewValueReader reads body, borrowed from lease. The caller holds a
// reference to lease while constructing the reader.
func valueReaderFromLease(typ DataType, state CellState, body wire.Chain, lease *wire.Lease) ValueReader {
	return newValueReader(typ, state, body, wire.RefOf(lease))
}

func newValueReader(typ DataType, state CellState, body wire.Chain, lease wire.Ref) ValueReader {
	r := ValueReader{typ: typ, state: state, body: body, lease: lease}
	r.err = validateDataType(typ)
	if state > EmptyCell || (state != Present && body.Len() != 0) {
		r.err = &ProtocolError{Stage: "cell", Err: fmt.Errorf("invalid state %v for %d bytes", state, body.Len())}
	}
	return r
}

// Type returns the cell's type.
func (r ValueReader) Type() DataType { return r.typ }

// State returns the cell's state: Present or Null. An empty value is
// Present with an empty body, as the response holds it; a decoder never sees
// EmptyCell.
func (r ValueReader) State() CellState { return r.state }

// Bytes returns the cell's body, a view with the lifetime of the response it
// was read from. It panics if the reader holds an invalid type or state,
// which the driver never passes to a decoder.
func (r ValueReader) Bytes() Bytes {
	// Invariant: err is set only for an invalid type or cell state, and the
	// driver builds readers only from validated column types and the
	// states it derives from a cell's length. The zero reader has no err.
	if r.err != nil {
		panic(r.err)
	}
	return bytesView(r.body, r.lease)
}

// Cursor returns a new cursor at the start of the cell's body.
func (r ValueReader) Cursor() *ValueCursor {
	c := r.cursorValue()
	return &c
}
func (r ValueReader) cursorValue() ValueCursor { return cursorFor(&r) }

// cursorFor reads r through a pointer, so a caller holding the reader on its
// stack does not copy it again.
func cursorFor(r *ValueReader) ValueCursor {
	return ValueCursor{cursor: wire.CursorOver(r.body), initialized: true, lease: r.lease, initial: r.body.Len(), err: r.err}
}

// ValueCursor reads checked network scalars and nested cells across spans. A
// failure is sticky; malformed data cannot be followed by a successful read.
//
// A zero ValueCursor rejects reads with a ValidationError.
type ValueCursor struct {
	err         error
	lease       wire.Ref
	cursor      wire.Cursor
	initial     int64
	initialized bool
}

// Remaining returns the number of unread bytes, or 0 for a nil or zero
// cursor.
func (c *ValueCursor) Remaining() int64 {
	if c == nil || !c.initialized {
		return 0
	}
	return c.cursor.Remaining()
}
func (c *ValueCursor) check() error {
	if c.err != nil {
		return c.err
	}
	if !c.initialized {
		c.err = typeValidation("cursor", "uninitialized cursor")
	} else if !c.lease.IsZero() && !c.lease.Alive() {
		c.err = ErrReleased
	}
	return c.err
}
func (c *ValueCursor) failure(err error) error {
	if err != nil && c.err == nil {
		c.err = &ProtocolError{Offset: c.initial - c.Remaining(), Stage: "value", Err: err}
	}
	return c.err
}

// Uint8 reads one byte.
func (c *ValueCursor) Uint8() (uint8, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint8()
	return v, c.failure(err)
}

// Uint16 reads a big-endian uint16.
func (c *ValueCursor) Uint16() (uint16, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint16()
	return v, c.failure(err)
}

// Uint32 reads a big-endian uint32.
func (c *ValueCursor) Uint32() (uint32, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint32()
	return v, c.failure(err)
}

// Uint64 reads a big-endian uint64.
func (c *ValueCursor) Uint64() (uint64, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint64()
	return v, c.failure(err)
}

// Take returns the next n bytes as a view with the lifetime of the cell, and
// advances past them.
func (c *ValueCursor) Take(n int64) (Bytes, error) {
	if err := c.check(); err != nil {
		return Bytes{}, err
	}
	body, err := c.cursor.TakeNested(n)
	if err != nil {
		return Bytes{}, c.failure(err)
	}
	return bytesView(body, c.lease), nil
}

// Child reads a nested length-framed cell of type typ and advances past it.
func (c *ValueCursor) Child(typ DataType) (ValueReader, error) {
	if err := c.check(); err != nil {
		return ValueReader{}, err
	}
	r, err := readFramedValue(&c.cursor, typ, c.lease, false, true)
	if err != nil {
		return ValueReader{}, c.failure(err)
	}
	return r, nil
}

// RequireEnd returns a ProtocolError if unread bytes remain.
func (c *ValueCursor) RequireEnd() error {
	if err := c.check(); err != nil {
		return err
	}
	return c.failure(c.cursor.RequireEnd())
}

// readValue validates a full framed cell before advancing the caller's cursor.
// Zero-length cells remain Present; EmptyCodec interprets scalar empty values.
func readValue(cursor *wire.Cursor, typ DataType, lease *wire.Lease, allowUnset bool) (ValueReader, error) {
	return readFramedValue(cursor, typ, wire.RefOf(lease), allowUnset, false)
}
func readFramedValue(cursor *wire.Cursor, typ DataType, lease wire.Ref, allowUnset, nested bool) (ValueReader, error) {
	if err := validateDataType(typ); err != nil {
		return ValueReader{}, err
	}
	if !lease.IsZero() && !lease.Alive() {
		return ValueReader{}, ErrReleased
	}
	if cursor == nil {
		return ValueReader{}, typeValidation("cursor", "nil cursor")
	}
	checkpoint := *cursor
	fail := func(err error) (ValueReader, error) {
		*cursor = checkpoint
		return ValueReader{}, &ProtocolError{Stage: "cell", Err: err}
	}
	prefix, err := cursor.Uint32()
	if err != nil {
		return fail(err)
	}
	length := int32(prefix)
	if length == -1 {
		return newValueReader(typ, Null, wire.Chain{}, lease), nil
	}
	if length == -2 && allowUnset {
		return newValueReader(typ, Unset, wire.Chain{}, lease), nil
	}
	if length < 0 {
		return fail(fmt.Errorf("invalid cell length %d", length))
	}
	var body wire.Chain
	if nested {
		body, err = cursor.TakeNested(int64(length))
	} else {
		body, err = cursor.Take(int64(length))
	}
	if err != nil {
		return fail(err)
	}
	return newValueReader(typ, Present, body, lease), nil
}
