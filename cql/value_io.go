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

func NewValueWriter(builder *wire.Builder, typ DataType, allowUnset bool) *ValueWriter {
	w := new(ValueWriter)
	initValueWriter(w, builder, typ, allowUnset)
	return w
}

func initValueWriter(w *ValueWriter, builder *wire.Builder, typ DataType, allowUnset bool) {
	*w = ValueWriter{builder: builder, typ: typ, allowUnset: allowUnset}
	if builder == nil {
		w.err = TypeValidation("writer", "nil builder")
		return
	}
	w.mark = builder.Mark()
	w.start = builder.Len()
	if err := ValidateDataType(typ); err != nil {
		w.err = err
		return
	}
	w.err = builder.Uint32(0)
}
func newOwnedValueWriter(builder *wire.Builder, typ DataType, allowUnset bool) *ValueWriter {
	w := NewValueWriter(builder, typ, allowUnset)
	w.ownBorrowed = true
	return w
}
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
		return w.fail(TypeValidation("writer", "cell is already finished"))
	}
	if w.builder == nil {
		return w.fail(TypeValidation("writer", "uninitialized writer"))
	}
	return nil
}
func (w *ValueWriter) payload() error {
	if err := w.ready(); err != nil {
		return err
	}
	if w.state != Present {
		return w.fail(TypeValidation("value", "payload cannot be combined with null or unset"))
	}
	w.wrote = true
	return nil
}
func (w *ValueWriter) setState(state CellState) error {
	if err := w.ready(); err != nil {
		return err
	}
	if w.wrote || w.state != Present {
		return w.fail(TypeValidation("value", "cell state was already written"))
	}
	if state == Unset && !w.allowUnset {
		return w.fail(TypeValidation("value", "unset is not permitted in this cell"))
	}
	w.state = state
	return nil
}
func (w *ValueWriter) Empty() error { return w.emptyValue() }
func (w *ValueWriter) Null() error  { return w.setState(Null) }
func (w *ValueWriter) Unset() error { return w.setState(Unset) }
func (w *ValueWriter) emptyValue() error {
	if !emptySupported(w.typ) {
		return w.fail(TypeValidation("type", "%s does not support empty values", w.typ))
	}
	return w.setState(EmptyCell)
}
func (w *ValueWriter) BorrowBytes(b []byte) error {
	if err := w.payload(); err != nil {
		return err
	}
	if w.ownBorrowed {
		b = append([]byte(nil), b...)
	}
	return w.fail(w.builder.Borrow(wire.Borrow(b)))
}
func (w *ValueWriter) BorrowString(s string) error {
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
	return w.fail(EachBytes(b, func(s wire.Span) error {
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
func (w *ValueWriter) Uint8(v uint8) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint8(v))
}
func (w *ValueWriter) Uint16(v uint16) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint16(v))
}
func (w *ValueWriter) Uint32(v uint32) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint32(v))
}
func (w *ValueWriter) Uint64(v uint64) error {
	if err := w.payload(); err != nil {
		return err
	}
	return w.fail(w.builder.Uint64(v))
}
func (w *ValueWriter) Child(typ DataType, encode func(*ValueWriter) error) error {
	if err := w.payload(); err != nil {
		return err
	}
	if encode == nil {
		return w.fail(TypeValidation("encoder", "nil child encoder"))
	}
	child := NewValueWriter(w.builder, typ, false)
	child.ownBorrowed = w.ownBorrowed
	if child.err != nil {
		abortValueWriter(child)
		return w.fail(child.err)
	}
	if err := encode(child); err != nil {
		abortValueWriter(child)
		return w.fail(err)
	}
	return w.fail(FinishValueWriter(child))
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
func FinishValueWriter(w *ValueWriter) error {
	if w == nil {
		return TypeValidation("writer", "nil writer")
	}
	if err := w.ready(); err != nil {
		abortValueWriter(w)
		return err
	}
	length := w.builder.Len() - w.start - 4
	if length < 0 || length > math.MaxInt32 {
		w.fail(TypeValidation("value", "cell length %d is outside signed 32-bit range", length))
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
		w.fail(TypeValidation("value", "invalid cell state %d", w.state))
	}
	if w.state != Present && length != 0 {
		w.fail(TypeValidation("value", "null or unset cell contains a payload"))
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
type ValueReader struct {
	err   error
	typ   DataType
	lease wire.Ref
	body  wire.Chain
	state CellState
}

// NewValueReader reads body, borrowed from lease. The caller holds a
// reference to lease while constructing the reader.
func NewValueReader(typ DataType, state CellState, body wire.Chain, lease *wire.Lease) ValueReader {
	return newValueReader(typ, state, body, wire.RefOf(lease))
}

func newValueReader(typ DataType, state CellState, body wire.Chain, lease wire.Ref) ValueReader {
	r := ValueReader{typ: typ, state: state, body: body, lease: lease}
	r.err = ValidateDataType(typ)
	if state > EmptyCell || (state != Present && body.Len() != 0) {
		r.err = &ProtocolError{Stage: "cell", Err: fmt.Errorf("invalid state %d for %d bytes", state, body.Len())}
	}
	return r
}
func (r ValueReader) Type() DataType   { return r.typ }
func (r ValueReader) State() CellState { return r.state }
func (r ValueReader) Bytes() Bytes {
	if r.err != nil {
		panic(r.err)
	}
	return BytesView(r.body, r.lease)
}
func (r ValueReader) Cursor() *ValueCursor {
	c := r.cursorValue()
	return &c
}
func (r ValueReader) cursorValue() ValueCursor {
	return ValueCursor{cursor: *wire.NewCursor(r.body), initialized: true, lease: r.lease, initial: r.body.Len(), err: r.err}
}

// ValueCursor reads checked network scalars and nested cells across spans. A
// failure is sticky; malformed data cannot be followed by a successful read.
type ValueCursor struct {
	err         error
	lease       wire.Ref
	cursor      wire.Cursor
	initial     int64
	initialized bool
}

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
		c.err = TypeValidation("cursor", "uninitialized cursor")
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
func (c *ValueCursor) Uint8() (uint8, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint8()
	return v, c.failure(err)
}
func (c *ValueCursor) Uint16() (uint16, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint16()
	return v, c.failure(err)
}
func (c *ValueCursor) Uint32() (uint32, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint32()
	return v, c.failure(err)
}
func (c *ValueCursor) Uint64() (uint64, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	v, err := c.cursor.Uint64()
	return v, c.failure(err)
}
func (c *ValueCursor) Take(n int64) (Bytes, error) {
	if err := c.check(); err != nil {
		return Bytes{}, err
	}
	body, err := c.cursor.TakeNested(n)
	if err != nil {
		return Bytes{}, c.failure(err)
	}
	return BytesView(body, c.lease), nil
}
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
func (c *ValueCursor) RequireEnd() error {
	if err := c.check(); err != nil {
		return err
	}
	return c.failure(c.cursor.RequireEnd())
}

// readValue validates a full framed cell before advancing the caller's cursor.
// Zero-length cells remain Present; EmptyCodec interprets scalar empty values.
func ReadValue(cursor *wire.Cursor, typ DataType, lease *wire.Lease, allowUnset bool) (ValueReader, error) {
	return readFramedValue(cursor, typ, wire.RefOf(lease), allowUnset, false)
}
func readFramedValue(cursor *wire.Cursor, typ DataType, lease wire.Ref, allowUnset, nested bool) (ValueReader, error) {
	if err := ValidateDataType(typ); err != nil {
		return ValueReader{}, err
	}
	if !lease.IsZero() && !lease.Alive() {
		return ValueReader{}, ErrReleased
	}
	if cursor == nil {
		return ValueReader{}, TypeValidation("cursor", "nil cursor")
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
