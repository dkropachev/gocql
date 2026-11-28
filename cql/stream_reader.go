/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cql

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// StreamCapacityError reports required final storage without implicit growth.
// The driver returns it filled in; its zero value is an error without detail.
type StreamCapacityError struct {
	// Path names the column of the value.
	Path string
	// Required is the capacity the value needs, in bytes or elements, and
	// Available the capacity the destination has.
	Required, Available int64
}

// Error names the value's path and the required and available capacity.
func (e *StreamCapacityError) Error() string {
	return fmt.Sprintf("gocql: streaming capacity at %s: need %d, have %d", e.Path, e.Required, e.Available)
}

// StreamCapabilityError identifies a mode or mapping without an incremental path.
//
// The driver returns it filled in; its zero value is an error without detail.
type StreamCapabilityError struct {
	// Reason says what cannot be streamed.
	Reason string
	// Protocol is the connection's protocol version.
	Protocol int
	// Compression is the connection's compression.
	Compression Compression
}

// Error names the protocol version, the compression and the reason.
func (e *StreamCapabilityError) Error() string {
	return fmt.Sprintf("gocql: streaming protocol %d/compression %v: %s", e.Protocol, e.Compression, e.Reason)
}

// StreamDecoder compiles a value decoder that writes into seeded caller storage.
type StreamDecoder[T any] interface {
	// Compile checks the decoder against the column type and returns the plan
	// that decodes values of it.
	Compile(DataType) (CompiledStreamValue[T], error)
	// Reset clears the value at the pointer, keeping its buffer capacity,
	// without a column type: it resets a field the result does not select.
	Reset(*T)
}

// CompiledStreamValue decodes one value of a compiled column type into caller
// storage.
type CompiledStreamValue[T any] interface {
	// Reset clears the logical value at the pointer, keeping its buffer
	// capacity.
	Reset(*T)
	// DecodeInto reads one value from the reader into the destination.
	DecodeInto(*StreamValueReader, *T) error
}

// StreamRowDecoder compiles row mappings that decode into seeded caller storage.
type StreamRowDecoder[R any] interface {
	// Compile checks the decoder against the result columns and returns the
	// plan that decodes rows of them.
	Compile(Columns) (CompiledStreamRow[R], error)
}

// CompiledStreamRow decodes rows of compiled metadata into caller storage through a
// StreamRowReader.
type CompiledStreamRow[R any] interface {
	// Reset clears the row at the pointer, keeping its buffer capacity.
	Reset(*R)
	// DecodeInto reads one row from the reader into the destination.
	DecodeInto(*StreamRowReader, *R) error
}

type streamCapture struct {
	dst    []byte
	offset int
}
type streamDecodeState struct {
	err          error
	typeMatches  map[streamTypePair]bool
	rowMatches   map[streamColumnsPair]bool
	childMatches map[streamChildPair]*streamValidationPlan
	values       []*StreamValueReader
	rows         []*StreamRowReader
	discard      []byte
	discardBytes int
	scalar       [32]byte
}

func (s *streamDecodeState) value(depth int) *StreamValueReader {
	for len(s.values) <= depth {
		s.values = append(s.values, new(StreamValueReader))
	}
	return s.values[depth]
}
func (s *streamDecodeState) row(depth int) *StreamRowReader {
	for len(s.rows) <= depth {
		s.rows = append(s.rows, new(StreamRowReader))
	}
	return s.rows[depth]
}
func (s *streamDecodeState) fail(err error) error {
	if s.err == nil {
		s.err = err
	}
	return s.err
}

// StreamValueReader is a scoped, limited reader over one streamed value.
//
// The driver passes one to a stream decoder; a zero StreamValueReader rejects
// reads with a ValidationError.
type StreamValueReader struct {
	scope        *wire.Stream
	typ          DataType
	shared       *streamDecodeState
	validation   *streamValidationPlan
	capture      *streamCapture
	path         string
	captureState streamCapture
	depth        int
	state        CellState
	active       bool
}

// Context returns the context of the streaming read, or context.Background
// for a nil or zero reader.
func (r *StreamValueReader) Context() context.Context {
	if r == nil || r.scope == nil {
		return context.Background()
	}
	return r.scope.Context()
}

// Type returns the value's type.
func (r *StreamValueReader) Type() DataType { return r.typ }

// State returns the value's cell state.
func (r *StreamValueReader) State() CellState { return r.state }

// Remaining returns the number of unread bytes of the value, or 0 for a nil or
// zero reader.
func (r *StreamValueReader) Remaining() int64 {
	if r == nil || r.scope == nil {
		return 0
	}
	return r.scope.Remaining()
}
func (r *StreamValueReader) check() error {
	if r == nil || r.scope == nil || r.shared == nil {
		return typeValidation("stream reader", "uninitialized value reader")
	}
	if r.shared.err != nil {
		return r.shared.err
	}
	if !r.active {
		return r.shared.fail(typeValidation("stream reader", "reader escaped its callback"))
	}
	return nil
}
func (r *StreamValueReader) fail(err error) error {
	if r == nil || r.shared == nil {
		return err
	}
	return r.shared.fail(err)
}

// Read reads up to len(p) bytes of the value, implementing io.Reader. It
// returns io.EOF at the end of the value, and an error once the reader is
// used after the callback it was passed to returned. Errors are sticky.
func (r *StreamValueReader) Read(p []byte) (int, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	var n int
	var err error
	if r.capture == nil {
		n, err = r.scope.Read(p)
	} else {
		take := min(int64(len(p)), r.Remaining())
		if take > int64(len(r.capture.dst)-r.capture.offset) {
			return 0, r.fail(wire.ErrBounds)
		}
		target := r.capture.dst[r.capture.offset : r.capture.offset+int(take)]
		n, err = r.scope.Read(target)
		copy(p, target[:n]) // Bounded scalar/framing scratch, never a payload gather.
		r.capture.offset += n
	}
	if err != nil && err != io.EOF { //nolint:errorlint // io.Reader returns io.EOF unwrapped; hot path
		return n, r.fail(err)
	}
	return n, err
}
func (r *StreamValueReader) readFull(p []byte) error {
	if err := r.check(); err != nil {
		return err
	}
	if int64(len(p)) > r.Remaining() {
		return r.fail(io.ErrUnexpectedEOF)
	}
	for len(p) > 0 {
		n, err := r.Read(p)
		p = p[n:]
		if err != nil {
			if err == io.EOF && len(p) == 0 { //nolint:errorlint // io.Reader returns io.EOF unwrapped; hot path
				return nil
			}
			return r.fail(err)
		}
	}
	return nil
}
func (r *StreamValueReader) uint(size int) (uint64, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	b := r.shared.scalar[:size]
	if err := r.readFull(b); err != nil {
		return 0, err
	}
	switch size {
	case 1:
		return uint64(b[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(b)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(b)), nil
	default:
		return binary.BigEndian.Uint64(b), nil
	}
}

// Uint8 reads one byte.
func (r *StreamValueReader) Uint8() (uint8, error) { n, e := r.uint(1); return uint8(n), e }

// Uint16 reads a big-endian uint16.
func (r *StreamValueReader) Uint16() (uint16, error) { n, e := r.uint(2); return uint16(n), e }

// Uint32 reads a big-endian uint32.
func (r *StreamValueReader) Uint32() (uint32, error) { n, e := r.uint(4); return uint32(n), e }

// Uint64 reads a big-endian uint64.
func (r *StreamValueReader) Uint64() (uint64, error) { return r.uint(8) }

// RequireEnd returns an error if unread bytes of the value remain.
func (r *StreamValueReader) RequireEnd() error {
	if err := r.check(); err != nil {
		return err
	}
	if err := r.scope.RequireEnd(); err != nil {
		return r.fail(err)
	}
	return nil
}
func (r *StreamValueReader) present() error {
	if err := r.check(); err != nil {
		return err
	}
	if r.state == CellNull {
		return r.fail(typeValidation("stream value", "nonnullable %s has state null: %w", r.typ, ErrNull))
	}
	if r.state != CellPresent {
		return r.fail(typeValidation("stream value", "nonnullable %s has state %v", r.typ, r.state))
	}
	return nil
}
func (r *StreamValueReader) capacity(n int64, available int) error {
	if n < 0 || n > int64(available) {
		return r.fail(&StreamCapacityError{Path: r.path, Required: n, Available: int64(available)})
	}
	return nil
}
func (r *StreamValueReader) childBody(typ DataType, state CellState, size int64, validation *streamValidationPlan, fn func(*StreamValueReader) error) error {
	if err := r.check(); err != nil {
		return err
	}
	if fn == nil {
		return r.fail(typeValidation("stream child", "nil callback"))
	}
	err := r.scope.Child(size, func(scope *wire.Stream) error {
		child := r.shared.value(r.depth + 1)
		*child = StreamValueReader{scope: scope, typ: typ, state: state, shared: r.shared, validation: validation, capture: r.capture, depth: r.depth + 1, path: r.path, active: true}
		defer func() { child.active = false; child.capture = nil }()
		if err := fn(child); err != nil {
			return err
		}
		return child.RequireEnd()
	})
	if err != nil {
		return r.fail(err)
	}
	return nil
}
func (r *StreamValueReader) child(typ DataType, validation *streamValidationPlan, allowNull bool, fn func(*StreamValueReader) error) error {
	length, err := r.Uint32()
	if err != nil {
		return err
	}
	n := int64(int32(length))
	state := CellPresent
	if n == -1 && allowNull {
		state, n = CellNull, 0
	} else if n < 0 {
		return r.fail(typeValidation("stream length", "invalid nested cell length %d", n))
	}
	return r.childBody(typ, state, n, validation, fn)
}

// Child reads one signed-length framed member. Result unset is always invalid.
func (r *StreamValueReader) Child(typ DataType, fn func(*StreamValueReader) error) error {
	if err := validateDataType(typ); err != nil {
		return r.fail(err)
	}
	validation, err := r.childValidation(typ)
	if err != nil {
		return r.fail(err)
	}
	return r.child(typ, validation, true, fn)
}

// Skip validates the entire discarded value, including nested scalar semantics.
func (r *StreamValueReader) Skip() error {
	if err := r.check(); err != nil {
		return err
	}
	p := r.validation
	if p == nil {
		var err error
		p, err = compileStreamValidation(r.typ, 0)
		if err != nil {
			return r.fail(err)
		}
	}
	return p.skip(r, true)
}
func (r *StreamValueReader) rawSkip(validate func([]byte) error) error {
	if err := r.check(); err != nil {
		return err
	}
	if r.shared.discard == nil && r.capture == nil {
		r.shared.discard = make([]byte, r.shared.discardBytes)
	}
	for r.Remaining() > 0 {
		var b []byte
		if r.capture != nil {
			n := min(r.Remaining(), int64(r.shared.discardBytes))
			if n > int64(len(r.capture.dst)-r.capture.offset) {
				return r.fail(wire.ErrBounds)
			}
			b = r.capture.dst[r.capture.offset : r.capture.offset+int(n)]
			nread, err := r.scope.Read(b)
			r.capture.offset += nread
			b = b[:nread]
			if validate != nil {
				if e := validate(b); e != nil {
					return r.fail(e)
				}
			}
			if err != nil && !(err == io.EOF && r.Remaining() == 0) { //nolint:errorlint // io.Reader returns io.EOF unwrapped; hot path
				return r.fail(err)
			}
		} else {
			b = r.shared.discard[:min(int64(len(r.shared.discard)), r.Remaining())]
			if err := r.readFull(b); err != nil {
				return err
			}
			if validate != nil {
				if err := validate(b); err != nil {
					return r.fail(err)
				}
			}
		}
	}
	return nil
}
func (r *StreamValueReader) count(minimum int64) (int, error) {
	v, err := r.Uint32()
	if err != nil {
		return 0, err
	}
	n := int64(int32(v))
	if n < 0 {
		return 0, r.fail(wire.ErrLength)
	}
	if n > r.scope.Limits().MaxCount || (minimum > 0 && n > r.Remaining()/minimum) {
		return 0, r.fail(wire.ErrLimit)
	}
	return int(n), nil
}
func (r *StreamValueReader) vectorLength() (int64, error) {
	first, err := r.Uint8()
	if err != nil {
		return 0, err
	}
	extra := bits.LeadingZeros8(^first)
	n := uint64(first & byte(0xff>>uint(extra)))
	for i := 0; i < extra; i++ {
		b, e := r.Uint8()
		if e != nil {
			return 0, e
		}
		n = n<<8 | uint64(b)
	}
	if (extra > 0 && n < uint64(1)<<uint(extra*7)) || n > math.MaxInt32 {
		return 0, r.fail(typeValidation("stream vector length", "invalid or noncanonical element length"))
	}
	return int64(n), nil
}

// StreamRowReader is a scoped, checked reader over one streamed row. Custom plans
// read each column with its ReadColumn method and skip one with SkipColumn;
// it never exposes retained byte views.
//
// The driver passes one to a stream row decoder; a zero StreamRowReader rejects
// reads with a ValidationError.
type StreamRowReader struct {
	scope        *wire.Stream
	shared       *streamDecodeState
	capture      *streamCapture
	columns      Columns
	validations  []*streamValidationPlan
	index, depth int
	allowMissing bool
	active       bool
	// nullZero makes the next ReadColumn reset its destination for a NULL
	// or missing cell instead of failing (NullAsZero).
	nullZero bool
}

// streamReaderWorkingStorageBytes reports the actual retained byte
// scratch plus conservative scope descriptor capacities. Validation plans/maps
// depend on schema and are accounted separately from payload working storage.
// Captures alias final application storage and must not be counted again.
func streamReaderWorkingStorageBytes(r *StreamRowReader) int64 {
	if r == nil || r.shared == nil {
		return 0
	}
	return int64(cap(r.shared.discard)) + 32 + int64(cap(r.shared.values))*192 + int64(cap(r.shared.rows))*128
}

func newStreamRowReader(columns Columns, scope *wire.Stream) (*StreamRowReader, error) {
	if scope == nil {
		return nil, typeValidation("stream reader", "nil envelope")
	}
	state := &streamDecodeState{discardBytes: scope.Limits().DiscardBytes}
	r := &StreamRowReader{columns: columns, scope: scope, shared: state, active: true}
	r.validations = make([]*streamValidationPlan, columns.Len())
	for i, c := range columnSlice(columns) {
		p, err := compileStreamValidation(c.Type, 0)
		if err != nil {
			return nil, &CodecError{Column: c.Name, Index: i, Stage: CodecStageStreamCompile, Err: err}
		}
		r.validations[i] = p
	}
	return r, nil
}

// NewStreamRowReaderReusingValidation shares only immutable validation plans
// with a preceding page whose columns have the same immutable backing storage.
// Structural equality is insufficient: plans must not keep replaced metadata
// alive after its owner releases its budget. All mutable decode state is fresh.
// Compilation always uses depth zero and the fixed descriptor nesting bound;
// per-envelope byte, depth and count limits remain enforced by the new scope.
func newStreamRowReaderReusingValidation(columns Columns, scope *wire.Stream, prior *StreamRowReader) (*StreamRowReader, error) {
	if prior != nil && len(prior.validations) == columns.Len() && prior.columns.Len() == columns.Len() {
		current, previous := columnSlice(columns), columnSlice(prior.columns)
		if len(current) == 0 || &current[0] == &previous[0] {
			if scope == nil {
				return nil, typeValidation("stream reader", "nil envelope")
			}
			state := &streamDecodeState{discardBytes: scope.Limits().DiscardBytes}
			return &StreamRowReader{columns: columns, scope: scope, shared: state, validations: prior.validations, active: true}, nil
		}
	}
	return newStreamRowReader(columns, scope)
}

// Context returns the context of the streaming read, or context.Background
// for a nil or zero reader.
func (r *StreamRowReader) Context() context.Context {
	if r == nil || r.scope == nil {
		return context.Background()
	}
	return r.scope.Context()
}
func (r *StreamRowReader) check() error {
	if r == nil || r.scope == nil || r.shared == nil {
		return typeValidation("stream row", "uninitialized reader")
	}
	if r.shared.err != nil {
		return r.shared.err
	}
	if !r.active {
		return r.shared.fail(typeValidation("stream row", "inactive reader"))
	}
	return nil
}
func (r *StreamRowReader) column(fn func(*StreamValueReader) error) error {
	if err := r.check(); err != nil {
		return err
	}
	column, ok := r.columns.At(r.index)
	if !ok {
		return r.shared.fail(typeValidation("stream column", "too many columns"))
	}
	i := r.index
	r.index++
	state := CellPresent
	var size int64
	if r.allowMissing && r.scope.Remaining() == 0 {
		state = CellNull
	} else {
		n, err := r.framingUint32()
		if err != nil {
			// Not yet the column's failure: a cell that fails after its
			// length is read is the column's, even when the connection
			// caused it.
			return r.shared.fail(err)
		}
		size = int64(int32(n))
		if size == -1 {
			state, size = CellNull, 0
		} else if size < 0 {
			err := typeValidation("stream length", "invalid result length %d", size)
			return r.shared.fail(&CodecError{Column: column.Name, Index: i, Stage: CodecStageStreamDecode, Err: err})
		}
	}
	err := r.scope.Child(size, func(scope *wire.Stream) error {
		value := r.shared.value(r.depth)
		*value = StreamValueReader{scope: scope, typ: column.Type, state: state, shared: r.shared, validation: r.validations[i], capture: r.capture, depth: r.depth, path: column.Name, active: true}
		defer func() { value.active = false; value.capture = nil }()
		if err := fn(value); err != nil {
			return err
		}
		return value.RequireEnd()
	})
	if err != nil {
		return r.shared.fail(&CodecError{Column: column.Name, Index: i, Stage: CodecStageStreamDecode, Err: err})
	}
	return nil
}

// ReadColumn decodes the next column into dst with plan.
func (r *StreamRowReader) ReadColumn[T any](plan CompiledStreamValue[T], dst *T) error {
	if plan == nil || dst == nil {
		return typeValidation("stream column", "nil plan or destination")
	}
	if r.nullZero {
		r.nullZero = false
		return r.column(func(value *StreamValueReader) error {
			if value.State() == CellNull {
				plan.Reset(dst)
				return nil
			}
			return plan.DecodeInto(value, dst)
		})
	}
	return r.column(func(value *StreamValueReader) error { return plan.DecodeInto(value, dst) })
}

// SkipColumn validates the next column and discards it.
func (r *StreamRowReader) SkipColumn() error {
	return r.column(func(value *StreamValueReader) error { return value.Skip() })
}

// RequireEnd returns an error unless every column of the row was read or
// skipped.
func (r *StreamRowReader) RequireEnd() error {
	if err := r.check(); err != nil {
		return err
	}
	if r.index != r.columns.Len() {
		return r.shared.fail(typeValidation("stream row", "decoded %d of %d columns", r.index, r.columns.Len()))
	}
	return nil
}

func (r *StreamRowReader) framingUint32() (uint32, error) {
	if r.capture == nil {
		return r.scope.Uint32()
	}
	if len(r.capture.dst)-r.capture.offset < 4 {
		return 0, wire.ErrBounds
	}
	b := r.capture.dst[r.capture.offset : r.capture.offset+4]
	if err := r.scope.ReadFull(b); err != nil {
		return 0, err
	}
	r.capture.offset += 4
	return binary.BigEndian.Uint32(b), nil
}

type streamTypePair struct {
	expected, actual *dataTypeDescriptor
}
type streamColumnsPair struct {
	expected, actual       *ColumnSpec
	expectedLen, actualLen int
}
type streamChildPair struct {
	parent, requested *dataTypeDescriptor
}

func (r *StreamValueReader) matches(t DataType) bool {
	if descriptorOf(t) == descriptorOf(r.typ) {
		return true
	}
	key := streamTypePair{descriptorOf(t), descriptorOf(r.typ)}
	if known, ok := r.shared.typeMatches[key]; ok {
		return known
	}
	equal := t.Equal(r.typ)
	if r.shared.typeMatches == nil {
		r.shared.typeMatches = make(map[streamTypePair]bool)
	}
	r.shared.typeMatches[key] = equal
	return equal
}
func (r *StreamRowReader) matches(columns Columns) bool {
	if columns.Len() == 0 || r.columns.Len() == 0 {
		return columns.Len() == r.columns.Len()
	}
	key := streamColumnsPair{&columnSlice(columns)[0], &columnSlice(r.columns)[0], columns.Len(), r.columns.Len()}
	if key.expected == key.actual && key.expectedLen == key.actualLen {
		return true
	}
	if known, ok := r.shared.rowMatches[key]; ok {
		return known
	}
	equal := columns.Equal(r.columns)
	if r.shared.rowMatches == nil {
		r.shared.rowMatches = make(map[streamColumnsPair]bool)
	}
	r.shared.rowMatches[key] = equal
	return equal
}
func (r *StreamValueReader) childValidation(t DataType) (*streamValidationPlan, error) {
	if r.validation == nil {
		return nil, typeValidation("stream child", "missing compiled parent validation")
	}
	if child := r.validation.children[descriptorOf(t)]; child != nil {
		return child, nil
	}
	key := streamChildPair{descriptorOf(r.typ), descriptorOf(t)}
	if child := r.shared.childMatches[key]; child != nil {
		return child, nil
	}
	// Equivalent application descriptors are resolved once per response shape.
	for _, child := range r.validation.children {
		if child.typ.Equal(t) {
			if r.shared.childMatches == nil {
				r.shared.childMatches = make(map[streamChildPair]*streamValidationPlan)
			}
			if len(r.shared.childMatches) >= 64 {
				clear(r.shared.childMatches)
			}
			r.shared.childMatches[key] = child
			return child, nil
		}
	}
	return nil, typeValidation("stream child", "requested type is absent from actual parent metadata")
}

func compileStreamValue[T any](decoder StreamDecoder[T], typ DataType) (CompiledStreamValue[T], error) {
	if decoder == nil {
		return nil, typeValidation("stream codec", "nil value decoder")
	}
	plan, err := decoder.Compile(typ)
	if err == nil && plan == nil {
		err = typeValidation("stream codec", "Compile returned a nil value plan")
	}
	return plan, err
}
func compileStreamRow[R any](decoder StreamRowDecoder[R], columns Columns) (CompiledStreamRow[R], error) {
	if decoder == nil {
		return nil, typeValidation("stream codec", "nil row decoder")
	}
	plan, err := decoder.Compile(columns)
	if err == nil && plan == nil {
		err = typeValidation("stream codec", "Compile returned a nil row plan")
	}
	return plan, err
}

// resetStream resets dst with decoder, which a composite decoder may hold
// nil until its Compile rejects it.
func resetStream[T any](decoder StreamDecoder[T], dst *T) {
	if decoder != nil {
		decoder.Reset(dst)
	}
}
