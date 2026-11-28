// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"iter"
	"math"
	"math/bits"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Entry is one key and value of a map, for decoding a map into a slice that
// preserves the order and duplicates of its encoded representation. Its zero
// value holds the zero K and V.
type Entry[K, V any] struct {
	// Key is the entry's key.
	Key K
	// Value is the entry's value.
	Value V
}

// valueIterator walks the members of a collection Value. Next advances and
// reports whether a member is available, Value returns it, and Err reports
// the failure that stopped iteration. All does the same as a range loop.
//
// The zero valueIterator is exhausted: Next reports false, and Err nil.
type valueIterator[T any] struct {
	value        T
	err          error
	next         func(int) (T, error)
	nextCursor   func(*ValueCursor, int) (T, error)
	cursor       ValueCursor
	count, index int
	done         bool
}

func newValueIterator[T any](count int, next func(int) (T, error)) *valueIterator[T] {
	return &valueIterator[T]{count: count, next: next}
}

// Next advances to the next member and reports whether there is one. It
// returns false at the end, after a failure, and for a nil iterator.
func (i *valueIterator[T]) Next() bool {
	if i == nil || i.done {
		return false
	}
	var zero T
	i.value = zero
	if i.nextCursor != nil && i.err == nil {
		i.err = i.cursor.check()
	}
	if i.err != nil || i.index == i.count {
		i.done = true
		return false
	}
	if i.count < 0 || (i.next == nil && i.nextCursor == nil) {
		i.err = typeValidation("iterator", "uninitialized iterator")
		i.done = true
		return false
	}
	var value T
	var err error
	if i.nextCursor != nil {
		value, err = i.nextCursor(&i.cursor, i.index)
	} else {
		value, err = i.next(i.index)
	}
	if err != nil {
		i.err, i.done = err, true
		return false
	}
	i.index++
	i.value = value
	return true
}

// Value returns the member Next advanced to, or the zero T before the first
// call and after the end. The member may borrow the collection's storage.
func (i *valueIterator[T]) Value() T {
	if i == nil {
		var zero T
		return zero
	}
	return i.value
}

// Err returns the failure that stopped iteration, or nil. It returns a
// ValidationError for a nil iterator.
func (i *valueIterator[T]) Err() error {
	if i == nil {
		return typeValidation("iterator", "nil iterator")
	}
	return i.err
}

// All returns an iterator over the remaining members, for use with range.
// It yields each member with a nil error; a failure ends the loop with one
// last pair holding the zero T and the error, which Err then also returns.
// A nil iterator yields only Err's ValidationError. The members may borrow
// the collection's storage, as Value's do.
func (i *valueIterator[T]) All() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for i.Next() {
			if !yield(i.Value(), nil) {
				return
			}
		}
		if err := i.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}

type sequenceEncoder[T any] struct {
	element Encoder[T]
	typ     DataType
	kind    Kind
	width   int64
}

// ListEncoder encodes a slice as a CQL list using element.
func ListEncoder[T any](element Encoder[T]) Encoder[[]T] {
	return sequenceEncoder[T]{kind: KindList, element: element}
}

// SetEncoder encodes a slice as a CQL set using element.
func SetEncoder[T any](element Encoder[T]) Encoder[[]T] {
	return sequenceEncoder[T]{kind: KindSet, element: element}
}

// VectorEncoder encodes a vector of the declared dimension using element.
func VectorEncoder[T any](element Encoder[T]) Encoder[[]T] {
	return sequenceEncoder[T]{kind: KindVector, element: element}
}

// anySequence is the kind of a sequence codec that takes the kind of the
// type it compiles for: a list, a set or a vector.
const anySequence = KindCustom

// resolveSequenceKind returns the kind a sequence codec of kind uses for typ.
func resolveSequenceKind(typ DataType, kind Kind) Kind {
	if kind != anySequence {
		return kind
	}
	switch k := typ.Kind(); k {
	case KindList, KindSet, KindVector:
		return k
	}
	return anySequence
}

func sequenceElement(typ DataType, kind Kind) (DataType, int64, error) {
	if err := validateDataType(typ); err != nil {
		return DataType{}, 0, err
	}
	element, ok := typ.Element()
	if kind == anySequence {
		if !ok || resolveSequenceKind(typ, kind) == anySequence {
			return DataType{}, 0, typeValidation("type", "expected list, set or vector, got %s", typ)
		}
		kind = typ.Kind()
	}
	if !ok || typ.Kind() != kind {
		return DataType{}, 0, typeValidation("type", "expected %s, got %s", kind, typ)
	}
	if kind != KindVector {
		return element, 0, nil
	}
	dimension, vector := typ.Dimension()
	if !vector || dimension == 0 {
		return DataType{}, 0, typeValidation("dimension", "vector dimension must be positive")
	}
	width, err := vectorFixedWidth(element)
	if err == nil && width > math.MaxInt32/int64(dimension) {
		err = typeValidation("dimension", "vector body exceeds signed 32-bit length")
	}
	return element, width, err
}
func (c sequenceEncoder[T]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueEncoder(typ)
	return err
}
func (c sequenceEncoder[T]) compileValueEncoder(typ DataType) (Encoder[[]T], error) {
	element, width, err := sequenceElement(typ, c.kind)
	if err != nil {
		return nil, err
	}
	encoder, err := compileValueEncoder(c.element, element)
	if err != nil {
		return nil, fmt.Errorf("gocql: element type: %w", err)
	}
	c.typ, c.element, c.width, c.kind = typ, encoder, width, typ.Kind()
	return c, nil
}
func (c sequenceEncoder[T]) Encode(w *ValueWriter, values []T) error {
	if descriptorOf(c.typ) == nil {
		bound, err := c.compileValueEncoder(w.Type())
		if err != nil {
			return err
		}
		return bound.Encode(w, values)
	}
	if !c.typ.Equal(w.Type()) {
		return typeValidation("type", "sequence metadata changed after compilation")
	}
	element, _ := c.typ.Element()
	if c.kind == KindVector {
		dimension, _ := c.typ.Dimension()
		if len(values) != int(dimension) {
			return typeValidation("dimension", "vector requires %d elements, got %d", dimension, len(values))
		}
		for i, value := range values {
			if err := writeVectorElement(w, element, c.width, c.element, value); err != nil {
				return fmt.Errorf("gocql: vector element %d: %w", i, err)
			}
		}
		return nil
	}
	if uint64(len(values)) > math.MaxInt32 {
		return typeValidation("count", "collection has too many elements")
	}
	if err := w.Uint32(uint32(len(values))); err != nil {
		return err
	}
	for i, value := range values {
		if err := writeCollectionElement(w, element, c.element, value); err != nil {
			return fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	return nil
}
func writeCollectionElement[T any](w *ValueWriter, typ DataType, encoder Encoder[T], value T) error {
	return w.Child(typ, func(child *ValueWriter) error {
		if err := encoder.Encode(child, value); err != nil {
			return err
		}
		if child.state != CellPresent && child.state != CellEmpty {
			return typeValidation("element", "collection elements cannot be null or unset")
		}
		return nil
	})
}

type sequenceDecoder[T any] struct {
	element     Decoder[T]
	elementInto valueIntoDecoder[T] // resolved by compileValueDecoder
	typ         DataType
	kind        Kind
	width       int64
}

// ListDecoder eagerly decodes every element into a []T. Elements may borrow
// page storage; Clone recursively detaches them.
func ListDecoder[T any](element Decoder[T]) Decoder[[]T] {
	return sequenceDecoder[T]{kind: KindList, element: element}
}

// SetDecoder eagerly decodes every element into a []T. Elements may borrow
// page storage; Clone recursively detaches them.
func SetDecoder[T any](element Decoder[T]) Decoder[[]T] {
	return sequenceDecoder[T]{kind: KindSet, element: element}
}

// VectorDecoder eagerly decodes every element into a []T. Elements may borrow
// page storage; Clone recursively detaches them.
func VectorDecoder[T any](element Decoder[T]) Decoder[[]T] {
	return sequenceDecoder[T]{kind: KindVector, element: element}
}
func (c sequenceDecoder[T]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueDecoder(typ)
	return err
}
func (c sequenceDecoder[T]) compileValueDecoder(typ DataType) (Decoder[[]T], error) {
	element, width, err := sequenceElement(typ, c.kind)
	if err != nil {
		return nil, err
	}
	decoder, err := compileValueDecoder(c.element, element)
	if err != nil {
		return nil, fmt.Errorf("gocql: element type: %w", err)
	}
	c.typ, c.element, c.width, c.kind = typ, decoder, width, typ.Kind()
	c.elementInto, _ = decoder.(valueIntoDecoder[T])
	return c, nil
}

type mapEncoder[K, V any] struct {
	key   Encoder[K]
	value Encoder[V]
	typ   DataType
}

// MapEntriesEncoder encodes ordered entries as a CQL map; keys need not be Go-comparable.
func MapEntriesEncoder[K, V any](key Encoder[K], value Encoder[V]) Encoder[[]Entry[K, V]] {
	return mapEncoder[K, V]{key: key, value: value}
}
func (c mapEncoder[K, V]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueEncoder(typ)
	return err
}
func (c mapEncoder[K, V]) compileValueEncoder(typ DataType) (Encoder[[]Entry[K, V]], error) {
	kt, vt, ok := typ.KeyValue()
	if !ok {
		return nil, typeValidation("type", "expected map, got %s", typ)
	}
	key, err := compileValueEncoder(c.key, kt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map key type: %w", err)
	}
	value, err := compileValueEncoder(c.value, vt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map value type: %w", err)
	}
	c.typ, c.key, c.value = typ, key, value
	return c, nil
}
func (c mapEncoder[K, V]) Encode(w *ValueWriter, entries []Entry[K, V]) error {
	if descriptorOf(c.typ) == nil {
		bound, err := c.compileValueEncoder(w.Type())
		if err != nil {
			return err
		}
		return bound.Encode(w, entries)
	}
	if !c.typ.Equal(w.Type()) {
		return typeValidation("type", "map metadata changed after compilation")
	}
	if uint64(len(entries)) > math.MaxInt32 {
		return typeValidation("count", "map has too many entries")
	}
	if err := w.Uint32(uint32(len(entries))); err != nil {
		return err
	}
	kt, vt, _ := c.typ.KeyValue()
	for i, entry := range entries {
		if err := writeCollectionElement(w, kt, c.key, entry.Key); err != nil {
			return fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		if err := writeCollectionElement(w, vt, c.value, entry.Value); err != nil {
			return fmt.Errorf("gocql: map value %d: %w", i, err)
		}
	}
	return nil
}

type mapDecoder[K, V any] struct {
	key       Decoder[K]
	value     Decoder[V]
	keyInto   valueIntoDecoder[K] // resolved by compileValueDecoder
	valueInto valueIntoDecoder[V]
	typ       DataType
}

// MapEntriesDecoder eagerly decodes ordered entries; keys need not be Go-comparable.
// Entries may contain borrowed values; Clone recursively detaches them.
func MapEntriesDecoder[K, V any](key Decoder[K], value Decoder[V]) Decoder[[]Entry[K, V]] {
	return mapDecoder[K, V]{key: key, value: value}
}
func (c mapDecoder[K, V]) CheckType(typ DataType) error {
	perfCheckType()

	_, err := c.compileValueDecoder(typ)
	return err
}
func (c mapDecoder[K, V]) compileValueDecoder(typ DataType) (Decoder[[]Entry[K, V]], error) {
	kt, vt, ok := typ.KeyValue()
	if !ok {
		return nil, typeValidation("type", "expected map, got %s", typ)
	}
	key, err := compileValueDecoder(c.key, kt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map key type: %w", err)
	}
	value, err := compileValueDecoder(c.value, vt)
	if err != nil {
		return nil, fmt.Errorf("gocql: map value type: %w", err)
	}
	c.typ, c.key, c.value = typ, key, value
	c.keyInto, _ = key.(valueIntoDecoder[K])
	c.valueInto, _ = value.(valueIntoDecoder[V])
	return c, nil
}

// validatedChild reads the next element of a collection whose layout
// validateSequenceLayout or validateMapBody already checked: every element
// is framed within the body and present, and typ is the compiled element
// type, so neither read can fail. It still refuses a released lease, as
// ValueCursor.Child does.
func validatedChild(cursor *ValueCursor, typ DataType) (ValueReader, error) {
	if !cursor.lease.IsZero() && !cursor.lease.Alive() {
		return ValueReader{}, ErrReleased
	}
	prefix, _ := cursor.cursor.Uint32()
	body, _ := cursor.cursor.TakeNested(int64(int32(prefix)))
	return ValueReader{typ: typ, state: CellPresent, body: body, lease: cursor.lease}, nil
}

// Collection framing is checked before allocating and decoding elements.
func collectionCount(cursor *ValueCursor, minimum int64) (int, error) {
	n, err := cursor.Uint32()
	if err != nil {
		return 0, err
	}
	if n > math.MaxInt32 || int64(n) > cursor.Remaining()/minimum {
		return 0, typeValidation("count", "collection count %d exceeds available framing", n)
	}
	return int(n), nil
}
func checkedCollectionChild(cursor *ValueCursor, typ DataType) error {
	child, err := cursor.Child(typ)
	if err != nil {
		return err
	}
	if child.State() != CellPresent {
		return typeValidation("element", "collection elements cannot be null or unset")
	}
	return nil
}
func validateSequenceBody(r ValueReader, kind Kind) (Bytes, int, int64, error) {
	element, width, err := sequenceElement(r.Type(), kind)
	if err != nil {
		return Bytes{}, 0, 0, err
	}
	return validateSequenceLayout(r, r.Type().Kind(), element, width)
}
func validateSequenceLayout(r ValueReader, kind Kind, element DataType, width int64) (Bytes, int, int64, error) {
	if err := requirePresent(r); err != nil {
		return Bytes{}, 0, 0, err
	}
	cursor := r.cursorValue()
	if kind == KindVector {
		dimension, _ := r.Type().Dimension()
		count := int(dimension)
		if width > 0 {
			if r.body.Len() != int64(count)*width {
				return Bytes{}, 0, 0, typeValidation("dimension", "vector requires %d bytes, got %d", int64(count)*width, r.body.Len())
			}
		} else {
			if int64(count) > cursor.Remaining() {
				return Bytes{}, 0, 0, typeValidation("dimension", "vector dimension exceeds available lengths")
			}
			for i := 0; i < count; i++ {
				if _, err := readVectorElement(&cursor, element, 0); err != nil {
					return Bytes{}, 0, 0, fmt.Errorf("gocql: vector element %d: %w", i, err)
				}
			}
			if err := cursor.RequireEnd(); err != nil {
				return Bytes{}, 0, 0, err
			}
		}
		return r.Bytes(), count, width, nil
	}
	count, err := collectionCount(&cursor, 4)
	if err != nil {
		return Bytes{}, 0, 0, err
	}
	body, err := r.Bytes().Slice(4, r.body.Len()-4)
	if err != nil {
		return Bytes{}, 0, 0, err
	}
	for i := 0; i < count; i++ {
		if err := checkedCollectionChild(&cursor, element); err != nil {
			return Bytes{}, 0, 0, fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	if err := cursor.RequireEnd(); err != nil {
		return Bytes{}, 0, 0, err
	}
	return body, count, 0, nil
}
func validateMapBody(r ValueReader) (Bytes, int, error) {
	if err := requirePresent(r); err != nil {
		return Bytes{}, 0, err
	}
	key, value, ok := r.Type().KeyValue()
	if !ok {
		return Bytes{}, 0, typeValidation("type", "expected map, got %s", r.Type())
	}
	cursor := r.cursorValue()
	count, err := collectionCount(&cursor, 8)
	if err != nil {
		return Bytes{}, 0, err
	}
	body, err := r.Bytes().Slice(4, r.body.Len()-4)
	if err != nil {
		return Bytes{}, 0, err
	}
	for i := 0; i < count; i++ {
		if err := checkedCollectionChild(&cursor, key); err != nil {
			return Bytes{}, 0, fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		if err := checkedCollectionChild(&cursor, value); err != nil {
			return Bytes{}, 0, fmt.Errorf("gocql: map value %d: %w", i, err)
		}
	}
	if err := cursor.RequireEnd(); err != nil {
		return Bytes{}, 0, err
	}
	return body, count, nil
}

// vectorFixedWidth follows the server's serializer classification. In
// particular smallint, tinyint, date, time, and counter are variable-width here.
func vectorFixedWidth(typ DataType) (int64, error) {
	if err := validateDataType(typ); err != nil {
		return 0, err
	}
	switch typ.Kind() {
	case KindBoolean:
		return 1, nil
	case KindInt, KindFloat:
		return 4, nil
	case KindBigInt, KindDouble, KindTimestamp:
		return 8, nil
	case KindUUID, KindTimeUUID:
		return 16, nil
	case KindVector:
		dimension, ok := typ.Dimension()
		if !ok || dimension == 0 {
			return 0, typeValidation("dimension", "vector dimension must be positive")
		}
		element, _ := typ.Element()
		width, err := vectorFixedWidth(element)
		if err != nil {
			return 0, err
		}
		if width > math.MaxInt32/int64(dimension) {
			return 0, typeValidation("dimension", "nested vector width exceeds signed 32-bit length")
		}
		return width * int64(dimension), nil
	default:
		return 0, nil
	}
}
func readVectorElement(cursor *ValueCursor, typ DataType, width int64) (ValueReader, error) {
	if width == 0 {
		first, err := cursor.Uint8()
		if err != nil {
			return ValueReader{}, err
		}
		extra := bits.LeadingZeros8(^first)
		length := uint64(first & byte(0xff>>uint(extra)))
		for j := 0; j < extra; j++ {
			b, err := cursor.Uint8()
			if err != nil {
				return ValueReader{}, err
			}
			length = length<<8 | uint64(b)
		}
		if extra > 0 && length < uint64(1)<<uint(extra*7) {
			return ValueReader{}, cursor.failure(typeValidation("length", "noncanonical vector length"))
		}
		if length > math.MaxInt32 {
			return ValueReader{}, cursor.failure(typeValidation("length", "vector element length exceeds signed 32-bit range"))
		}
		width = int64(length)
	}
	body, err := cursor.Take(width)
	if err != nil {
		return ValueReader{}, err
	}
	return newValueReader(typ, CellPresent, bytesChain(body), bytesLease(body)), nil
}

// Fixed vector members write directly into the enclosing builder. Variable
// members use an independent descriptor builder because their unsigned vint
// length must precede the body and cannot be patched as a four-byte cell length.
func writeVectorElement[T any](w *ValueWriter, typ DataType, width int64, encoder Encoder[T], value T) error {
	if err := w.payload(); err != nil {
		return err
	}
	if width > 0 {
		start := w.builder.Len()
		child := &ValueWriter{builder: w.builder, typ: typ, mark: w.builder.Mark(), start: start - 4, ownBorrowed: w.ownBorrowed}
		if err := encoder.Encode(child, value); err != nil {
			abortValueWriter(child)
			return w.fail(err)
		}
		if child.err != nil {
			abortValueWriter(child)
			return w.fail(child.err)
		}
		if child.state != CellPresent || w.builder.Len()-start != width {
			abortValueWriter(child)
			return w.fail(typeValidation("element", "fixed vector element requires %d present bytes", width))
		}
		child.finished = true
		return nil
	}
	builder := wire.NewBuilder(64)
	child := newValueWriter(builder, typ, false)
	child.ownBorrowed = w.ownBorrowed
	if err := encoder.Encode(child, value); err != nil {
		abortValueWriter(child)
		return w.fail(err)
	}
	if child.state != CellPresent && child.state != CellEmpty {
		abortValueWriter(child)
		return w.fail(typeValidation("element", "vector elements cannot be null or unset"))
	}
	if err := finishValueWriter(child); err != nil {
		return w.fail(err)
	}
	chain, err := builder.Freeze()
	if err != nil {
		return w.fail(err)
	}
	body, err := chain.Slice(4, chain.Len()-4)
	if err != nil {
		return w.fail(err)
	}
	if err := scalarWriteUnsignedVInt(w, uint64(body.Len())); err != nil {
		return err
	}
	return w.fail(body.Each(w.builder.Borrow))
}
