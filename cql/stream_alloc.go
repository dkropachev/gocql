// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"time"
	"unsafe"
)

// The stream decoders below allocate their result, as the buffered codecs
// do, instead of writing into storage the caller seeded: they never reuse a
// buffer between rows, so a value they decode stays valid after the next
// row. They suit fields an application keeps; the seeded decoders, such as
// StreamTextBytes and StreamMapEntries, suit a scan that copies or aggregates as it
// goes.

// StreamString decodes text and ascii into a newly allocated string,
// validating UTF-8 for text and ASCII for ascii, as StringCodec does.
var StreamString StreamDecoder[string] = streamStringCodec{}

type streamStringCodec struct{}
type streamStringPlan struct{ bytes streamBytesPlan }

func (streamStringCodec) Compile(t DataType) (CompiledStreamValue[string], error) {
	p, err := StreamTextBytes.Compile(t)
	if err != nil {
		return nil, err
	}
	return streamStringPlan{bytes: p.(streamBytesPlan)}, nil
}
func (streamStringCodec) Reset(dst *string) {
	if dst != nil {
		*dst = ""
	}
}
func (streamStringPlan) Reset(dst *string) {
	if dst != nil {
		*dst = ""
	}
}
func (p streamStringPlan) DecodeInto(r *StreamValueReader, dst *string) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil string destination"))
	}
	b := make([]byte, 0, max(r.Remaining(), 0))
	if err := p.bytes.DecodeInto(r, &b); err != nil {
		return err
	}
	// unsafe: b is a fresh array that nothing else refers to.
	*dst = unsafe.String(unsafe.SliceData(b), len(b))
	return nil
}

// StreamTimestampTime decodes timestamp into a UTC time.Time, as
// TimestampTimeCodec does.
var StreamTimestampTime StreamDecoder[time.Time] = streamTimeCodec{}

type streamTimeCodec struct{}
type streamTimePlan struct {
	base CompiledStreamValue[Timestamp]
}

func (streamTimeCodec) Compile(t DataType) (CompiledStreamValue[time.Time], error) {
	p, err := StreamTimestamp.Compile(t)
	if err != nil {
		return nil, err
	}
	return streamTimePlan{base: p}, nil
}
func (streamTimeCodec) Reset(dst *time.Time) {
	if dst != nil {
		*dst = time.Time{}
	}
}
func (streamTimePlan) Reset(dst *time.Time) {
	if dst != nil {
		*dst = time.Time{}
	}
}
func (p streamTimePlan) DecodeInto(r *StreamValueReader, dst *time.Time) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil time destination"))
	}
	var ts Timestamp
	if err := p.base.DecodeInto(r, &ts); err != nil {
		return err
	}
	*dst = ts.Time()
	return nil
}

// StreamTimeDuration decodes duration into time.Duration, as
// TimeDurationCodec does, rejecting a duration with months or days.
var StreamTimeDuration StreamDecoder[time.Duration] = streamTimeDurationCodec{}

type streamTimeDurationCodec struct{}
type streamTimeDurationPlan struct{ base CompiledStreamValue[Duration] }

func (streamTimeDurationCodec) Compile(t DataType) (CompiledStreamValue[time.Duration], error) {
	p, err := StreamDuration.Compile(t)
	if err != nil {
		return nil, err
	}
	return streamTimeDurationPlan{base: p}, nil
}
func (streamTimeDurationCodec) Reset(dst *time.Duration) {
	if dst != nil {
		*dst = 0
	}
}
func (streamTimeDurationPlan) Reset(dst *time.Duration) {
	if dst != nil {
		*dst = 0
	}
}
func (p streamTimeDurationPlan) DecodeInto(r *StreamValueReader, dst *time.Duration) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil duration destination"))
	}
	var d Duration
	if err := p.base.DecodeInto(r, &d); err != nil {
		return err
	}
	if d.Months != 0 || d.Days != 0 {
		return r.fail(fmt.Errorf("gocql: duration %s has months or days, which time.Duration cannot hold; decode it into cql.Duration", d))
	}
	*dst = time.Duration(d.Nanoseconds)
	return nil
}

// StreamDecimal decodes decimal into a Decimal whose unscaled integer
// is newly allocated, as DecimalCodec decodes one.
var StreamDecimal StreamDecoder[Decimal] = streamDecimalValueCodec{}

type streamDecimalValueCodec struct{}
type streamDecimalValuePlan struct{ parts streamDecimalPlan }

func (streamDecimalValueCodec) Compile(t DataType) (CompiledStreamValue[Decimal], error) {
	p, err := StreamDecimalParts.Compile(t)
	if err != nil {
		return nil, err
	}
	return streamDecimalValuePlan{parts: p.(streamDecimalPlan)}, nil
}
func (streamDecimalValueCodec) Reset(dst *Decimal) {
	if dst != nil {
		*dst = Decimal{}
	}
}
func (streamDecimalValuePlan) Reset(dst *Decimal) {
	if dst != nil {
		*dst = Decimal{}
	}
}
func (p streamDecimalValuePlan) DecodeInto(r *StreamValueReader, dst *Decimal) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil decimal destination"))
	}
	parts := DecimalParts{Unscaled: make([]byte, 0, max(r.Remaining()-4, 0))}
	if err := p.parts.DecodeInto(r, &parts); err != nil {
		return err
	}
	unscaled, err := VarintFromBytes(parts.Unscaled)
	if err != nil {
		return r.fail(err)
	}
	*dst = Decimal{Unscaled: unscaled, Scale: parts.Scale}
	return nil
}

// StreamPointer decodes a value with base into a newly allocated T, and
// NULL as nil, as PointerCodec does. base decodes into a fresh, reset T, so
// it must not need seeded capacity: use it with a scalar or an allocating
// decoder, such as StreamString.
func StreamPointer[T any](base StreamDecoder[T]) StreamDecoder[*T] {
	return streamPointerCodec[T]{base: base}
}

type streamPointerCodec[T any] struct{ base StreamDecoder[T] }
type streamPointerPlan[T any] struct {
	base CompiledStreamValue[T]
	typ  DataType
}

func (c streamPointerCodec[T]) Compile(t DataType) (CompiledStreamValue[*T], error) {
	if c.base == nil {
		return nil, typeValidation("stream codec", "nil pointer child")
	}
	p, err := compileStreamValue(c.base, t)
	if err != nil {
		return nil, err
	}
	return streamPointerPlan[T]{base: p, typ: t}, nil
}
func (streamPointerCodec[T]) Reset(dst **T) {
	if dst != nil {
		*dst = nil
	}
}
func (streamPointerPlan[T]) Reset(dst **T) {
	if dst != nil {
		*dst = nil
	}
}
func (p streamPointerPlan[T]) DecodeInto(r *StreamValueReader, dst **T) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil pointer destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled pointer metadata changed"))
	}
	*dst = nil
	if r.state == CellNull {
		return r.RequireEnd()
	}
	v := new(T)
	p.base.Reset(v)
	if err := p.base.DecodeInto(r, v); err != nil {
		return err
	}
	*dst = v
	return nil
}

// StreamMap decodes a map into a Go map, as MapCodec does: a newly
// allocated one, or the destination's, cleared, when it has one. NULL, how
// CQL stores an empty map, decodes as an empty map, and a response that
// repeats a key fails. key and value decode into fresh, reset values, so
// they must not need seeded capacity: use scalar or allocating decoders,
// such as StreamString.
func StreamMap[K comparable, V any](key StreamDecoder[K], value StreamDecoder[V]) StreamDecoder[map[K]V] {
	return streamGoMapCodec[K, V]{key: key, value: value}
}

type streamGoMapCodec[K comparable, V any] struct {
	key   StreamDecoder[K]
	value StreamDecoder[V]
}
type streamGoMapPlan[K comparable, V any] struct {
	entries streamMapPlan[K, V]
}

func (c streamGoMapCodec[K, V]) Compile(t DataType) (CompiledStreamValue[map[K]V], error) {
	p, err := streamMapCodec[K, V](c).Compile(t)
	if err != nil {
		return nil, err
	}
	return streamGoMapPlan[K, V]{entries: p.(streamMapPlan[K, V])}, nil
}
func (streamGoMapCodec[K, V]) Reset(dst *map[K]V) {
	if dst != nil {
		clear(*dst)
	}
}
func (streamGoMapPlan[K, V]) Reset(dst *map[K]V) {
	if dst != nil {
		clear(*dst)
	}
}
func (p streamGoMapPlan[K, V]) DecodeInto(r *StreamValueReader, dst *map[K]V) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil map destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.entries.typ) {
		return r.fail(typeValidation("type", "compiled map metadata changed"))
	}
	clear(*dst)
	if r.state == CellNull {
		return r.RequireEnd()
	}
	if err := r.present(); err != nil {
		return err
	}
	count, err := r.count(8)
	if err != nil {
		return err
	}
	m := *dst
	if m == nil {
		m = make(map[K]V, count)
	}
	for i := range count {
		var k K
		var v V
		p.entries.key.Reset(&k)
		p.entries.value.Reset(&v)
		if err := r.child(p.entries.keyType, p.entries.keyValidation, false, func(child *StreamValueReader) error { return p.entries.key.DecodeInto(child, &k) }); err != nil {
			return fmt.Errorf("gocql: streaming map key %d: %w", i, err)
		}
		if err := r.child(p.entries.valueType, p.entries.valueValidation, false, func(child *StreamValueReader) error { return p.entries.value.DecodeInto(child, &v) }); err != nil {
			return fmt.Errorf("gocql: streaming map value %d: %w", i, err)
		}
		m[k] = v
		if len(m) != i+1 {
			return r.fail(typeValidation("map", "key %d repeats an earlier key", i))
		}
	}
	*dst = m
	return r.RequireEnd()
}
