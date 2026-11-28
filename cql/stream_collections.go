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
	"fmt"
)

type streamSequenceCodec[T any] struct {
	element StreamDecoder[T]
	kind    Kind
}
type streamSequencePlan[T any] struct {
	typ, element DataType
	plan         CompiledStreamValue[T]
	validation   *streamValidationPlan
	width        int64
}

// StreamList decodes a list into seeded slice slots using child. Seed each nested buffer too.
// NULL decodes as an empty slice while preserving capacity; StreamNullable preserves NULL.
func StreamList[T any](child StreamDecoder[T]) StreamDecoder[[]T] {
	return streamSequenceCodec[T]{kind: KindList, element: child}
}

// StreamSet decodes a set into seeded slice slots using child, preserving server order.
// NULL decodes as an empty slice while preserving capacity; StreamNullable preserves NULL.
func StreamSet[T any](child StreamDecoder[T]) StreamDecoder[[]T] {
	return streamSequenceCodec[T]{kind: KindSet, element: child}
}

// StreamSlice decodes a list, a set or a vector, whichever the column is
// when the decoder is compiled, as StreamList, StreamSet or StreamVector
// does; StreamRowOf uses it for slice fields.
func StreamSlice[T any](child StreamDecoder[T]) StreamDecoder[[]T] {
	return streamSequenceCodec[T]{kind: anySequence, element: child}
}

// StreamVector decodes a vector into seeded slice storage using child.
func StreamVector[T any](child StreamDecoder[T]) StreamDecoder[[]T] {
	return streamSequenceCodec[T]{kind: KindVector, element: child}
}
func (c streamSequenceCodec[T]) Compile(t DataType) (CompiledStreamValue[[]T], error) {
	if c.element == nil {
		return nil, typeValidation("stream codec", "nil sequence child")
	}
	e, width, err := sequenceElement(t, c.kind)
	if err != nil {
		return nil, err
	}
	plan, err := compileStreamValue(c.element, e)
	if err != nil {
		return nil, err
	}
	validation, err := compileStreamValidation(e, 0)
	if err != nil {
		return nil, err
	}
	return streamSequencePlan[T]{typ: t, element: e, plan: plan, validation: validation, width: width}, nil
}
func (p streamSequencePlan[T]) Reset(dst *[]T) {
	if dst != nil {
		*dst = (*dst)[:0]
	}
}
func (p streamSequencePlan[T]) DecodeInto(r *StreamValueReader, dst *[]T) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil sequence destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled sequence metadata changed"))
	}
	if r.state == CellNull && p.typ.Kind() != KindVector {
		p.Reset(dst)
		return r.RequireEnd()
	}
	if err := r.present(); err != nil {
		return err
	}
	var count int
	if p.typ.Kind() == KindVector {
		count = int(descriptorOf(p.typ).Dimension)
		if p.width > 0 && r.Remaining() != int64(count)*p.width {
			return r.fail(typeValidation("dimension", "incorrect fixed vector body width"))
		}
		if p.width == 0 && r.Remaining() < int64(count) {
			return r.fail(typeValidation("dimension", "vector dimension exceeds available lengths"))
		}
	} else {
		var err error
		count, err = r.count(4)
		if err != nil {
			return err
		}
	}
	if err := r.capacity(int64(count), cap(*dst)); err != nil {
		return err
	}
	*dst = (*dst)[:count]
	for i := range *dst {
		value := &(*dst)[i]
		p.plan.Reset(value)
		decode := func(child *StreamValueReader) error { return p.plan.DecodeInto(child, value) }
		var err error
		if p.typ.Kind() == KindVector {
			size := p.width
			if size == 0 {
				size, err = r.vectorLength()
			}
			if err == nil {
				err = r.childBody(p.element, CellPresent, size, p.validation, decode)
			}
		} else {
			err = r.child(p.element, p.validation, false, decode)
		}
		if err != nil {
			return fmt.Errorf("gocql: streaming element %d: %w", i, err)
		}
	}
	return r.RequireEnd()
}

type streamMapCodec[K, V any] struct {
	key   StreamDecoder[K]
	value StreamDecoder[V]
}
type streamMapPlan[K, V any] struct {
	typ, keyType, valueType        DataType
	key                            CompiledStreamValue[K]
	value                          CompiledStreamValue[V]
	keyValidation, valueValidation *streamValidationPlan
}

// StreamMapEntries decodes a map into seeded entry slots using key and value decoders.
// NULL decodes as an empty slice while preserving capacity; StreamNullable preserves NULL.
func StreamMapEntries[K, V any](key StreamDecoder[K], value StreamDecoder[V]) StreamDecoder[[]Entry[K, V]] {
	return streamMapCodec[K, V]{key, value}
}
func (c streamMapCodec[K, V]) Compile(t DataType) (CompiledStreamValue[[]Entry[K, V]], error) {
	kt, vt, ok := t.KeyValue()
	if !ok {
		return nil, typeValidation("type", "expected map, got %s", t)
	}
	if c.key == nil || c.value == nil {
		return nil, typeValidation("stream codec", "nil map child")
	}
	key, e := compileStreamValue(c.key, kt)
	if e != nil {
		return nil, e
	}
	value, e := compileStreamValue(c.value, vt)
	if e != nil {
		return nil, e
	}
	kp, e := compileStreamValidation(kt, 0)
	if e != nil {
		return nil, e
	}
	vp, e := compileStreamValidation(vt, 0)
	if e != nil {
		return nil, e
	}
	return streamMapPlan[K, V]{typ: t, keyType: kt, valueType: vt, key: key, value: value, keyValidation: kp, valueValidation: vp}, nil
}
func (p streamMapPlan[K, V]) Reset(dst *[]Entry[K, V]) {
	if dst != nil {
		*dst = (*dst)[:0]
	}
}
func (p streamMapPlan[K, V]) DecodeInto(r *StreamValueReader, dst *[]Entry[K, V]) error {
	if dst == nil {
		return r.fail(typeValidation("destination", "nil map destination"))
	}
	if err := r.check(); err != nil {
		return err
	}
	if !r.matches(p.typ) {
		return r.fail(typeValidation("type", "compiled map metadata changed"))
	}
	if r.state == CellNull {
		p.Reset(dst)
		return r.RequireEnd()
	}
	if err := r.present(); err != nil {
		return err
	}
	count, err := r.count(8)
	if err != nil {
		return err
	}
	if err := r.capacity(int64(count), cap(*dst)); err != nil {
		return err
	}
	*dst = (*dst)[:count]
	for i := range *dst {
		entry := &(*dst)[i]
		p.key.Reset(&entry.Key)
		p.value.Reset(&entry.Value)
		if err := r.child(p.keyType, p.keyValidation, false, func(child *StreamValueReader) error { return p.key.DecodeInto(child, &entry.Key) }); err != nil {
			return fmt.Errorf("gocql: streaming map key %d: %w", i, err)
		}
		if err := r.child(p.valueType, p.valueValidation, false, func(child *StreamValueReader) error { return p.value.DecodeInto(child, &entry.Value) }); err != nil {
			return fmt.Errorf("gocql: streaming map value %d: %w", i, err)
		}
	}
	return r.RequireEnd()
}

func (c streamSequenceCodec[T]) Reset(dst *[]T) {
	if dst != nil {
		for i := range *dst {
			resetStream(c.element, &(*dst)[i])
		}
		*dst = (*dst)[:0]
	}
}
func (c streamMapCodec[K, V]) Reset(dst *[]Entry[K, V]) {
	if dst != nil {
		for i := range *dst {
			resetStream(c.key, &(*dst)[i].Key)
			resetStream(c.value, &(*dst)[i].Value)
		}
		*dst = (*dst)[:0]
	}
}
