// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// Collection codecs and decoder-only constructors use the same eager decoders.
// They allocate result slices unless DecodeInto finds enough capacity. Elements
// may borrow page storage; Clone recursively detaches them.

type sliceCodec[T any] struct {
	enc sequenceEncoder[T]
	dec sequenceDecoder[T]
}

// ListCodec encodes and decodes a list as a []T.
func ListCodec[T any](element Codec[T]) Codec[[]T] {
	return sliceCodec[T]{
		enc: sequenceEncoder[T]{kind: cqltype.TypeList, element: element},
		dec: sequenceDecoder[T]{kind: cqltype.TypeList, element: element},
	}
}

// SetCodec encodes and decodes a set as a []T, in the order the server sends.
func SetCodec[T any](element Codec[T]) Codec[[]T] {
	return sliceCodec[T]{
		enc: sequenceEncoder[T]{kind: cqltype.TypeSet, element: element},
		dec: sequenceDecoder[T]{kind: cqltype.TypeSet, element: element},
	}
}

// VectorCodec encodes and decodes a vector as a []T of its declared dimension.
func VectorCodec[T any](element Codec[T]) Codec[[]T] {
	return sliceCodec[T]{
		enc: sequenceEncoder[T]{kind: cqltype.TypeCustom, element: element},
		dec: sequenceDecoder[T]{kind: cqltype.TypeCustom, element: element},
	}
}

func (c sliceCodec[T]) CheckType(typ cqltype.DataType) error {
	perfCheckType()
	if _, err := c.enc.compileValueEncoder(typ); err != nil {
		return err
	}
	_, err := c.dec.compileValueDecoder(typ)
	return err
}

func (c sliceCodec[T]) compileValueEncoder(typ cqltype.DataType) (Encoder[[]T], error) {
	enc, err := c.enc.compileValueEncoder(typ)
	if err != nil {
		return nil, err
	}
	c.enc = enc.(sequenceEncoder[T])
	return c, nil
}

func (c sliceCodec[T]) compileValueDecoder(typ cqltype.DataType) (Decoder[[]T], error) {
	dec, err := c.dec.compileValueDecoder(typ)
	if err != nil {
		return nil, err
	}
	c.dec = dec.(sequenceDecoder[T])
	return c, nil
}

func (c sliceCodec[T]) Encode(w *ValueWriter, values []T) error { return c.enc.Encode(w, values) }

func (c sliceCodec[T]) Decode(r ValueReader) ([]T, error)        { return c.dec.Decode(r) }
func (c sliceCodec[T]) DecodeInto(r ValueReader, dst *[]T) error { return c.dec.DecodeInto(r, dst) }
func (c sliceCodec[T]) Clone(v []T) []T                          { return c.dec.Clone(v) }

func (c sequenceDecoder[T]) Decode(r ValueReader) ([]T, error) {
	var values []T
	err := c.DecodeInto(r, &values)
	if err != nil {
		return nil, err
	}
	return values, nil
}

// DecodeInto decodes into dst's storage when it has the capacity, and into
// its elements' storage when the element codec decodes in place.
func (c sequenceDecoder[T]) DecodeInto(r ValueReader, dst *[]T) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil slice destination")
	}
	dec := c
	if cqltype.DescriptorOf(dec.typ) == nil {
		compiled, err := dec.compileValueDecoder(r.Type())
		if err != nil {
			return err
		}
		dec = compiled.(sequenceDecoder[T])
	} else if !dec.typ.Equal(r.Type()) {
		return cqltype.TypeValidation("type", "sequence metadata changed after compilation")
	}
	if dec.kind != cqltype.TypeCustom && r.err == nil && r.State() == cqltype.Null {
		if err := cqltype.CheckBytesAlive(r.Bytes()); err != nil {
			return err
		}
		*dst = (*dst)[:0]
		return nil
	}
	element, _ := dec.typ.Element()
	body, count, _, err := validateSequenceLayout(r, dec.kind, element, dec.width)
	if err != nil {
		return err
	}
	out := *dst
	if cap(out) < count {
		out = make([]T, count)
	} else {
		out = out[:count]
	}
	cursor := ValueReader{body: cqltype.BytesChain(body), lease: cqltype.BytesLease(body)}.cursorValue()
	for i := range out {
		var child ValueReader
		var err error
		if dec.kind == cqltype.TypeCustom {
			child, err = readVectorElement(&cursor, element, dec.width)
		} else {
			child, err = cursor.Child(element)
		}
		if err == nil {
			err = decodeValueInto(dec.element, child, &out[i])
		}
		if err != nil {
			return fmt.Errorf("gocql: element %d: %w", i, err)
		}
	}
	*dst = out
	return nil
}

func (c sequenceDecoder[T]) Clone(values []T) []T {
	if values == nil {
		return nil
	}
	out := make([]T, len(values))
	for i, v := range values {
		out[i] = c.element.Clone(v)
	}
	return out
}

type mapSliceCodec[K, V any] struct {
	enc mapEncoder[K, V]
	dec mapDecoder[K, V]
}

// MapCodec encodes and decodes a map as a []Entry[K, V], in the order the
// server sends. Keys need not be Go-comparable.
func MapCodec[K, V any](key Codec[K], value Codec[V]) Codec[[]Entry[K, V]] {
	return mapSliceCodec[K, V]{enc: mapEncoder[K, V]{key: key, value: value}, dec: mapDecoder[K, V]{key: key, value: value}}
}

func (c mapSliceCodec[K, V]) CheckType(typ cqltype.DataType) error {
	perfCheckType()
	if _, err := c.enc.compileValueEncoder(typ); err != nil {
		return err
	}
	_, err := c.dec.compileValueDecoder(typ)
	return err
}

func (c mapSliceCodec[K, V]) compileValueEncoder(typ cqltype.DataType) (Encoder[[]Entry[K, V]], error) {
	enc, err := c.enc.compileValueEncoder(typ)
	if err != nil {
		return nil, err
	}
	c.enc = enc.(mapEncoder[K, V])
	return c, nil
}

func (c mapSliceCodec[K, V]) compileValueDecoder(typ cqltype.DataType) (Decoder[[]Entry[K, V]], error) {
	dec, err := c.dec.compileValueDecoder(typ)
	if err != nil {
		return nil, err
	}
	c.dec = dec.(mapDecoder[K, V])
	return c, nil
}

func (c mapSliceCodec[K, V]) Encode(w *ValueWriter, entries []Entry[K, V]) error {
	return c.enc.Encode(w, entries)
}

func (c mapSliceCodec[K, V]) Decode(r ValueReader) ([]Entry[K, V], error) { return c.dec.Decode(r) }
func (c mapSliceCodec[K, V]) DecodeInto(r ValueReader, dst *[]Entry[K, V]) error {
	return c.dec.DecodeInto(r, dst)
}
func (c mapSliceCodec[K, V]) Clone(v []Entry[K, V]) []Entry[K, V] { return c.dec.Clone(v) }

func (c mapDecoder[K, V]) Decode(r ValueReader) ([]Entry[K, V], error) {
	var entries []Entry[K, V]
	err := c.DecodeInto(r, &entries)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (c mapDecoder[K, V]) DecodeInto(r ValueReader, dst *[]Entry[K, V]) error {
	if dst == nil {
		return cqltype.TypeValidation("destination", "nil map destination")
	}
	dec := c
	if cqltype.DescriptorOf(dec.typ) == nil {
		compiled, err := dec.compileValueDecoder(r.Type())
		if err != nil {
			return err
		}
		dec = compiled.(mapDecoder[K, V])
	} else if !dec.typ.Equal(r.Type()) {
		return cqltype.TypeValidation("type", "map metadata changed after compilation")
	}
	if r.err == nil && r.State() == cqltype.Null {
		if err := cqltype.CheckBytesAlive(r.Bytes()); err != nil {
			return err
		}
		*dst = (*dst)[:0]
		return nil
	}
	body, count, err := validateMapBody(r)
	if err != nil {
		return err
	}
	kt, vt, _ := dec.typ.KeyValue()
	out := *dst
	if cap(out) < count {
		out = make([]Entry[K, V], count)
	} else {
		out = out[:count]
	}
	cursor := ValueReader{body: cqltype.BytesChain(body), lease: cqltype.BytesLease(body)}.cursorValue()
	for i := range out {
		kr, err := cursor.Child(kt)
		if err == nil {
			err = decodeValueInto(dec.key, kr, &out[i].Key)
		}
		if err != nil {
			return fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		vr, err := cursor.Child(vt)
		if err == nil {
			err = decodeValueInto(dec.value, vr, &out[i].Value)
		}
		if err != nil {
			return fmt.Errorf("gocql: map value %d: %w", i, err)
		}
	}
	*dst = out
	return nil
}

func (c mapDecoder[K, V]) Clone(entries []Entry[K, V]) []Entry[K, V] {
	if entries == nil {
		return nil
	}
	out := make([]Entry[K, V], len(entries))
	for i, e := range entries {
		out[i] = Entry[K, V]{Key: c.key.Clone(e.Key), Value: c.value.Clone(e.Value)}
	}
	return out
}
