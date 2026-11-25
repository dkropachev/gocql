// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
)

// PointerCodec maps a *T with base, as NullableCodec maps a Nullable[T]: a
// nil pointer encodes NULL, and NULL decodes to nil. A present value
// decodes into a newly allocated T. Struct mappings use it for *T fields.
func PointerCodec[T any](base Codec[T]) Codec[*T] {
	return pointerCodec[T]{base: base}
}

type pointerCodec[T any] struct {
	base    Codec[T]
	encoder Encoder[T]
	decoder Decoder[T]
	typ     DataType
}

func (c pointerCodec[T]) CheckType(t DataType) error {
	perfCheckType()
	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil pointer codec")
	}
	return c.base.CheckType(t)
}

func (c pointerCodec[T]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c pointerCodec[T]) Encode(w *ValueWriter, v *T) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	if v == nil {
		return w.Null()
	}
	encoder := c.encoder
	if encoder == nil {
		encoder = c.base
	}
	return encoder.Encode(w, *v)
}

func (c pointerCodec[T]) Decode(r ValueReader) (*T, error) {
	if e := c.typeGuard(r.Type()); e != nil {
		return nil, e
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.State() == Null {
		return nil, checkBytesAlive(r.Bytes())
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	v := new(T)
	if err := decodeValueInto(decoder, r, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (c pointerCodec[T]) Clone(v *T) *T {
	if v == nil {
		return nil
	}
	out := new(T)
	*out = c.base.Clone(*v)
	return out
}

func (c pointerCodec[T]) compileValueEncoder(t DataType) (Encoder[*T], error) {
	if c.base == nil {
		return nil, errors.New("gocql: nil pointer codec")
	}
	base, err := compileValueEncoder(Encoder[T](c.base), t)
	if err != nil {
		return nil, err
	}
	c.encoder, c.typ = base, t
	return c, nil
}

func (c pointerCodec[T]) compileValueDecoder(t DataType) (Decoder[*T], error) {
	if c.base == nil {
		return nil, errors.New("gocql: nil pointer codec")
	}
	base, err := compileValueDecoder(Decoder[T](c.base), t)
	if err != nil {
		return nil, err
	}
	c.decoder, c.typ = base, t
	return c, nil
}
