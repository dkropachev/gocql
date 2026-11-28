// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
)

// ConvertCodec maps A through base, which maps B: to converts a value before
// base encodes it, and from converts what base decodes. Use it for an
// application type with no built-in codec, such as one that wraps, packs or
// validates a built-in type. An error from to fails the encode, and one from
// from the decode, as the codec's own errors do (see CodecError). Clone
// clones through base; when a conversion fails there, which a value that
// decoded cannot do unless the conversions disagree, it returns the value
// as it is.
func ConvertCodec[A, B any](base Codec[B], to func(A) (B, error), from func(B) (A, error)) Codec[A] {
	return convertCodec[A, B]{convertEncoder: convertEncoder[A, B]{base: base, to: to}, convertDecoder: convertDecoder[A, B]{base: base, from: from, to: to}}
}

// ConvertEncoder is the encoding half of ConvertCodec, for a type that is
// only ever bound.
func ConvertEncoder[A, B any](base Encoder[B], to func(A) (B, error)) Encoder[A] {
	return convertEncoder[A, B]{base: base, to: to}
}

// ConvertDecoder is the decoding half of ConvertCodec, for a type that is
// only ever read. Clone clones the value through base only when it has no
// way back to B, so it returns the value as it is: from must return a value
// that holds no borrowed storage, such as an owned string or slice.
func ConvertDecoder[A, B any](base Decoder[B], from func(B) (A, error)) Decoder[A] {
	return convertDecoder[A, B]{base: base, from: from}
}

type convertEncoder[A, B any] struct {
	base    Encoder[B]
	to      func(A) (B, error)
	encoder Encoder[B] // base compiled for typ
	typ     DataType
}

func (c convertEncoder[A, B]) CheckType(t DataType) error {
	perfCheckType()
	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled converted codec metadata changed")
	}
	if c.base == nil || c.to == nil {
		return errors.New("gocql: incomplete converted encoder")
	}
	return c.base.CheckType(t)
}

func (c convertEncoder[A, B]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c convertEncoder[A, B]) Encode(w *ValueWriter, v A) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	encoder := c.encoder
	if encoder == nil {
		encoder = c.base
	}
	b, err := c.to(v)
	if err != nil {
		return err
	}
	return encoder.Encode(w, b)
}

func (c convertEncoder[A, B]) compileValueEncoder(t DataType) (Encoder[A], error) {
	if c.base == nil || c.to == nil {
		return nil, typeValidation("codec", "nil conversion")
	}
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.encoder, c.typ = base, t
	return c, nil
}

type convertDecoder[A, B any] struct {
	base    Decoder[B]
	from    func(B) (A, error)
	to      func(A) (B, error) // nil for ConvertDecoder: Clone keeps the value
	decoder Decoder[B]         // base compiled for typ
	typ     DataType
}

func (c convertDecoder[A, B]) CheckType(t DataType) error {
	perfCheckType()
	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled converted codec metadata changed")
	}
	if c.base == nil || c.from == nil {
		return errors.New("gocql: incomplete converted decoder")
	}
	return c.base.CheckType(t)
}

func (c convertDecoder[A, B]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c convertDecoder[A, B]) Decode(r ValueReader) (A, error) {
	var zero A
	if e := c.typeGuard(r.Type()); e != nil {
		return zero, e
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	v, e := decoder.Decode(r)
	if e != nil {
		return zero, e
	}
	return c.from(v)
}

func (c convertDecoder[A, B]) Clone(v A) A {
	if c.to == nil || c.base == nil || c.from == nil {
		return v
	}
	b, err := c.to(v)
	if err != nil {
		return v
	}
	out, err := c.from(c.base.Clone(b))
	if err != nil {
		return v
	}
	return out
}

func (c convertDecoder[A, B]) compileValueDecoder(t DataType) (Decoder[A], error) {
	if c.base == nil || c.from == nil {
		return nil, typeValidation("codec", "nil conversion")
	}
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.decoder, c.typ = base, t
	return c, nil
}

type convertCodec[A, B any] struct {
	convertEncoder[A, B]
	convertDecoder[A, B]
}

func (c convertCodec[A, B]) CheckType(t DataType) error {
	if err := c.convertEncoder.CheckType(t); err != nil {
		return err
	}
	return c.convertDecoder.CheckType(t)
}

func (c convertCodec[A, B]) compileValueEncoder(t DataType) (Encoder[A], error) {
	enc, err := c.convertEncoder.compileValueEncoder(t)
	if err != nil {
		return nil, err
	}
	c.convertEncoder = enc.(convertEncoder[A, B])
	return c, nil
}

func (c convertCodec[A, B]) compileValueDecoder(t DataType) (Decoder[A], error) {
	dec, err := c.convertDecoder.compileValueDecoder(t)
	if err != nil {
		return nil, err
	}
	c.convertDecoder = dec.(convertDecoder[A, B])
	return c, nil
}
