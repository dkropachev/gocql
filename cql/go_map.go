// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"math"
)

type goMapCodec[K comparable, V any] struct {
	key   Codec[K]
	value Codec[V]
	// Compiled for typ; nil until then.
	keyEncoder   Encoder[K]
	keyDecoder   Decoder[K]
	valueEncoder Encoder[V]
	valueDecoder Decoder[V]
	typ          DataType
}

// GoMapCodec encodes and decodes a map as a Go map[K]V, the form struct
// mappings use for map fields of built-in key and value types. Encoding
// writes the entries in Go's map iteration order; the server keeps a map in
// key order. A response that repeats a key fails to decode, and a null cell,
// how CQL stores an empty map, decodes as an empty one. DecodeInto clears the
// destination map and reuses it.
func GoMapCodec[K comparable, V any](key Codec[K], value Codec[V]) Codec[map[K]V] {
	return goMapCodec[K, V]{key: key, value: value}
}

func (c goMapCodec[K, V]) CheckType(typ DataType) error {
	perfCheckType()
	_, err := c.compile(typ)
	return err
}

func (c goMapCodec[K, V]) compile(typ DataType) (goMapCodec[K, V], error) {
	if c.key == nil || c.value == nil {
		return c, typeValidation("codec", "nil map key or value codec")
	}
	kt, vt, ok := typ.KeyValue()
	if !ok {
		return c, typeValidation("type", "expected map, got %s", typ)
	}
	var err error
	if c.keyEncoder, err = compileValueEncoder(Encoder[K](c.key), kt); err == nil {
		c.keyDecoder, err = compileValueDecoder(Decoder[K](c.key), kt)
	}
	if err != nil {
		return c, fmt.Errorf("gocql: map key type: %w", err)
	}
	if c.valueEncoder, err = compileValueEncoder(Encoder[V](c.value), vt); err == nil {
		c.valueDecoder, err = compileValueDecoder(Decoder[V](c.value), vt)
	}
	if err != nil {
		return c, fmt.Errorf("gocql: map value type: %w", err)
	}
	c.typ = typ
	return c, nil
}

func (c goMapCodec[K, V]) compileValueEncoder(typ DataType) (Encoder[map[K]V], error) {
	return c.compile(typ)
}

func (c goMapCodec[K, V]) compileValueDecoder(typ DataType) (Decoder[map[K]V], error) {
	return c.compile(typ)
}

// bound returns c compiled for typ, compiling it now when it was not.
func (c goMapCodec[K, V]) bound(typ DataType) (goMapCodec[K, V], error) {
	if descriptorOf(c.typ) == nil {
		return c.compile(typ)
	}
	if !c.typ.Equal(typ) {
		return c, typeValidation("type", "map metadata changed after compilation")
	}
	return c, nil
}

func (c goMapCodec[K, V]) Encode(w *ValueWriter, m map[K]V) error {
	c, err := c.bound(w.Type())
	if err != nil {
		return err
	}
	if uint64(len(m)) > math.MaxInt32 {
		return typeValidation("count", "map has too many entries")
	}
	if err := w.Uint32(uint32(len(m))); err != nil {
		return err
	}
	kt, vt, _ := c.typ.KeyValue()
	for k, v := range m {
		if err := writeCollectionElement(w, kt, c.keyEncoder, k); err != nil {
			return fmt.Errorf("gocql: map key: %w", err)
		}
		if err := writeCollectionElement(w, vt, c.valueEncoder, v); err != nil {
			return fmt.Errorf("gocql: map value: %w", err)
		}
	}
	return nil
}

func (c goMapCodec[K, V]) Decode(r ValueReader) (map[K]V, error) {
	var m map[K]V
	if err := c.DecodeInto(r, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c goMapCodec[K, V]) DecodeInto(r ValueReader, dst *map[K]V) error {
	if dst == nil {
		return typeValidation("destination", "nil map destination")
	}
	c, err := c.bound(r.Type())
	if err != nil {
		return err
	}
	if r.err == nil && r.State() == Null {
		if err := checkBytesAlive(r.Bytes()); err != nil {
			return err
		}
		clear(*dst)
		return nil
	}
	body, count, err := validateMapBody(r)
	if err != nil {
		return err
	}
	m := *dst
	if m == nil {
		m = make(map[K]V, count)
	} else {
		clear(m)
	}
	kt, vt, _ := c.typ.KeyValue()
	cursor := ValueReader{body: bytesChain(body), lease: bytesLease(body)}.cursorValue()
	for i := range count {
		var k K
		var v V
		kr, err := cursor.Child(kt)
		if err == nil {
			err = decodeValueInto(c.keyDecoder, kr, &k)
		}
		if err != nil {
			return fmt.Errorf("gocql: map key %d: %w", i, err)
		}
		vr, err := cursor.Child(vt)
		if err == nil {
			err = decodeValueInto(c.valueDecoder, vr, &v)
		}
		if err != nil {
			return fmt.Errorf("gocql: map value %d: %w", i, err)
		}
		m[k] = v
		if len(m) != i+1 {
			return typeValidation("map", "key %d repeats an earlier key", i)
		}
	}
	*dst = m
	return nil
}

func (c goMapCodec[K, V]) Clone(m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[c.key.Clone(k)] = c.value.Clone(v)
	}
	return out
}

// detachValue detaches the values in place when the keys own their storage,
// as the built-in codecs a struct mapping picks do; otherwise it clones.
func (c goMapCodec[K, V]) detachValue(dst *map[K]V) {
	if !ownsDecoded(Decoder[K](c.key)) {
		*dst = c.Clone(*dst)
		return
	}
	if ownsDecoded(Decoder[V](c.value)) {
		return
	}
	for k, v := range *dst {
		detachValue(Decoder[V](c.value), &v)
		(*dst)[k] = v
	}
}

// ownsDecoded reports whether d is a built-in codec whose decoded values hold
// no response storage.
func ownsDecoded[T any](d Decoder[T]) bool {
	switch c := any(d).(type) {
	case scalarCodec[T]:
		return c.clone == nil
	case stringCodec, byteSliceCodec:
		return true
	}
	return false
}
