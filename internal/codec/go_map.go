// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"fmt"
	"math"
	"net/netip"
	"reflect"
	"sync"
	"unsafe"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type goMapCodec[K comparable, V any] struct {
	key   Codec[K]
	value Codec[V]
	// Compiled for typ; nil until then.
	keyEncoder   Encoder[K]
	keyDecoder   Decoder[K]
	valueEncoder Encoder[V]
	valueDecoder Decoder[V]
	typ          cqltype.DataType
}

// GoMapCodec encodes and decodes a CQL map as a Go map. Entries are written
// in Go's map iteration order; the server keeps a map in key order. A
// response that repeats a key fails to decode. A null cell decodes as an
// empty map. DecodeInto clears the destination map and reuses it.
func GoMapCodec[K comparable, V any](key Codec[K], value Codec[V]) Codec[map[K]V] {
	return goMapCodec[K, V]{key: key, value: value}
}

func (c goMapCodec[K, V]) CheckType(typ cqltype.DataType) error {
	perfCheckType()
	_, err := c.compile(typ)
	return err
}

func (c goMapCodec[K, V]) compile(typ cqltype.DataType) (goMapCodec[K, V], error) {
	if c.key == nil || c.value == nil {
		return c, cqltype.TypeValidation("codec", "nil map key or value codec")
	}
	kt, vt, ok := typ.KeyValue()
	if !ok {
		return c, cqltype.TypeValidation("type", "expected map, got %s", typ)
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

func (c goMapCodec[K, V]) compileValueEncoder(typ cqltype.DataType) (Encoder[map[K]V], error) {
	return c.compile(typ)
}

func (c goMapCodec[K, V]) compileValueDecoder(typ cqltype.DataType) (Decoder[map[K]V], error) {
	return c.compile(typ)
}

// bound returns c compiled for typ, compiling it now when it was not.
func (c goMapCodec[K, V]) bound(typ cqltype.DataType) (goMapCodec[K, V], error) {
	if cqltype.DescriptorOf(c.typ) == nil {
		return c.compile(typ)
	}
	if !c.typ.Equal(typ) {
		return c, cqltype.TypeValidation("type", "map metadata changed after compilation")
	}
	return c, nil
}

func (c goMapCodec[K, V]) Encode(w *ValueWriter, m map[K]V) error {
	c, err := c.bound(w.Type())
	if err != nil {
		return err
	}
	if uint64(len(m)) > math.MaxInt32 {
		return cqltype.TypeValidation("count", "map has too many entries")
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
		return cqltype.TypeValidation("destination", "nil map destination")
	}
	c, err := c.bound(r.Type())
	if err != nil {
		return err
	}
	if r.err == nil && r.State() == cqltype.Null {
		if err := cqltype.CheckBytesAlive(r.Bytes()); err != nil {
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
	cursor := ValueReader{body: cqltype.BytesChain(body), lease: cqltype.BytesLease(body)}.cursorValue()
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
			return cqltype.TypeValidation("map", "key %d repeats an earlier key", i)
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

// goMapField is a GoMapCodec erased to the map's single pointer word, so the
// struct-mapping table holds every built-in key and value pair without
// instantiating code for each pair and struct type: the fields below are
// generic only in the struct.
type goMapField interface {
	compile(cqltype.DataType) (goMapField, error)
	encode(w *ParamWriter, m unsafe.Pointer) error
	decode(r *RowReader, field unsafe.Pointer) error
	clone(m unsafe.Pointer) unsafe.Pointer
	detach(field unsafe.Pointer)
}

type erasedGoMap[K comparable, V any] struct{ codec goMapCodec[K, V] }

func (e erasedGoMap[K, V]) compile(typ cqltype.DataType) (goMapField, error) {
	c, err := e.codec.compile(typ)
	return erasedGoMap[K, V]{codec: c}, err
}
func (e erasedGoMap[K, V]) encode(w *ParamWriter, m unsafe.Pointer) error {
	return w.WriteParam(Encoder[map[K]V](e.codec), *(*map[K]V)(unsafe.Pointer(&m)))
}
func (e erasedGoMap[K, V]) decode(r *RowReader, field unsafe.Pointer) error {
	return readColumnInto(r, Decoder[map[K]V](e.codec), (*map[K]V)(field))
}
func (e erasedGoMap[K, V]) clone(m unsafe.Pointer) unsafe.Pointer {
	out := e.codec.Clone(*(*map[K]V)(unsafe.Pointer(&m)))
	return *(*unsafe.Pointer)(unsafe.Pointer(&out))
}
func (e erasedGoMap[K, V]) detach(field unsafe.Pointer) { e.codec.detachValue((*map[K]V)(field)) }

type goMapParam[P any] struct {
	m      goMapField
	loc    fieldLocation
	offset uintptr
}

func (f goMapParam[P]) location() fieldLocation { return f.loc }
func (f goMapParam[P]) compile(typ cqltype.DataType) (InputField[P], error) {
	m, err := f.m.compile(typ)
	if err != nil {
		return nil, err
	}
	f.m = m
	return f, nil
}
func (f goMapParam[P]) encode(w *ParamWriter, value P) error {
	return f.m.encode(w, *(*unsafe.Pointer)(unsafe.Add(unsafe.Pointer(&value), f.offset)))
}

type goMapColumn[R any] struct {
	m      goMapField
	loc    fieldLocation
	offset uintptr
}

func (f goMapColumn[R]) location() fieldLocation { return f.loc }
func (f goMapColumn[R]) compile(typ cqltype.DataType) (OutputField[R], error) {
	m, err := f.m.compile(typ)
	if err != nil {
		return nil, err
	}
	f.m = m
	return f, nil
}
func (f goMapColumn[R]) decode(r *RowReader, row *R) error {
	return f.m.decode(r, unsafe.Add(unsafe.Pointer(row), f.offset))
}
func (f goMapColumn[R]) clone(dst, source *R) {
	src := *(*unsafe.Pointer)(unsafe.Add(unsafe.Pointer(source), f.offset))
	*(*unsafe.Pointer)(unsafe.Add(unsafe.Pointer(dst), f.offset)) = f.m.clone(src)
}
func (f goMapColumn[R]) detachField(row *R) { f.m.detach(unsafe.Add(unsafe.Pointer(row), f.offset)) }

// goMapBuiltin is one built-in map[K]V field type of the struct-mapping table.
type goMapBuiltin struct {
	m goMapField
	BuiltinFieldType
}

type mapElement[T any] struct {
	codec        Codec[T]
	typeSrc, src string
}

// goMapBuiltins returns map[K]V for every pair of built-in codecs that decode
// into owned storage, with the comparable ones as keys. CQL allows neither a
// counter in a collection nor a duration as a key.
var goMapBuiltins = sync.OnceValue(func() []goMapBuiltin {
	var out []goMapBuiltin
	keys(&out, mapElement[bool]{Bool, "bool", "BoolCodec"})
	keys(&out, mapElement[int8]{Int8, "int8", "Int8Codec"})
	keys(&out, mapElement[int16]{Int16, "int16", "Int16Codec"})
	keys(&out, mapElement[int32]{Int32, "int32", "Int32Codec"})
	keys(&out, mapElement[int64]{Int64, "int64", "Int64Codec"})
	keys(&out, mapElement[float32]{Float32, "float32", "Float32Codec"})
	keys(&out, mapElement[float64]{Float64, "float64", "Float64Codec"})
	keys(&out, mapElement[string]{StringCodec, "string", "StringCodec"})
	keys(&out, mapElement[cqltype.UUID]{UUIDCodec, "cql.UUID", "UUIDCodec"})
	keys(&out, mapElement[TimeUUID]{TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec"})
	keys(&out, mapElement[netip.Addr]{InetCodec, "netip.Addr", "InetCodec"})
	keys(&out, mapElement[Date]{DateCodec, "cql.Date", "DateCodec"})
	keys(&out, mapElement[TimeOfDay]{TimeCodec, "cql.TimeOfDay", "TimeCodec"})
	keys(&out, mapElement[Timestamp]{TimestampCodec, "cql.Timestamp", "TimestampCodec"})
	return out
})

func keys[K comparable](out *[]goMapBuiltin, key mapElement[K]) {
	pair(out, key, mapElement[bool]{Bool, "bool", "BoolCodec"})
	pair(out, key, mapElement[int8]{Int8, "int8", "Int8Codec"})
	pair(out, key, mapElement[int16]{Int16, "int16", "Int16Codec"})
	pair(out, key, mapElement[int32]{Int32, "int32", "Int32Codec"})
	pair(out, key, mapElement[int64]{Int64, "int64", "Int64Codec"})
	pair(out, key, mapElement[float32]{Float32, "float32", "Float32Codec"})
	pair(out, key, mapElement[float64]{Float64, "float64", "Float64Codec"})
	pair(out, key, mapElement[string]{StringCodec, "string", "StringCodec"})
	pair(out, key, mapElement[[]byte]{ByteSliceCodec, "[]byte", "ByteSliceCodec"})
	pair(out, key, mapElement[cqltype.UUID]{UUIDCodec, "cql.UUID", "UUIDCodec"})
	pair(out, key, mapElement[TimeUUID]{TimeUUIDCodec, "cql.TimeUUID", "TimeUUIDCodec"})
	pair(out, key, mapElement[netip.Addr]{InetCodec, "netip.Addr", "InetCodec"})
	pair(out, key, mapElement[Date]{DateCodec, "cql.Date", "DateCodec"})
	pair(out, key, mapElement[TimeOfDay]{TimeCodec, "cql.TimeOfDay", "TimeCodec"})
	pair(out, key, mapElement[Timestamp]{TimestampCodec, "cql.Timestamp", "TimestampCodec"})
	pair(out, key, mapElement[cqltype.Duration]{DurationCodec, "cql.Duration", "DurationCodec"})
}

func pair[K comparable, V any](out *[]goMapBuiltin, key mapElement[K], value mapElement[V]) {
	src := "cql.GoMapCodec(cql." + key.src + ", cql." + value.src + ")"
	*out = append(*out, goMapBuiltin{
		m: erasedGoMap[K, V]{codec: goMapCodec[K, V]{key: key.codec, value: value.codec}},
		BuiltinFieldType: BuiltinFieldType{
			Type:       reflect.TypeFor[map[K]V](),
			TypeSrc:    "map[" + key.typeSrc + "]" + value.typeSrc,
			EncoderSrc: src, DecoderSrc: src,
			Names: []string{"GoMapCodec"}, Default: true,
		},
	})
}

// goMapFields returns the struct-mapping entries of goMapBuiltins for P.
func goMapFields[P any]() []builtinField[P] {
	builtins := goMapBuiltins()
	out := make([]builtinField[P], len(builtins))
	for i, b := range builtins {
		m := b.m
		out[i] = builtinField[P]{
			BuiltinFieldType: b.BuiltinFieldType,
			param: func(name string, offset uintptr) InputField[P] {
				return goMapParam[P]{m: m, loc: fieldLocation{name: name}, offset: offset}
			},
			column: func(name string, offset uintptr) OutputField[P] {
				return goMapColumn[P]{m: m, loc: fieldLocation{name: name}, offset: offset}
			},
		}
	}
	return out
}
