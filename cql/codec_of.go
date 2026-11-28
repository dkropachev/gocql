// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"reflect"
	"unsafe"
)

// CodecOf returns the codec a struct mapping derives for a field of type T:
// a built-in codec for a leaf type such as int32, string or UUID, a type's
// own CQLCodec method, and slices, arrays, pointers, maps, Nullable and Pair
// built from those, as ParamsOf and RowOf map a field. WithCodec maps a type
// T is derived from with the codec it names. It fails for a type the rules
// do not map, and for one that only binds parameters, such as BindValue;
// use ParamOf for that.
func CodecOf[T any](opts ...StructOption) (Codec[T], error) {
	d, err := deriveValue[T](opts)
	if err != nil {
		return nil, err
	}
	if !d.decodes {
		return nil, typeValidation("codec", "%s only binds parameters; use ParamOf", reflect.TypeFor[T]())
	}
	return derivedCodecOf[T](d), nil
}

// MustCodecOf is CodecOf that panics on error, for package-level variables.
func MustCodecOf[T any](opts ...StructOption) Codec[T] {
	c, err := CodecOf[T](opts...)
	if err != nil {
		panic(err)
	}
	return c
}

// ParamOf binds one parameter of type T with the codec CodecOf derives for
// it, as OneParam does with an explicit encoder; a BindValue is accepted. A
// type the rules do not map fails when the statement is prepared or bound.
func ParamOf[T any]() ParamEncoder[T] {
	d, err := deriveValue[T](nil)
	if err != nil {
		return OneParam[T](failedCodec[T]{err: err})
	}
	return OneParam[T](derivedCodecOf[T](d))
}

// ColumnOf decodes one result column of type T with the codec CodecOf
// derives for it, as OneColumn does with an explicit decoder. A type the
// rules do not map, or one that only binds parameters, fails when the
// statement is prepared or run.
func ColumnOf[T any]() RowDecoder[T] {
	c, err := CodecOf[T]()
	if err != nil {
		return OneColumn[T](failedCodec[T]{err: err})
	}
	return OneColumn(c)
}

// deriveValue derives the codec of a value of type T with opts.
func deriveValue[T any](opts []StructOption) (derived, error) {
	t := reflect.TypeFor[T]()
	set, err := applyOptions(targetValue, t.String(), opts)
	if err != nil {
		return derived{}, err
	}
	return deriveField(t, "", set.codecs)
}

// derivedCodecOf returns d as a Codec[T]: its generic leaf codec when it has
// one, else its rcodec over the value's address.
func derivedCodecOf[T any](d derived) Codec[T] {
	if leaf, ok := d.codec.(leafR[T]); ok {
		if c, ok := leaf.enc.(Codec[T]); ok && leaf.dec != nil {
			return c
		}
	}
	return derivedCodec[T]{c: d.codec, decodes: d.decodes}
}

// derivedCodec adapts an rcodec to Codec[T]. Compiled for a type, it keeps
// the compiled rcodec and checks that a value's type is still that one.
type derivedCodec[T any] struct {
	c        rcodec
	typ      DataType
	decodes  bool
	compiled bool
}

func (c derivedCodec[T]) dirs() rdir {
	if c.decodes {
		return encDir | decDir
	}
	return encDir
}

func (c derivedCodec[T]) CheckType(t DataType) error {
	perfCheckType()
	if c.compiled {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled derived codec metadata changed")
	}
	_, err := c.c.compile(t, c.dirs())
	return err
}

// forType returns c compiled for t.
func (c derivedCodec[T]) forType(t DataType) (derivedCodec[T], error) {
	if c.compiled {
		if !c.typ.Equal(t) {
			return c, typeValidation("type", "compiled derived codec metadata changed")
		}
		return c, nil
	}
	compiled, err := c.c.compile(t, c.dirs())
	if err != nil {
		return c, err
	}
	return derivedCodec[T]{c: compiled, typ: t, decodes: c.decodes, compiled: true}, nil
}

func (c derivedCodec[T]) compileValueEncoder(t DataType) (Encoder[T], error) {
	return c.forType(t)
}

func (c derivedCodec[T]) compileValueDecoder(t DataType) (Decoder[T], error) {
	return c.forType(t)
}

func (c derivedCodec[T]) Encode(w *ValueWriter, v T) error {
	compiled, err := c.forType(w.Type())
	if err != nil {
		return err
	}
	return compiled.c.encode(w, unsafe.Pointer(&v))
}

func (c derivedCodec[T]) Decode(r ValueReader) (T, error) {
	var v T
	if !c.decodes {
		return v, typeValidation("codec", "%s only binds parameters", reflect.TypeFor[T]())
	}
	compiled, err := c.forType(r.Type())
	if err != nil {
		return v, err
	}
	err = compiled.c.decode(r, unsafe.Pointer(&v))
	return v, err
}

func (c derivedCodec[T]) Clone(v T) T {
	var out T
	c.c.clone(unsafe.Pointer(&out), unsafe.Pointer(&v))
	return out
}

// failedCodec is the codec of a type that could not be derived: it fails
// when a plan compiles it.
type failedCodec[T any] struct{ err error }

func (c failedCodec[T]) CheckType(DataType) error { return c.err }
func (c failedCodec[T]) Encode(*ValueWriter, T) error {
	return c.err
}
func (c failedCodec[T]) Decode(ValueReader) (T, error) {
	var zero T
	return zero, c.err
}
func (c failedCodec[T]) Clone(v T) T { return v }
