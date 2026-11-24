// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"math"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// EncodedValue contains one checked value body, its complete type, and cell
// state. Borrowed payloads remain subject to their source lifetime. The zero
// EncodedValue has the invalid zero DataType, which DecodeValue rejects with a
// ValidationError; NewEncodedValue and EncodeValue make valid ones.
type EncodedValue struct {
	typ   DataType
	body  Bytes
	state CellState
}

// NewEncodedValue checks the descriptor and state/body framing. Scalar content
// is checked by the selected decoder when DecodeValue is called.
func NewEncodedValue(typ DataType, state CellState, body Bytes) (EncodedValue, error) {
	if err := validateDataType(typ); err != nil {
		return EncodedValue{}, err
	}
	if err := checkBytesAlive(body); err != nil {
		return EncodedValue{}, err
	}
	if body.Len() > math.MaxInt32 {
		return EncodedValue{}, typeValidation("value", "cell body exceeds signed 32-bit length")
	}
	if state > EmptyCell {
		return EncodedValue{}, typeValidation("state", "unknown cell state %v", state)
	}
	if state != Present && body.Len() != 0 {
		return EncodedValue{}, typeValidation("value", "state %v cannot contain payload bytes", state)
	}
	if state == EmptyCell && !emptySupported(typ) {
		return EncodedValue{}, typeValidation("type", "%s does not support empty values", typ)
	}
	return EncodedValue{typ: typ, state: state, body: body}, nil
}

// Type returns the value's type.
func (v EncodedValue) Type() DataType { return v.typ }

// State returns the value's cell state.
func (v EncodedValue) State() CellState { return v.state }

// Bytes returns the value's body. A borrowed body has the lifetime of its
// source.
func (v EncodedValue) Bytes() Bytes { return v.body }

// Clone detaches all encoded storage. It panics with ErrReleased after release.
func (v EncodedValue) Clone() EncodedValue {
	return EncodedValue{typ: v.typ, state: v.state, body: v.body.Clone()}
}

// EncodeValueBorrowed encodes one value without copying borrowed input payloads.
// The caller must keep those payloads immutable while the encoded value is used.
func EncodeValueBorrowed[T any](typ DataType, encoder Encoder[T], value T) (EncodedValue, error) {
	return encodeValue(typ, encoder, value, false)
}

// EncodeValue encodes one value and copies every borrowed input payload, so the
// result does not depend on the caller's buffers. Use EncodeValueBorrowed to
// skip the copy.
func EncodeValue[T any](typ DataType, encoder Encoder[T], value T) (EncodedValue, error) {
	return encodeValue(typ, encoder, value, true)
}
func encodeValue[T any](typ DataType, encoder Encoder[T], value T, owned bool) (EncodedValue, error) {
	if err := validateDataType(typ); err != nil {
		return EncodedValue{}, err
	}
	if encoder == nil {
		return EncodedValue{}, typeValidation("encoder", "nil encoder")
	}
	compiled, err := compileValueEncoder(encoder, typ)
	if err != nil {
		return EncodedValue{}, err
	}
	builder := wire.NewBuilder(0)
	writer := newValueWriter(builder, typ, true)
	writer.ownBorrowed = owned
	if err := compiled.Encode(writer, value); err != nil {
		abortValueWriter(writer)
		return EncodedValue{}, err
	}
	if err := finishValueWriter(writer); err != nil {
		return EncodedValue{}, err
	}
	chain, err := builder.Freeze()
	if err != nil {
		return EncodedValue{}, err
	}
	// Slice skips only the enclosing length, retaining the existing payload spans.
	body, err := chain.Slice(4, chain.Len()-4)
	if err != nil {
		return EncodedValue{}, err
	}
	return EncodedValue{typ: typ, state: writer.state, body: bytesView(body, wire.Ref{})}, nil
}

// DecodeValue checks the requested type before invoking the decoder. Unset is
// an input-binding state and is never admitted as a result value. The decoder
// sees an EmptyCell value as a response holds it: Present, with an empty body.
func DecodeValue[T any](decoder Decoder[T], value EncodedValue) (T, error) {
	var zero T
	if err := validateDataType(value.typ); err != nil {
		return zero, err
	}
	if decoder == nil {
		return zero, typeValidation("decoder", "nil decoder")
	}
	compiled, err := compileValueDecoder(decoder, value.typ)
	if err != nil {
		return zero, err
	}
	if err := checkBytesAlive(value.body); err != nil {
		return zero, err
	}
	if value.state == Unset {
		return zero, typeValidation("state", "unset cannot be decoded as a result")
	}
	state := value.state
	if state == EmptyCell {
		state = Present
	}
	return compiled.Decode(newValueReader(value.typ, state, bytesChain(value.body), bytesLease(value.body)))
}
