// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
)

func canonicalVarint(b Bytes) (Varint, error) { return varintFromCanonical(b) }
func cloneVarint(v Varint) Varint             { return varintView(varintBytes(v).Clone()) }
func encodeVarint(w *ValueWriter, v Varint) error {
	if err := checkBytesAlive(varintBytes(v)); err != nil {
		return err
	}
	if varintBytes(v).Len() == 0 {
		return w.Uint8(0)
	}
	return w.borrowView(varintBytes(v))
}
func decodeVarint(r ValueReader) (Varint, error) { return canonicalVarint(r.Bytes()) }

// VarintCodec maps varint to Varint. A decoded Varint borrows the response;
// use VarintCodec.Clone to detach it.
var VarintCodec Codec[Varint] = scalarCodec[Varint]{kind: TypeVarint, encode: encodeVarint, decode: decodeVarint, clone: cloneVarint}

// DecimalCodec maps decimal to Decimal. A decoded Decimal borrows the
// response; use DecimalCodec.Clone to detach it.
var DecimalCodec Codec[Decimal] = scalarCodec[Decimal]{kind: TypeDecimal,
	encode: func(w *ValueWriter, v Decimal) error {
		if err := w.Uint32(uint32(v.Scale)); err != nil {
			return err
		}
		return encodeVarint(w, v.Unscaled)
	},
	decode: func(r ValueReader) (Decimal, error) {
		if r.Bytes().Len() < 5 {
			return Decimal{}, errors.New("gocql: decimal requires a scale and nonempty varint")
		}
		c := r.cursorValue()
		scale, err := c.Uint32()
		if err != nil {
			return Decimal{}, err
		}
		b, err := c.Take(c.Remaining())
		if err != nil {
			return Decimal{}, err
		}
		v, err := canonicalVarint(b)
		if err != nil {
			return Decimal{}, err
		}
		return Decimal{Scale: int32(scale), Unscaled: v}, nil
	},
	clone: func(v Decimal) Decimal { return Decimal{Scale: v.Scale, Unscaled: cloneVarint(v.Unscaled)} },
}
