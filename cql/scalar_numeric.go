// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"math/big"
)

func canonicalVarint(b Bytes) (Varint, error) { return varintFromCanonical(b) }
func encodeVarint(w *ValueWriter, v Varint) error {
	if len(v.b) == 0 {
		return w.Uint8(0)
	}
	// A Varint's string never changes, so the writer may borrow it.
	return w.BorrowText(v.b)
}
func decodeVarint(r ValueReader) (Varint, error) { return canonicalVarint(r.Bytes()) }

// VarintCodec maps varint to Varint. A decoded Varint owns its bytes;
// VarintViewCodec reads them without copying.
var VarintCodec Codec[Varint] = scalarCodec[Varint]{kind: KindVarint, encode: encodeVarint, decode: decodeVarint}

// VarintViewCodec maps varint to Bytes holding its two's-complement
// big-endian body, as the server sent it. A decoded Bytes borrows the
// response, and encoding borrows the Bytes, as BytesViewCodec does; Clone
// detaches it. Encoding rejects an empty Bytes, which is no varint;
// VarintFromBytes converts the bytes to a Varint.
var VarintViewCodec Codec[Bytes] = scalarCodec[Bytes]{kind: KindVarint,
	encode: func(w *ValueWriter, v Bytes) error {
		if err := checkBytesAlive(v); err != nil {
			return err
		}
		if v.Len() == 0 {
			return errors.New("gocql: varint requires at least one byte")
		}
		return w.borrowView(v)
	},
	decode: func(r ValueReader) (Bytes, error) {
		if r.Bytes().Len() == 0 {
			return Bytes{}, errors.New("gocql: varint requires at least one byte")
		}
		return r.Bytes(), nil
	},
	clone: func(v Bytes) Bytes { return v.Clone() },
}

// VarintBigIntCodec maps varint to *big.Int. A nil *big.Int encodes NULL,
// and NULL decodes to nil, as PointerCodec does; a present value decodes
// into a new big.Int that does not borrow the response.
var VarintBigIntCodec Codec[*big.Int] = varintBigIntCodec{}

type varintBigIntCodec struct{}

func (varintBigIntCodec) CheckType(t DataType) error { return VarintCodec.CheckType(t) }
func (varintBigIntCodec) Encode(w *ValueWriter, v *big.Int) error {
	if v == nil {
		if err := VarintCodec.CheckType(w.Type()); err != nil {
			return err
		}
		return w.Null()
	}
	return VarintCodec.Encode(w, VarintFromBigInt(v))
}
func (varintBigIntCodec) Decode(r ValueReader) (*big.Int, error) {
	if err := VarintCodec.CheckType(r.Type()); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.State() == CellNull {
		return nil, checkBytesAlive(r.Bytes())
	}
	v, err := VarintCodec.Decode(r)
	if err != nil {
		return nil, err
	}
	return v.BigInt(), nil
}
func (varintBigIntCodec) Clone(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

// DecimalCodec maps decimal to Decimal. A decoded Decimal owns its value.
var DecimalCodec Codec[Decimal] = scalarCodec[Decimal]{kind: KindDecimal,
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
}
