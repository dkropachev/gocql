// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"math/big"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Varint is a CQL varint, an integer of any size; BigInt converts it. The zero
// Varint is 0.
type Varint struct{ bytes Bytes }

// Decimal represents Unscaled * 10^-Scale without rounding.
//
// The zero Decimal is 0.
type Decimal struct {
	// Unscaled is the unscaled integer.
	Unscaled Varint
	// Scale is the power of ten Unscaled is divided by.
	Scale int32
}

// VarintFromBytes borrows a nonempty signed two's-complement big-endian representation.
// Keep the input immutable until parameter capture, or clone the result.
func VarintFromBytes(b []byte) (Varint, error) {
	if len(b) == 0 {
		return Varint{}, errors.New("gocql: varint requires at least one byte")
	}
	return varintFromCanonical(BorrowBytes(b))
}
func varintFromCanonical(b Bytes) (Varint, error) {
	if err := checkBytesAlive(b); err != nil {
		return Varint{}, err
	}
	if b.Len() == 0 {
		return Varint{}, errors.New("gocql: varint requires at least one byte")
	}
	var first byte
	seen := false
	var trim int64
	done := false
	err := eachBytes(b, func(s wire.Span) error {
		if done {
			return nil
		}
		for _, v := range s.Bytes() {
			if !seen {
				first = v
				seen = true
				continue
			}
			if (first == 0 && v&0x80 == 0) || (first == 0xff && v&0x80 != 0) {
				trim++
				first = v
				continue
			}
			done = true
			break
		}
		return nil
	})
	if err != nil {
		return Varint{}, err
	}
	if trim > 0 {
		b, err = b.Slice(trim, b.Len()-trim)
		if err != nil {
			return Varint{}, err
		}
	}
	return Varint{bytes: b}, nil
}

// VarintFromBigInt constructs a Varint from v without retaining its mutable big.Int storage.
func VarintFromBigInt(v *big.Int) Varint {
	if v == nil || v.Sign() == 0 {
		return Varint{}
	}
	if v.Sign() > 0 {
		b := v.Bytes()
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return Varint{bytes: BorrowBytes(b)}
	}
	// -x has the bits of ^(x-1). Include a sign byte when x-1 has its high bit set.
	magnitude := new(big.Int).Neg(v)
	magnitude.Sub(magnitude, big.NewInt(1))
	b := magnitude.Bytes()
	if len(b) == 0 {
		b = []byte{0}
	} else if b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	for i := range b {
		b[i] = ^b[i]
	}
	return Varint{bytes: BorrowBytes(b)}
}

// BigInt explicitly materializes an independent arbitrary precision integer.
// It panics, like Bytes.OwnedBytes, when v borrows a page that was already
// released.
func (v Varint) BigInt() *big.Int {
	if v.bytes.Len() == 0 {
		if err := checkBytesAlive(v.bytes); err != nil {
			panic(err)
		}
		return new(big.Int)
	}
	b := v.bytes.OwnedBytes()
	if b[0]&0x80 == 0 {
		return new(big.Int).SetBytes(b)
	}
	for i := range b {
		b[i] = ^b[i]
	}
	n := new(big.Int).SetBytes(b)
	n.Add(n, big.NewInt(1))
	return n.Neg(n)
}

func varintBytes(v Varint) Bytes { return v.bytes }
func varintView(b Bytes) Varint  { return Varint{bytes: b} }
