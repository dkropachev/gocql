// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

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

// VarintFromInt64 constructs a Varint holding n.
func VarintFromInt64(n int64) Varint {
	if n == 0 {
		return Varint{}
	}
	var b [8]byte
	for i := range b {
		b[i] = byte(n >> (56 - 8*i))
	}
	// Drop leading bytes that only repeat the sign.
	start := 0
	for start < 7 && (b[start] == 0 && b[start+1]&0x80 == 0 || b[start] == 0xff && b[start+1]&0x80 != 0) {
		start++
	}
	return Varint{bytes: BorrowBytes(append([]byte(nil), b[start:]...))}
}

// Int64 returns v as an int64. It fails with a ValidationError when v does
// not fit, and with ErrReleased once the view has expired.
func (v Varint) Int64() (int64, error) {
	if err := checkBytesAlive(v.bytes); err != nil {
		return 0, err
	}
	if v.bytes.Len() > 8 {
		return 0, typeValidation("varint", "value overflows int64")
	}
	if v.bytes.Len() == 0 {
		return 0, nil
	}
	var b [8]byte
	if _, err := v.bytes.CopyTo(b[:v.bytes.Len()]); err != nil {
		return 0, err
	}
	n := int64(int8(b[0]))
	for _, c := range b[1:v.bytes.Len()] {
		n = n<<8 | int64(c)
	}
	return n, nil
}

// BigInt explicitly materializes an independent arbitrary precision integer.
// It panics with ErrReleased, as Bytes.Clone does, when v borrows a page that
// was already released.
func (v Varint) BigInt() *big.Int {
	if v.bytes.Len() == 0 {
		if err := checkBytesAlive(v.bytes); err != nil {
			panic(err)
		}
		return new(big.Int)
	}
	b := v.bytes.ownedBytes()
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

// String returns the integer in base 10, or "<released>" once the view has
// expired.
func (v Varint) String() string {
	if checkBytesAlive(v.bytes) != nil {
		return "<released>"
	}
	return v.BigInt().String()
}

// ParseDecimal parses s in the forms Decimal.String prints and Java's
// BigDecimal accepts: an optional sign, digits with an optional decimal
// point, and an optional exponent, such as "12.30", "-7" or "1.23E+5". The
// scale is the number of fraction digits less the exponent, so "12.30" has
// scale 2 and "1.23E+5" scale -3.
func ParseDecimal(s string) (Decimal, error) {
	fail := func() (Decimal, error) {
		return Decimal{}, typeValidation("decimal", "invalid decimal %q", s)
	}
	mantissa, exponent := s, int64(0)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.ParseInt(s[i+1:], 10, 32)
		if err != nil {
			return fail()
		}
		mantissa, exponent = s[:i], e
	}
	sign := ""
	if mantissa != "" && (mantissa[0] == '-' || mantissa[0] == '+') {
		sign, mantissa = mantissa[:1], mantissa[1:]
	}
	whole, fraction, _ := strings.Cut(mantissa, ".")
	digits := whole + fraction
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return fail()
	}
	scale := int64(len(fraction)) - exponent
	if scale < math.MinInt32 || scale > math.MaxInt32 {
		return fail()
	}
	n, ok := new(big.Int).SetString(sign+digits, 10)
	if !ok {
		return fail()
	}
	return Decimal{Unscaled: VarintFromBigInt(n), Scale: int32(scale)}, nil
}

// Rat returns the decimal as an exact rational number. It panics, like
// Varint.BigInt, when the decimal borrows a page that was already released.
func (d Decimal) Rat() *big.Rat {
	r := new(big.Rat).SetInt(d.Unscaled.BigInt())
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(math.Abs(float64(d.Scale)))), nil)
	if d.Scale >= 0 {
		return r.Quo(r, new(big.Rat).SetInt(power))
	}
	return r.Mul(r, new(big.Rat).SetInt(power))
}

// String returns the decimal as Java's BigDecimal.toString, which Cassandra
// uses: plain notation such as "12.30", or scientific notation such as
// "1.23E+5" for a negative scale or a value below 1e-6. It returns
// "<released>" once the view has expired.
func (d Decimal) String() string {
	if checkBytesAlive(d.Unscaled.bytes) != nil {
		return "<released>"
	}
	digits := d.Unscaled.BigInt().String()
	sign := ""
	if digits[0] == '-' {
		sign, digits = "-", digits[1:]
	}
	scale := int64(d.Scale)
	adjusted := int64(len(digits)) - 1 - scale
	if scale >= 0 && adjusted >= -6 {
		if scale == 0 {
			return sign + digits
		}
		if pad := scale - int64(len(digits)) + 1; pad > 0 {
			digits = strings.Repeat("0", int(pad)) + digits
		}
		point := len(digits) - int(scale)
		return sign + digits[:point] + "." + digits[point:]
	}
	mantissa := digits[:1]
	if len(digits) > 1 {
		mantissa += "." + digits[1:]
	}
	return fmt.Sprintf("%s%sE%+d", sign, mantissa, adjusted)
}
