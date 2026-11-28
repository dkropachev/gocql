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
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Varint is a CQL varint, an integer of any size, held as its shortest
// two's-complement big-endian bytes; BigInt and Int64 convert it, and Cmp
// orders it. A Varint owns its bytes: a decoded one stays valid after the
// response is released, and two Varints of the same integer are ==. The zero
// Varint is 0.
type Varint struct{ b string }

// Decimal represents Unscaled * 10^-Scale without rounding. Like Varint, it
// owns its value.
//
// The zero Decimal is 0.
type Decimal struct {
	// Unscaled is the unscaled integer.
	Unscaled Varint
	// Scale is the power of ten Unscaled is divided by.
	Scale int32
}

// VarintFromBytes returns the Varint of a nonempty signed two's-complement
// big-endian representation, such as a varint cell's body. It copies b.
func VarintFromBytes(b []byte) (Varint, error) {
	if len(b) == 0 {
		return Varint{}, errors.New("gocql: varint requires at least one byte")
	}
	return varintFromCanonical(BorrowBytes(b))
}

// varintFromCanonical returns the Varint of the two's-complement bytes b,
// copied without their redundant sign bytes.
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
	s, err := textView(b).Copy()
	if err != nil {
		return Varint{}, err
	}
	if s == "\x00" {
		// The zero Varint holds no bytes, so that it is == to Varint{}.
		s = ""
	}
	return Varint{b: s}, nil
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
		return Varint{b: string(b)}
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
	return Varint{b: string(b)}
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
	return Varint{b: string(b[start:])}
}

// Int64 returns v as an int64. It fails with a ValidationError when v does
// not fit.
func (v Varint) Int64() (int64, error) {
	if len(v.b) > 8 {
		return 0, typeValidation("varint", "value overflows int64")
	}
	if len(v.b) == 0 {
		return 0, nil
	}
	n := int64(int8(v.b[0]))
	for i := 1; i < len(v.b); i++ {
		n = n<<8 | int64(v.b[i])
	}
	return n, nil
}

// BigInt returns v as a new arbitrary precision integer.
func (v Varint) BigInt() *big.Int {
	if len(v.b) == 0 {
		return new(big.Int)
	}
	b := []byte(v.b)
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

// Sign returns -1, 0 or +1 as v is negative, zero or positive.
func (v Varint) Sign() int {
	switch {
	case len(v.b) == 0:
		return 0
	case v.b[0]&0x80 != 0:
		return -1
	default:
		return 1
	}
}

// Cmp compares v and w and returns -1, 0 or +1 as v is less than, equal to
// or greater than w.
func (v Varint) Cmp(w Varint) int {
	sv, sw := v.Sign(), w.Sign()
	if sv != sw {
		return cmpInt(sv, sw)
	}
	if sv == 0 {
		return 0
	}
	// Both are shortest forms of one sign: a longer one has the larger
	// magnitude, and same-length ones order as unsigned bytes.
	if len(v.b) != len(w.b) {
		if sv > 0 {
			return cmpInt(len(v.b), len(w.b))
		}
		return cmpInt(len(w.b), len(v.b))
	}
	return strings.Compare(v.b, w.b)
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// varintBytes returns v's bytes as a read-only view of its string.
func varintBytes(v Varint) Bytes {
	if len(v.b) == 0 {
		return Bytes{}
	}
	return BorrowBytes(unsafe.Slice(unsafe.StringData(v.b), len(v.b)))
}

// varintOf returns the Varint of a dynamic value's bytes, empty for the zero
// Varint, copied.
func varintOf(b Bytes) (Varint, error) {
	if err := checkBytesAlive(b); err != nil {
		return Varint{}, err
	}
	if b.Len() == 0 {
		return Varint{}, nil
	}
	return varintFromCanonical(b)
}

// String returns the integer in base 10.
func (v Varint) String() string { return v.BigInt().String() }

// MarshalText returns the integer in base 10, as String does.
func (v Varint) MarshalText() ([]byte, error) { return []byte(v.String()), nil }

// UnmarshalText parses an optionally signed base-10 integer, the form
// MarshalText returns.
func (v *Varint) UnmarshalText(text []byte) error {
	n, ok := new(big.Int).SetString(string(text), 10)
	if !ok {
		return typeValidation("varint", "invalid varint %q", text)
	}
	*v = VarintFromBigInt(n)
	return nil
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

// Rat returns the decimal as an exact rational number.
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
// "1.23E+5" for a negative scale or a value below 1e-6.
func (d Decimal) String() string {
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

// MarshalText returns the decimal as String does.
func (d Decimal) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalText parses the forms ParseDecimal accepts.
func (d *Decimal) UnmarshalText(text []byte) error {
	v, err := ParseDecimal(string(text))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// Cmp compares the values of d and e, whatever their scales, and returns -1,
// 0 or +1 as d is less than, equal to or greater than e. Decimals of one
// value at different scales, such as 1.0 and 1.00, compare equal but are not
// ==.
func (d Decimal) Cmp(e Decimal) int {
	if d.Scale == e.Scale {
		return d.Unscaled.Cmp(e.Unscaled)
	}
	return d.Rat().Cmp(e.Rat())
}
