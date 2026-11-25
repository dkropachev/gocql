// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"math"
	"math/big"
	"testing"
)

func TestVarintInt64(t *testing.T) {
	for _, n := range []int64{0, 1, -1, 127, 128, -128, -129, 255, 256, math.MaxInt64, math.MinInt64, 1 << 40, -(1 << 40)} {
		v := VarintFromInt64(n)
		if v.BigInt().Int64() != n || !v.BigInt().IsInt64() {
			t.Fatalf("%d: BigInt %v", n, v.BigInt())
		}
		canonical := VarintFromBigInt(big.NewInt(n))
		if a, b := CodecTestMust(varintBytes(v).Copy()), CodecTestMust(varintBytes(canonical).Copy()); string(a) != string(b) {
			t.Fatalf("%d: bytes %x, want %x", n, a, b)
		}
		got, err := v.Int64()
		if err != nil || got != n {
			t.Fatalf("%d: Int64 = %d, %v", n, got, err)
		}
	}
	big := VarintFromBigInt(new(big.Int).Lsh(big.NewInt(1), 63))
	if _, err := big.Int64(); err == nil {
		t.Fatal("2^63 fit an int64")
	}
}

func TestParseDecimal(t *testing.T) {
	for _, tc := range []struct {
		in, unscaled string
		scale        int32
		out          string
	}{
		{"12.30", "1230", 2, "12.30"},
		{"-7", "-7", 0, "-7"},
		{"+0.001", "1", 3, "0.001"},
		{"1.23E+5", "123", -3, "1.23E+5"},
		{"5e-2", "5", 2, "0.05"},
		{".5", "5", 1, "0.5"},
	} {
		d, err := ParseDecimal(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if d.Unscaled.BigInt().String() != tc.unscaled || d.Scale != tc.scale || d.String() != tc.out {
			t.Fatalf("%s: %v scale %d, %s", tc.in, d.Unscaled, d.Scale, d)
		}
	}
	for _, bad := range []string{"", "-", ".", "1.2.3", "1e", "abc", "1,5", "1e99999999999"} {
		if _, err := ParseDecimal(bad); err == nil {
			t.Fatalf("%q parsed", bad)
		}
	}
	d := CodecTestMust(ParseDecimal("-12.5"))
	if d.Rat().Cmp(big.NewRat(-25, 2)) != 0 {
		t.Fatal(d.Rat())
	}
	if r := CodecTestMust(ParseDecimal("1.5E+2")).Rat(); r.Cmp(big.NewRat(150, 1)) != 0 {
		t.Fatal(r)
	}
}

func TestVarintBigIntCodecClone(t *testing.T) {
	if VarintBigIntCodec.Clone(nil) != nil {
		t.Fatal("Clone(nil) is not nil")
	}
	v := big.NewInt(42)
	c := VarintBigIntCodec.Clone(v)
	if c == v || c.Cmp(v) != 0 {
		t.Fatalf("Clone = %v (same pointer %t), want a copy of %v", c, c == v, v)
	}
	v.SetInt64(7)
	if c.Int64() != 42 {
		t.Fatalf("clone changed with the original: %v", c)
	}
}

func TestPointerCodecCheckType(t *testing.T) {
	c := PointerCodec(Int32Codec)
	if err := c.CheckType(IntType); err != nil {
		t.Fatalf("CheckType(int) = %v", err)
	}
	if err := c.CheckType(TextType); err == nil {
		t.Fatal("CheckType(text) accepted a text column for *int32")
	}
	if err := PointerCodec[int32](nil).CheckType(IntType); err == nil {
		t.Fatal("CheckType accepted a nil base codec")
	}
}
