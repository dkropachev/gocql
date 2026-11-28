// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"testing"
	"time"
)

// TestInterfaceOnlyMethods runs the exported methods the driver reaches only
// through an interface it never calls them by: CheckType of encoders that
// WriteParam writes without checking, and Reset of compiled stream values,
// which the row plan clears itself.
func TestInterfaceOnlyMethods(t *testing.T) {
	intType := rowTestType(t, KindInt)
	textType := rowTestType(t, KindText)

	t.Run("CheckType", func(t *testing.T) {
		codec := ConvertCodec(Int32Codec,
			func(v int64) (int32, error) { return int32(v), nil },
			func(v int32) (int64, error) { return int64(v), nil })
		if err := codec.CheckType(intType); err != nil {
			t.Fatalf("ConvertCodec on int: %v", err)
		}
		if err := codec.CheckType(textType); err == nil {
			t.Fatal("ConvertCodec on text: no error")
		}
		if err := (unsetEncoder{}).CheckType(textType); err != nil {
			t.Fatalf("unsetEncoder: %v", err)
		}
		enc := encoderOf(deriveCodec(Int32Codec).codec)
		if err := enc.CheckType(intType); err != nil {
			t.Fatalf("derived encoder on int: %v", err)
		}
		if err := enc.CheckType(textType); err == nil {
			t.Fatal("derived encoder on text: no error")
		}
	})

	t.Run("StreamReset", func(t *testing.T) {
		s := "x"
		StreamString.Reset(&s)
		if s != "" {
			t.Fatalf("StreamString.Reset: %q", s)
		}
		at := time.Now()
		StreamTimestampTime.Reset(&at)
		if !at.IsZero() {
			t.Fatalf("StreamTimestampTime.Reset: %v", at)
		}
		d := time.Second
		StreamTimeDuration.Reset(&d)
		if d != 0 {
			t.Fatalf("StreamTimeDuration.Reset: %v", d)
		}
		dec, err := ParseDecimal("1.5")
		if err != nil {
			t.Fatal(err)
		}
		StreamDecimalValue.Reset(&dec)
		if dec.String() != (Decimal{}).String() {
			t.Fatalf("StreamDecimalValue.Reset: %v", dec)
		}
		n := new(int32)
		StreamPointer(StreamInt32).Reset(&n)
		if n != nil {
			t.Fatalf("StreamPointer.Reset: %v", n)
		}
		m := map[string]int32{"k": 1}
		StreamGoMap(StreamString, StreamInt32).Reset(&m)
		if len(m) != 0 {
			t.Fatalf("StreamGoMap.Reset: %v", m)
		}

		// The compiled plans clear the same way.
		cs, err := StreamString.Compile(textType)
		if err != nil {
			t.Fatal(err)
		}
		s = "x"
		cs.Reset(&s)
		ct, err := StreamTimestampTime.Compile(rowTestType(t, KindTimestamp))
		if err != nil {
			t.Fatal(err)
		}
		at = time.Now()
		ct.Reset(&at)
		cd, err := StreamTimeDuration.Compile(rowTestType(t, KindDuration))
		if err != nil {
			t.Fatal(err)
		}
		d = time.Second
		cd.Reset(&d)
		cdec, err := StreamDecimalValue.Compile(rowTestType(t, KindDecimal))
		if err != nil {
			t.Fatal(err)
		}
		if dec, err = ParseDecimal("1.5"); err != nil {
			t.Fatal(err)
		}
		cdec.Reset(&dec)
		cp, err := StreamPointer(StreamInt32).Compile(intType)
		if err != nil {
			t.Fatal(err)
		}
		n = new(int32)
		cp.Reset(&n)
		mt, err := MapType(textType, intType)
		if err != nil {
			t.Fatal(err)
		}
		cm, err := StreamGoMap(StreamString, StreamInt32).Compile(mt)
		if err != nil {
			t.Fatal(err)
		}
		m = map[string]int32{"k": 1}
		cm.Reset(&m)
		if s != "" || !at.IsZero() || d != 0 || n != nil || len(m) != 0 {
			t.Fatalf("compiled Reset left %q %v %v %v %v", s, at, d, n, m)
		}
	})

	t.Run("StreamDecode", func(t *testing.T) {
		durType := rowTestType(t, KindDuration)
		enc, err := EncodeValue(durType, DurationCodec, Duration{Nanoseconds: int64(1500 * time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		body, err := enc.Bytes().Copy()
		if err != nil {
			t.Fatal(err)
		}
		var d time.Duration
		if err := StreamDecodeFixture(t, durType, StreamTimeDuration, streamCellBytes(body), &d, 1); err != nil || d != 1500*time.Millisecond {
			t.Fatalf("StreamTimeDuration: %v, %v", d, err)
		}
		decType := rowTestType(t, KindDecimal)
		var dec Decimal
		// scale 2, unscaled 123: 1.23
		if err := StreamDecodeFixture(t, decType, StreamDecimalValue, streamCellBytes([]byte{0, 0, 0, 2, 123}), &dec, 1); err != nil || dec.String() != "1.23" {
			t.Fatalf("StreamDecimalValue: %v, %v", dec, err)
		}
	})
}
