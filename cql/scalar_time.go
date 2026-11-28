// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"
)

const nanosecondsPerDay int64 = 24 * 60 * 60 * 1_000_000_000

// DateCodec maps date to Date.
var DateCodec Codec[Date] = scalarCodec[Date]{kind: KindDate,
	encode: func(w *ValueWriter, v Date) error { return w.Uint32(uint32(v) ^ 0x80000000) },
	decode: func(r ValueReader) (Date, error) {
		if v, ok := fixedValue(r, 4); ok {
			return Date(int32(uint32(v) ^ 0x80000000)), nil
		}
		c, e := fixedCursor(r, 4)
		if e != nil {
			return 0, e
		}
		v, e := c.Uint32()
		return Date(int32(v ^ 0x80000000)), e
	},
}

// TimeOfDayCodec maps time to TimeOfDay, rejecting a value outside one day.
var TimeOfDayCodec Codec[TimeOfDay] = scalarCodec[TimeOfDay]{kind: KindTime,
	encode: func(w *ValueWriter, v TimeOfDay) error {
		if int64(v) < 0 || int64(v) >= nanosecondsPerDay {
			return errors.New("gocql: time outside one day")
		}
		return w.Uint64(uint64(v))
	},
	decode: func(r ValueReader) (TimeOfDay, error) {
		v, ok := fixedValue(r, 8)
		if !ok {
			c, e := fixedCursor(r, 8)
			if e != nil {
				return 0, e
			}
			if v, e = c.Uint64(); e != nil {
				return 0, e
			}
		}
		if v >= uint64(nanosecondsPerDay) {
			return 0, errors.New("gocql: time outside one day")
		}
		return TimeOfDay(v), nil
	},
}

// TimestampCodec maps timestamp to Timestamp.
var TimestampCodec Codec[Timestamp] = integerCodec[Timestamp](KindTimestamp, 8)

// TimestampTimeCodec maps timestamp to time.Time, the default codec of
// time.Time struct fields. It decodes a UTC time and encodes the time's
// instant truncated to the millisecond, rejecting one outside the int64
// millisecond range. It rejects the zero time.Time, which is almost always
// a field the application forgot to set rather than the year 1: bind a
// Nullable[time.Time] or a nil *time.Time for NULL.
var TimestampTimeCodec Codec[time.Time] = scalarCodec[time.Time]{kind: KindTimestamp,
	encode: func(w *ValueWriter, v time.Time) error {
		if v.IsZero() {
			return errZeroTime
		}
		ts, err := TimestampFromTime(v)
		if err != nil {
			return err
		}
		return w.Uint64(uint64(ts))
	},
	decode: func(r ValueReader) (time.Time, error) {
		v, ok := fixedValue(r, 8)
		if !ok {
			c, e := fixedCursor(r, 8)
			if e != nil {
				return time.Time{}, e
			}
			if v, e = c.Uint64(); e != nil {
				return time.Time{}, e
			}
		}
		return time.UnixMilli(int64(v)).UTC(), nil
	},
}

// errZeroTime is TimestampTimeCodec's error for the zero time.Time.
var errZeroTime = errors.New("gocql: zero time.Time; bind a cql.Nullable[time.Time] or a nil *time.Time for NULL")

// TimeDurationCodec maps duration to time.Duration, the default codec of
// time.Duration struct fields, which therefore map a CQL duration and not a
// bigint. It encodes the nanoseconds with zero months and days, and rejects
// decoding a duration with months or days, whose length in time depends on
// the calendar; decode those into Duration.
var TimeDurationCodec Codec[time.Duration] = scalarCodec[time.Duration]{kind: KindDuration,
	encode: func(w *ValueWriter, v time.Duration) error {
		return encodeDuration(w, Duration{Nanoseconds: int64(v)})
	},
	decode: func(r ValueReader) (time.Duration, error) {
		d, err := decodeDuration(r)
		if err != nil {
			return 0, err
		}
		if d.Months != 0 || d.Days != 0 {
			return 0, fmt.Errorf("gocql: duration %s has months or days, which time.Duration cannot hold; decode it into cql.Duration", d)
		}
		return time.Duration(d.Nanoseconds), nil
	},
}

// DurationCodec maps duration to Duration.
var DurationCodec Codec[Duration] = scalarCodec[Duration]{kind: KindDuration, encode: encodeDuration, decode: decodeDuration}

func durationSignsValid(v Duration) bool {
	hasNegative := v.Months < 0 || v.Days < 0 || v.Nanoseconds < 0
	hasPositive := v.Months > 0 || v.Days > 0 || v.Nanoseconds > 0
	return !(hasNegative && hasPositive)
}
func encodeDuration(w *ValueWriter, v Duration) error {
	if !durationSignsValid(v) {
		return errors.New("gocql: duration components must have compatible signs")
	}
	values := [3]uint64{(uint64(int64(v.Months)) << 1) ^ uint64(int64(v.Months)>>63), (uint64(int64(v.Days)) << 1) ^ uint64(int64(v.Days)>>63), (uint64(v.Nanoseconds) << 1) ^ uint64(v.Nanoseconds>>63)}
	size := 0
	for _, v := range values {
		if v < 128 {
			size++
		} else {
			size += 1 + min(8, (bits.Len64(v)-1)/7)
		}
	}
	data, e := w.scalarBytes(size)
	if e != nil {
		return e
	}
	at := 0
	for _, v := range values {
		if v < 128 {
			data[at] = byte(v)
			at++
			continue
		}
		extra := min(8, (bits.Len64(v)-1)/7)
		if extra == 8 {
			data[at] = 0xff
		} else {
			data[at] = byte(0xff<<uint(8-extra)) | byte(v>>uint(extra*8))
		}
		at++
		for i := extra - 1; i >= 0; i-- {
			data[at] = byte(v >> uint(i*8))
			at++
		}
	}
	return nil
}
func scalarWriteUnsignedVInt(w *ValueWriter, v uint64) error {
	if v < 128 {
		return w.Uint8(uint8(v))
	}
	extra := (bits.Len64(v) - 1) / 7
	if extra > 8 {
		extra = 8
	}
	if extra == 8 {
		if err := w.Uint8(0xff); err != nil {
			return err
		}
	} else {
		prefix := byte(0xff << uint(8-extra))
		if err := w.Uint8(prefix | byte(v>>uint(extra*8))); err != nil {
			return err
		}
	}
	for i := extra - 1; i >= 0; i-- {
		if err := w.Uint8(byte(v >> uint(i*8))); err != nil {
			return err
		}
	}
	return nil
}

// scalarVInt owns the native scalar conversion shared by buffered and streaming
// readers. I/O-specific loops feed bytes without allocating cursor adapters.
type scalarVInt struct {
	remaining int
	value     uint64
}

func newScalarVInt(first byte) scalarVInt {
	extra := bits.LeadingZeros8(^first)
	return scalarVInt{remaining: extra, value: uint64(first & byte(0xff>>uint(extra)))}
}
func (v *scalarVInt) add(b byte)   { v.value = v.value<<8 | uint64(b); v.remaining-- }
func (v scalarVInt) signed() int64 { return int64(v.value>>1) ^ -int64(v.value&1) }
func scalarReadUnsignedVInt(c *ValueCursor) (uint64, error) {
	first, err := c.Uint8()
	if err != nil {
		return 0, err
	}
	v := newScalarVInt(first)
	for v.remaining > 0 {
		next, err := c.Uint8()
		if err != nil {
			return 0, err
		}
		v.add(next)
	}
	return v.value, nil
}
func readSignedVInt(c *ValueCursor) (int64, error) {
	value, err := scalarReadUnsignedVInt(c)
	return int64(value>>1) ^ -int64(value&1), err
}
func decodeDuration(r ValueReader) (Duration, error) {
	c := r.cursorValue()
	months, e := readSignedVInt(&c)
	if e != nil {
		return Duration{}, e
	}
	days, e := readSignedVInt(&c)
	if e != nil {
		return Duration{}, e
	}
	nanos, e := readSignedVInt(&c)
	if e != nil {
		return Duration{}, e
	}
	if e := c.RequireEnd(); e != nil {
		return Duration{}, e
	}
	return durationFromComponents(months, days, nanos)
}

// durationFromComponents is shared by buffered and incremental decoding.
func durationFromComponents(months, days, nanos int64) (Duration, error) {
	if months < math.MinInt32 || months > math.MaxInt32 || days < math.MinInt32 || days > math.MaxInt32 {
		return Duration{}, errors.New("gocql: duration months or days overflow int32")
	}
	v := Duration{Months: int32(months), Days: int32(days), Nanoseconds: nanos}
	if !durationSignsValid(v) {
		return Duration{}, errors.New("gocql: duration components must have compatible signs")
	}
	return v, nil
}
