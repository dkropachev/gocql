// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"math"
	"time"
)

// Date counts calendar days relative to 1970-01-01.
type Date int32

// TimeOfDay counts nanoseconds since midnight, in [0, 24 hours).
type TimeOfDay int64

// Timestamp counts milliseconds relative to the Unix epoch.
type Timestamp int64

// DateFromTime converts a time to a CQL date, rejecting dates outside the supported range.
func DateFromTime(t time.Time) (Date, error) {
	y, m, d := t.Date()
	// Guard Unix against overflow for dates outside the CQL calendar's range.
	if y < -6_000_000 || y > 6_000_000 {
		return 0, errors.New("gocql: calendar date outside CQL date range")
	}
	days := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, errors.New("gocql: calendar date outside CQL date range")
	}
	return Date(days), nil
}

// Time returns midnight UTC of the date. Its error is always nil.
func (d Date) Time() (time.Time, error) { return time.Unix(int64(d)*86400, 0).UTC(), nil }

// TimestampFromTime converts a time to a CQL millisecond timestamp, rejecting overflow.
func TimestampFromTime(t time.Time) (Timestamp, error) {
	y := t.Year()
	if y < -300_000_000 || y > 300_000_000 {
		return 0, errors.New("gocql: timestamp outside int64 millisecond range")
	}
	seconds := t.Unix()
	fraction := int64(t.Nanosecond() / 1_000_000)
	const minSecond = math.MinInt64/1000 - 1
	const minFraction = 1000 + math.MinInt64%1000
	if seconds < minSecond || seconds > math.MaxInt64/1000 || (seconds == minSecond && fraction < minFraction) || (seconds == math.MaxInt64/1000 && fraction > math.MaxInt64%1000) {
		return 0, errors.New("gocql: timestamp outside int64 millisecond range")
	}
	if seconds == minSecond {
		return Timestamp(math.MinInt64 + (fraction - minFraction)), nil
	}
	return Timestamp(seconds*1000 + fraction), nil
}

// Time returns the timestamp as a UTC time. Its error is always nil.
func (t Timestamp) Time() (time.Time, error) {
	return time.Unix(int64(t)/1000, (int64(t)%1000)*1_000_000).UTC(), nil
}
