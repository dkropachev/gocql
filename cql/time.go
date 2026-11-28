// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Date counts calendar days relative to 1970-01-01.
type Date int32

// TimeOfDay counts nanoseconds since midnight, in [0, 24 hours).
type TimeOfDay int64

// Timestamp counts milliseconds relative to the Unix epoch.
type Timestamp int64

// DateFromTime converts a time to a CQL date: the calendar date of t in t's
// own location, as t.Date returns it, not of the instant in UTC. Convert t
// first, with t.UTC() or t.In(loc), for the date in another location. It
// rejects a date outside the CQL date range.
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

// Time returns midnight UTC of the date.
func (d Date) Time() time.Time { return time.Unix(int64(d)*86400, 0).UTC() }

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

// Time returns the timestamp as a UTC time.
func (t Timestamp) Time() time.Time { return time.UnixMilli(int64(t)).UTC() }

// String returns the date as YYYY-MM-DD, the form CQL accepts as a literal.
func (d Date) String() string { return d.Time().Format("2006-01-02") }

// String returns the timestamp in RFC 3339 form with milliseconds, in UTC,
// such as 2026-01-02T15:04:05.123Z.
func (t Timestamp) String() string { return t.Time().Format("2006-01-02T15:04:05.000Z07:00") }

// String returns the time as HH:MM:SS.nnnnnnnnn, the form CQL accepts as a
// literal, or TimeOfDay(n) for a value outside one day.
func (t TimeOfDay) String() string {
	n := int64(t)
	if n < 0 || n >= nanosecondsPerDay {
		return fmt.Sprintf("TimeOfDay(%d)", n)
	}
	const second = int64(time.Second)
	return fmt.Sprintf("%02d:%02d:%02d.%09d", n/(3600*second), n/(60*second)%60, n/second%60, n%second)
}
