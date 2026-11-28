// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"time"
)

// The value types implement encoding.TextMarshaler and TextUnmarshaler with
// their CQL literal forms, String and the matching Parse function, so that
// encoding/json and other text encodings write a date as "2026-01-02"
// rather than its day number.

// MarshalText returns the date as String does.
func (d Date) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalText parses the form ParseDate accepts.
func (d *Date) UnmarshalText(text []byte) error {
	v, err := ParseDate(string(text))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// MarshalText returns the timestamp as String does.
func (t Timestamp) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// UnmarshalText parses the forms ParseTimestamp accepts.
func (t *Timestamp) UnmarshalText(text []byte) error {
	v, err := ParseTimestamp(string(text))
	if err != nil {
		return err
	}
	*t = v
	return nil
}

// MarshalText returns the time as String does. It fails for a value outside
// one day, which String prints as TimeOfDay(n).
func (t TimeOfDay) MarshalText() ([]byte, error) {
	if t < 0 || int64(t) >= nanosecondsPerDay {
		return nil, typeValidation("time", "%d is outside one day", int64(t))
	}
	return []byte(t.String()), nil
}

// UnmarshalText parses the form ParseTimeOfDay accepts.
func (t *TimeOfDay) UnmarshalText(text []byte) error {
	v, err := ParseTimeOfDay(string(text))
	if err != nil {
		return err
	}
	*t = v
	return nil
}

// MarshalText returns the duration as String does. It fails for components
// with mixed signs, which the server rejects and String prints as
// {months days ns}.
func (d Duration) MarshalText() ([]byte, error) {
	if !durationSignsValid(d) {
		return nil, typeValidation("duration", "components %d, %d and %d have mixed signs", d.Months, d.Days, d.Nanoseconds)
	}
	return []byte(d.String()), nil
}

// UnmarshalText parses the forms ParseDuration accepts.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// TimeDuration returns d as a time.Duration, as TimeDurationCodec decodes
// it. It fails for a duration with months or days, whose length in time
// depends on the calendar.
func (d Duration) TimeDuration() (time.Duration, error) {
	if d.Months != 0 || d.Days != 0 {
		return 0, fmt.Errorf("gocql: duration %s has months or days, which time.Duration cannot hold", d)
	}
	return time.Duration(d.Nanoseconds), nil
}

// DurationFromTime returns d as a Duration of nanoseconds, with zero months
// and days, as TimeDurationCodec encodes it.
func DurationFromTime(d time.Duration) Duration { return Duration{Nanoseconds: int64(d)} }
