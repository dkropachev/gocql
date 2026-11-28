// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ParseDate parses a date in the YYYY-MM-DD form CQL accepts as a literal and
// Date.String prints. The year may have a sign and more than four digits,
// such as -0001-01-01 or +12345-06-07, for dates outside years 1 to 9999.
func ParseDate(s string) (Date, error) {
	year, month, day, ok := parseDateParts(s)
	if !ok {
		return 0, fmt.Errorf("gocql: invalid date %q; want YYYY-MM-DD", s)
	}
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if t.Day() != day || int(t.Month()) != month {
		return 0, fmt.Errorf("gocql: invalid date %q: no such day", s)
	}
	return DateFromTime(t)
}

func parseDateParts(s string) (year, month, day int, ok bool) {
	sign := 1
	rest := s
	switch {
	case strings.HasPrefix(rest, "-"):
		sign, rest = -1, rest[1:]
	case strings.HasPrefix(rest, "+"):
		rest = rest[1:]
	}
	y, rest, found := strings.Cut(rest, "-")
	if !found || len(y) < 4 || len(rest) != 5 || rest[2] != '-' {
		return 0, 0, 0, false
	}
	yy, err := strconv.Atoi(y)
	if err != nil || len(y) > 10 {
		return 0, 0, 0, false
	}
	mm, err1 := strconv.Atoi(rest[:2])
	dd, err2 := strconv.Atoi(rest[3:])
	if err1 != nil || err2 != nil || mm < 1 || mm > 12 || dd < 1 || dd > 31 {
		return 0, 0, 0, false
	}
	return sign * yy, mm, dd, true
}

// ParseTimeOfDay parses a time of day in the HH:MM:SS[.fffffffff] form CQL
// accepts as a literal and TimeOfDay.String prints, with up to nine
// fractional digits; HH:MM is accepted too.
func ParseTimeOfDay(s string) (TimeOfDay, error) {
	bad := func() (TimeOfDay, error) {
		return 0, fmt.Errorf("gocql: invalid time %q; want HH:MM:SS[.fffffffff]", s)
	}
	clock, fraction, hasFraction := strings.Cut(s, ".")
	parts := strings.Split(clock, ":")
	if len(parts) < 2 || len(parts) > 3 || hasFraction && len(parts) != 3 {
		return bad()
	}
	limits := [...]int64{24, 60, 60}
	var n int64
	for i, p := range parts {
		if len(p) != 2 {
			return bad()
		}
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil || v < 0 || v >= limits[i] {
			return bad()
		}
		n = n*60 + v
	}
	if len(parts) == 2 {
		n *= 60
	}
	n *= int64(time.Second)
	if hasFraction {
		if fraction == "" || len(fraction) > 9 {
			return bad()
		}
		f, err := strconv.ParseInt(fraction, 10, 64)
		if err != nil || f < 0 {
			return bad()
		}
		for range 9 - len(fraction) {
			f *= 10
		}
		n += f
	}
	return TimeOfDay(n), nil
}

// timestampLayouts are the forms ParseTimestamp accepts, after a trailing Z
// or zone offset is split off.
var timestampLayouts = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04",
	"2006-01-02 15:04",
	"2006-01-02",
}

// ParseTimestamp parses a timestamp in the forms CQL accepts as a literal:
// a date, optionally followed by T or a space and HH:MM[:SS[.fff]], and
// optionally by Z or a zone offset such as +0100 or +01:00. A timestamp
// without a zone is UTC, as Timestamp.String prints one. A fraction finer
// than a millisecond, which a timestamp cannot hold, is rejected rather than
// truncated.
func ParseTimestamp(s string) (Timestamp, error) {
	bad := func(reason string) (Timestamp, error) {
		return 0, fmt.Errorf("gocql: invalid timestamp %q: %s", s, reason)
	}
	body, loc := s, time.UTC
	if strings.HasSuffix(body, "Z") {
		body = body[:len(body)-1]
	} else if i := strings.LastIndexAny(body, "+-"); i > len("2006-01-02") {
		zone := strings.ReplaceAll(body[i+1:], ":", "")
		if len(zone) != 4 && len(zone) != 2 {
			return bad("invalid zone offset")
		}
		h, err1 := strconv.Atoi(zone[:2])
		m := 0
		var err2 error
		if len(zone) == 4 {
			m, err2 = strconv.Atoi(zone[2:])
		}
		if err1 != nil || err2 != nil || h > 23 || m > 59 {
			return bad("invalid zone offset")
		}
		offset := h*3600 + m*60
		if body[i] == '-' {
			offset = -offset
		}
		body, loc = body[:i], time.FixedZone("", offset)
	}
	for _, layout := range timestampLayouts {
		t, err := time.ParseInLocation(layout, body, loc)
		if err != nil {
			continue
		}
		if t.Nanosecond()%int(time.Millisecond) != 0 {
			return bad("finer than a millisecond")
		}
		return TimestampFromTime(t)
	}
	return bad("want YYYY-MM-DD[(T| )HH:MM[:SS[.fff]]][Z|(+|-)HH[:]MM]")
}

// ParseDuration parses a CQL duration literal in any of the forms CQL
// accepts: units, such as 1y2mo3d4h5m6s7ms8us9ns (µs for us, and upper
// case too); ISO 8601 designators, such as P1Y2M3DT4H5M6S or P2W; and the
// ISO 8601 alternative form, P0001-02-03T04:05:06. A leading minus negates
// the whole duration. Each component must fit Duration's fields.
func ParseDuration(s string) (Duration, error) {
	bad := func(reason string) (Duration, error) {
		return Duration{}, fmt.Errorf("gocql: invalid duration %q: %s", s, reason)
	}
	body := s
	negative := strings.HasPrefix(body, "-")
	if negative {
		body = body[1:]
	}
	if body == "" {
		return bad("empty")
	}
	var months, days, nanos int64
	add := func(dst *int64, n, scale int64) bool {
		if n != 0 && scale > math.MaxInt64/n {
			return false
		}
		v := n * scale
		if *dst > math.MaxInt64-v {
			return false
		}
		*dst += v
		return true
	}
	var err error
	switch {
	case body[0] == 'P' || body[0] == 'p':
		err = parseISODuration(body[1:], add, &months, &days, &nanos)
	default:
		err = parseUnitDuration(body, add, &months, &days, &nanos)
	}
	if err != nil {
		return bad(err.Error())
	}
	if months > math.MaxInt32 || days > math.MaxInt32 {
		return bad("months or days overflow int32")
	}
	if negative {
		months, days, nanos = -months, -days, -nanos
	}
	return Duration{Months: int32(months), Days: int32(days), Nanoseconds: nanos}, nil
}

var durationUnits = []struct {
	name  string
	field int // 0 months, 1 days, 2 nanoseconds
	scale int64
}{
	// Longer names first, so "mo" and "ms" match before "m".
	{"mo", 0, 1}, {"ms", 2, int64(time.Millisecond)}, {"us", 2, int64(time.Microsecond)},
	{"µs", 2, int64(time.Microsecond)}, {"ns", 2, 1},
	{"y", 0, 12}, {"w", 1, 7}, {"d", 1, 1}, {"h", 2, int64(time.Hour)},
	{"m", 2, int64(time.Minute)}, {"s", 2, int64(time.Second)},
}

func parseUnitDuration(body string, add func(*int64, int64, int64) bool, months, days, nanos *int64) error {
	fields := [3]*int64{months, days, nanos}
	for body != "" {
		i := 0
		for i < len(body) && body[i] >= '0' && body[i] <= '9' {
			i++
		}
		if i == 0 {
			return fmt.Errorf("expected a number at %q", body)
		}
		n, err := strconv.ParseInt(body[:i], 10, 64)
		if err != nil {
			return fmt.Errorf("number %s out of range", body[:i])
		}
		body = body[i:]
		lower := strings.ToLower(body)
		found := false
		for _, u := range durationUnits {
			if strings.HasPrefix(lower, u.name) {
				if !add(fields[u.field], n, u.scale) {
					return fmt.Errorf("overflow")
				}
				body, found = body[len(u.name):], true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown unit at %q", body)
		}
	}
	return nil
}

func parseISODuration(body string, add func(*int64, int64, int64) bool, months, days, nanos *int64) error {
	if body == "" {
		return fmt.Errorf("no components")
	}
	if strings.ContainsAny(body, "-:") {
		// The alternative form: YYYY-MM-DDThh:mm:ss.
		date, clock, ok := strings.Cut(body, "T")
		dp := strings.Split(date, "-")
		cp := strings.Split(clock, ":")
		if !ok || len(dp) != 3 || len(cp) != 3 {
			return fmt.Errorf("want PYYYY-MM-DDThh:mm:ss")
		}
		var v [6]int64
		for i, p := range append(dp, cp...) {
			n, err := strconv.ParseInt(p, 10, 64)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid component %q", p)
			}
			v[i] = n
		}
		if !add(months, v[0], 12) || !add(months, v[1], 1) || !add(days, v[2], 1) ||
			!add(nanos, v[3], int64(time.Hour)) || !add(nanos, v[4], int64(time.Minute)) || !add(nanos, v[5], int64(time.Second)) {
			return fmt.Errorf("overflow")
		}
		return nil
	}
	upper := strings.ToUpper(body)
	date, clock, hasClock := strings.Cut(upper, "T")
	if hasClock && clock == "" {
		return fmt.Errorf("T without time components")
	}
	parse := func(part string, units map[byte]struct {
		field *int64
		scale int64
	}) error {
		for part != "" {
			i := 0
			for i < len(part) && part[i] >= '0' && part[i] <= '9' {
				i++
			}
			if i == 0 || i == len(part) {
				return fmt.Errorf("invalid component at %q", part)
			}
			n, err := strconv.ParseInt(part[:i], 10, 64)
			if err != nil {
				return fmt.Errorf("number %s out of range", part[:i])
			}
			u, ok := units[part[i]]
			if !ok {
				return fmt.Errorf("unknown designator %q", part[i])
			}
			if !add(u.field, n, u.scale) {
				return fmt.Errorf("overflow")
			}
			part = part[i+1:]
		}
		return nil
	}
	type unit = struct {
		field *int64
		scale int64
	}
	if err := parse(date, map[byte]unit{'Y': {months, 12}, 'M': {months, 1}, 'W': {days, 7}, 'D': {days, 1}}); err != nil {
		return err
	}
	return parse(clock, map[byte]unit{'H': {nanos, int64(time.Hour)}, 'M': {nanos, int64(time.Minute)}, 'S': {nanos, int64(time.Second)}})
}
