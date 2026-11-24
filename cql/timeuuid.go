// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/json"
	"errors"
	"time"
)

// TimeUUIDFromUUID checks that u has version 1, as required for a CQL timeuuid.
func TimeUUIDFromUUID(u UUID) (TimeUUID, error) {
	if u.Version() != 1 {
		return TimeUUID{}, errors.New("gocql: timeuuid must have version 1")
	}
	return TimeUUID(u), nil
}

// ParseTimeUUID parses a UUID and checks that it has version 1.
func ParseTimeUUID(input string) (TimeUUID, error) {
	u, err := ParseUUID(input)
	if err != nil {
		return TimeUUID{}, err
	}
	return TimeUUIDFromUUID(u)
}

// MustParseTimeUUID is ParseTimeUUID that panics on error.
func MustParseTimeUUID(input string) TimeUUID {
	u, err := ParseTimeUUID(input)
	if err != nil {
		panic(err)
	}
	return u
}

// String returns the UUID's canonical form; see UUID.String. It formats any
// value, the zero one included.
func (u TimeUUID) String() string { return UUID(u).String() }

// Bytes returns a copy of the UUID's 16 bytes.
func (u TimeUUID) Bytes() []byte { return UUID(u).Bytes() }

// IsZero reports whether u is the zero UUID.
func (u TimeUUID) IsZero() bool { return UUID(u).IsZero() }

// Variant returns the UUID's variant; see UUID.Variant.
func (u TimeUUID) Variant() int { return UUID(u).Variant() }

// Version returns the UUID's version, 1 for a valid TimeUUID.
func (u TimeUUID) Version() int { return UUID(u).Version() }

// Node returns the node (MAC address) field of the UUID, as TimeUUIDWith
// takes it, or the zero array if u is not version 1, as the zero TimeUUID is
// not.
func (u TimeUUID) Node() [6]byte {
	if u.Version() != 1 {
		return [6]byte{}
	}
	return [6]byte(u[10:])
}

// Clock returns the 14-bit clock sequence of the UUID, as TimeUUIDWith takes
// it, or zero if u is not version 1.
func (u TimeUUID) Clock() uint16 {
	if u.Version() != 1 {
		return 0
	}
	// The clock sequence is the lower 14 bits of u[8:10].
	return uint16(u[8]&0x3F)<<8 | uint16(u[9])
}

// Timestamp returns the 100-nanosecond intervals since 15 October 1582 the
// UUID records, or zero if u is not version 1.
func (u TimeUUID) Timestamp() int64 {
	if u.Version() != 1 {
		return 0
	}
	return int64(uint64(u[0])<<24|uint64(u[1])<<16|
		uint64(u[2])<<8|uint64(u[3])) +
		int64(uint64(u[4])<<40|uint64(u[5])<<32) +
		int64(uint64(u[6]&0x0F)<<56|uint64(u[7])<<48)
}

// Time returns the UUID's timestamp as a UTC time, or the zero time.Time if it
// is not version 1.
func (u TimeUUID) Time() time.Time {
	if u.Version() != 1 {
		return time.Time{}
	}
	t := u.Timestamp()
	sec := t / 1e7
	nsec := (t % 1e7) * 100
	return time.Unix(sec+timeBase, nsec).UTC()
}

// MarshalJSON uses the same canonical string representation as UUID, after
// checking version 1. String supports zero for diagnostics; serialization and
// CQL binding reject zero and other non-v1 values.
func (u TimeUUID) MarshalJSON() ([]byte, error) {
	if _, err := TimeUUIDFromUUID(UUID(u)); err != nil {
		return nil, err
	}
	return UUID(u).MarshalJSON()
}

// UnmarshalJSON accepts a UUID string only when it has version 1. On error the
// receiver is unchanged.
func (u *TimeUUID) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	value, err := ParseTimeUUID(text)
	if err != nil {
		return err
	}
	*u = value
	return nil
}

// MarshalText writes the canonical UUID string after checking version 1.
func (u TimeUUID) MarshalText() ([]byte, error) {
	if _, err := TimeUUIDFromUUID(UUID(u)); err != nil {
		return nil, err
	}
	return UUID(u).MarshalText()
}

// UnmarshalText accepts a version 1 UUID, leaving the receiver unchanged on error.
func (u *TimeUUID) UnmarshalText(data []byte) error {
	value, err := ParseTimeUUID(string(data))
	if err != nil {
		return err
	}
	*u = value
	return nil
}
