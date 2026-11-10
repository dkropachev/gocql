// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"encoding/json"
	"errors"
	"time"
)

// TimeUUIDFromUUID checks that u has version 1. As with the wire codec, the
// version is checked independently of the UUID variant.
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

func (u TimeUUID) String() string   { return UUID(u).String() }
func (u TimeUUID) Bytes() []byte    { return UUID(u).Bytes() }
func (u TimeUUID) IsEmpty() bool    { return UUID(u).IsEmpty() }
func (u TimeUUID) Variant() int     { return UUID(u).Variant() }
func (u TimeUUID) Version() int     { return UUID(u).Version() }
func (u TimeUUID) Node() []byte     { return UUID(u).Node() }
func (u TimeUUID) Clock() uint32    { return UUID(u).Clock() }
func (u TimeUUID) Timestamp() int64 { return UUID(u).Timestamp() }
func (u TimeUUID) Time() time.Time  { return UUID(u).Time() }

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
