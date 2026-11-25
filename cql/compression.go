// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
	"strings"
)

// Compression is the frame compression a connection negotiates. The zero
// Compression is NoCompression. Later releases may add values, so a switch over
// them needs a default case.
type Compression uint8

// Frame compression algorithms.
const (
	// NoCompression sends frames uncompressed.
	NoCompression Compression = iota

	// LZ4Compression compresses frames with LZ4.
	LZ4Compression

	// SnappyCompression compresses frames with Snappy.
	SnappyCompression
)

// String returns the compression name, or Compression(n) for a value it does
// not define.
func (c Compression) String() string {
	switch c {
	case NoCompression:
		return "none"
	case LZ4Compression:
		return "lz4"
	case SnappyCompression:
		return "snappy"
	default:
		return fmt.Sprintf("Compression(%d)", uint8(c))
	}
}

// MarshalText implements encoding.TextMarshaler with the name String
// returns, and returns an error for a value String does not name.
func (c Compression) MarshalText() ([]byte, error) {
	if c > SnappyCompression {
		return nil, fmt.Errorf("invalid compression %v", c)
	}
	return []byte(c.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler. It accepts the names
// String returns, "none", "lz4" and "snappy", in any case, and returns an
// error for any other text.
func (c *Compression) UnmarshalText(text []byte) error {
	switch strings.ToLower(string(text)) {
	case "none":
		*c = NoCompression
	case "lz4":
		*c = LZ4Compression
	case "snappy":
		*c = SnappyCompression
	default:
		return fmt.Errorf("invalid compression %q; want none, lz4 or snappy", string(text))
	}
	return nil
}

// ParseCompression parses a compression name, such as "lz4", ignoring
// case. It is Compression.UnmarshalText for a string.
func ParseCompression(s string) (Compression, error) {
	var c Compression
	err := c.UnmarshalText([]byte(s))
	return c, err
}
