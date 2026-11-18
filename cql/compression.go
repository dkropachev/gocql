// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "fmt"

type Compression uint8

const (
	NoCompression Compression = iota
	LZ4Compression
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
