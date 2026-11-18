// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "fmt"

// BatchKind selects native protocol batch semantics.
type BatchKind byte

const (
	LoggedBatch   BatchKind = 0
	UnloggedBatch BatchKind = 1
	CounterBatch  BatchKind = 2
)

// String returns the batch kind name, or BatchKind(n) for a value it does not define.
func (k BatchKind) String() string {
	switch k {
	case LoggedBatch:
		return "logged"
	case UnloggedBatch:
		return "unlogged"
	case CounterBatch:
		return "counter"
	default:
		return fmt.Sprintf("BatchKind(%d)", uint8(k))
	}
}
