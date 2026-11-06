// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"strings"

	binding "github.com/scylladb/gocql/v2/internal/binding"
	codec "github.com/scylladb/gocql/v2/internal/codec"
)

// Statement is CQL text with its typed parameter encoder and row decoder.
type Statement[P any, R any] = binding.Statement[P, R]

var OwnedBlobCodec = codec.OwnedBlobCodec

func RowInto[R any](reset func(*R), fields ...OutputField[R]) RowDecoder[R] {
	return codec.RowInto(reset, fields...)
}

// ParseConsistency parses a consistency name, ignoring case. It panics on an
// unknown name; use ParseConsistencyWrapper to get an error instead.
func ParseConsistency(s string) Consistency {
	var c Consistency
	if err := c.UnmarshalText([]byte(strings.ToUpper(s))); err != nil {
		panic(err)
	}
	return c
}

// ParseConsistencyWrapper parses a consistency name, ignoring case.
func ParseConsistencyWrapper(s string) (consistency Consistency, err error) {
	err = consistency.UnmarshalText([]byte(strings.ToUpper(s)))
	return
}
