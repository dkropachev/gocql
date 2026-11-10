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

// ParseConsistency parses a consistency name, ignoring case.
func ParseConsistency(s string) (Consistency, error) {
	var consistency Consistency
	err := consistency.UnmarshalText([]byte(strings.ToUpper(s)))
	return consistency, err
}

// MustParseConsistency is ParseConsistency that panics on error.
func MustParseConsistency(s string) Consistency {
	consistency, err := ParseConsistency(s)
	if err != nil {
		panic(err)
	}
	return consistency
}

// Command returns a statement with no bind markers and no result columns,
// such as DDL or a write with literal values.
func Command(text string) Statement[EmptyParams, EmptyRow] {
	return Statement[EmptyParams, EmptyRow]{CQL: text, Params: EmptyParamsEncoder, Rows: EmptyRowDecoder}
}
