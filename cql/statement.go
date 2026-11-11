// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"strings"

	codec "github.com/scylladb/gocql/v2/internal/codec"
)

// Statement pairs CQL text with a typed parameter encoder and row decoder.
// Nil Params or Rows means none when its type is EmptyParams or EmptyRow.
// BindMarkers supplies parameter metadata for unprepared execution; Prepare
// takes that metadata from the server. Kind classifies conditional statements.
type Statement[P, R any] struct {
	// CQL is the statement text sent to the server.
	CQL string
	// Params encodes P. It may be nil when P is EmptyParams.
	Params ParamEncoder[P]
	// Rows decodes R. It may be nil when R is EmptyRow.
	Rows RowDecoder[R]
	// BindMarkers supplies parameter metadata for unprepared execution.
	// Preparation obtains this metadata from the server instead.
	BindMarkers Columns
	// Kind classifies ordinary and conditional statements for safe execution.
	Kind StatementKind
}

// StreamStatement pairs CQL and typed parameters with a decoder that writes
// rows into caller-owned storage. Session.PrepareStream prepares it; streaming
// is always prepared. No buffered row decoder is required.
type StreamStatement[P, R any] struct {
	// Params encodes P. It may be nil when P is EmptyParams.
	Params ParamEncoder[P]
	// Rows decodes into seeded caller storage and must not be nil.
	Rows StreamRowDecoder[R]
	CQL  string
	// Kind classifies ordinary and conditional statements for safe execution.
	Kind StatementKind
}

// OwnedBlobCodec encodes []byte and decodes into caller-owned storage. RowInto
// and ReuseBuffers preserve a seeded slice when its capacity is sufficient.
// The decoded bytes survive response release, including One returning.
var OwnedBlobCodec = codec.OwnedBlobCodec

// RowInto maps columns into the actual destination after calling reset. The
// callback clears application state and may preserve owned slices for reuse.
// Borrowed fields follow the execution's normal lifetime; One detaches them.
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
