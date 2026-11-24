// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"strings"
)

// Statement pairs CQL text with a typed parameter encoder and row decoder.
// Nil Params or Rows means none when its type is EmptyParams or EmptyRow.
// BindMarkers supplies parameter metadata for unprepared execution; Prepare
// takes that metadata from the server. Kind classifies conditional statements,
// and Idempotent marks a statement safe to run more than once.
//
// The zero Statement has no CQL, which preparation and execution reject with a
// ValidationError.
type Statement[P, R any] struct {
	// CQL is the statement text sent to the server.
	CQL string
	// Params encodes P. It may be nil when P is EmptyParams.
	Params ParamEncoder[P]
	// Rows decodes R. It may be nil when R is EmptyRow.
	Rows RowDecoder[R]
	// BindMarkers supplies parameter metadata for unprepared execution, which
	// rejects a statement with parameters (P other than EmptyParams) that has
	// none, unless Params is DynamicParams: its markers then take the types
	// of the values. Preparation obtains this metadata from the server
	// instead.
	BindMarkers Columns
	// Kind classifies ordinary and conditional statements for safe execution.
	// A statement left UnknownStatement whose R is EmptyRow executes as an
	// OrdinaryStatement, since it reads no rows; see UnknownStatement.
	Kind StatementKind
	// Idempotent marks the statement safe to run more than once: speculative
	// execution requires it, and retry.Safe moves a request that may have
	// executed to another host only when it is set. Prepared statements,
	// their queries and batch members carry it; a batch is idempotent when
	// every member is. exec.Options.Idempotent overrides it for one
	// execution.
	Idempotent bool
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
