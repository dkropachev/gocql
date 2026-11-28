// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// Statement pairs CQL text with a typed parameter encoder and row decoder.
// Nil Params or Rows means none when its type is EmptyParams or EmptyRow.
// NewStatement, Select and Write build one with its type arguments inferred.
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
	// The driver infers an UnknownStatement's kind from Rows and the CQL;
	// see UnknownStatement.
	Kind StatementKind
	// Idempotent marks the statement safe to run more than once: speculative
	// execution requires it, and retry.Safe moves a request that may have
	// executed to another host only when it is set. Prepared statements,
	// their queries and batch members carry it; a batch is idempotent when
	// every member is. A profile with Idempotent set
	// (config.ProfileBuilder.WithIdempotent) marks every statement it runs
	// idempotent, and execopt.Options.Idempotent overrides both for one
	// execution.
	Idempotent bool
}

// NoParams encodes the parameters of a statement without bind markers. Pass
// it to NewStatement, Select or Write, where a nil encoder leaves P without a
// type to infer; a Statement literal may leave Params nil instead. The driver
// does not read this variable, so reassigning it changes only code that does.
var NoParams ParamEncoder[EmptyParams] = emptyParams

// NewStatement returns a statement of text with params and rows, its type
// arguments inferred from them. Its Kind is UnknownStatement and it is not
// Idempotent; set either on the result.
func NewStatement[P, R any](text string, params ParamEncoder[P], rows RowDecoder[R]) Statement[P, R] {
	return Statement[P, R]{CQL: text, Params: params, Rows: rows}
}

// Select returns a statement that reads rows, such as a SELECT, with params
// and rows, its type arguments inferred from them. A read runs as often as it
// is retried without changing anything, so it is an OrdinaryStatement and
// Idempotent. A conditional statement, one whose CQL is an INSERT, UPDATE,
// DELETE or BATCH with an IF or whose rows is a CASRows decoder, declared
// that way is rejected before it is sent.
func Select[P, R any](text string, params ParamEncoder[P], rows RowDecoder[R]) Statement[P, R] {
	return Statement[P, R]{CQL: text, Params: params, Rows: rows, Kind: OrdinaryStatement, Idempotent: true}
}

// Write returns a statement that returns no rows, such as an INSERT, UPDATE
// or DELETE, with params, P inferred from it. Its Kind is UnknownStatement,
// which the driver resolves from the CQL, and it is not Idempotent; set
// Idempotent on the result when running it twice is safe.
func Write[P any](text string, params ParamEncoder[P]) Statement[P, EmptyRow] {
	return Statement[P, EmptyRow]{CQL: text, Params: params, Rows: emptyRows}
}

// Command returns a statement with no bind markers and no result columns,
// such as DDL or a write with literal values.
func Command(text string) Statement[EmptyParams, EmptyRow] {
	return Statement[EmptyParams, EmptyRow]{CQL: text, Params: emptyParams, Rows: emptyRows}
}
