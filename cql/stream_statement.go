// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// StreamStatement pairs CQL and typed parameters with a decoder that writes rows
// into caller-owned storage. Session.PrepareStream prepares it; streaming is
// always prepared. No buffered row decoder is required.
//
// Its fields are ordered for memory layout, not as Statement's are.
//
// The zero StreamStatement has no CQL or decoder, which preparation rejects
// with a ValidationError.
type StreamStatement[P, R any] struct {
	// Params encodes P. It may be nil when P is cql.EmptyParams.
	Params ParamEncoder[P]
	// Rows decodes into seeded caller storage and must not be nil.
	Rows StreamRowDecoder[R]
	// CQL is the statement's text.
	CQL string
	// Kind classifies ordinary and conditional statements for safe execution.
	// The driver infers an UnknownStatement's kind from the CQL, as
	// Statement.Kind describes.
	Kind StatementKind
	// Idempotent marks the statement safe to run more than once, as
	// Statement.Idempotent does.
	Idempotent bool
}

// StreamRowOf decodes streamed rows into struct R by column name, mapped like
// cql.RowOf but with the streaming decoders, which have their own Go types:
// text, ascii, blob and varint columns decode into []byte (blob by default;
// pick another with `cql:"name,stream=Text"`), and slices decode a list, a
// set or a vector, whichever the column is (`stream=List`, `stream=Set` or
// `stream=Vector` requires one). A named type maps like its underlying type,
// also inside a slice or a Nullable.
// A [][]byte field decodes a sequence of blobs, or with `stream=Text` or
// `stream=Varint` one of those. Storage of []byte and
// slice fields is reused across rows. A string, time.Time, time.Duration,
// Decimal or pointer field decodes with an allocating decoder (StreamString,
// StreamTimestampTime, StreamTimeDuration, StreamDecimalValue, StreamPointer),
// a new value for each row; a map field needs WithStreamDecoder, as of
// StreamGoMap. Every column needs a field, unless IgnoreExtraFields is given.
// A NULL cell fails a field that cannot hold it, with an error matching
// ErrNull, unless the field's tag has nullzero or NullAsZero is given.
//
// When R or *R has a CQLStreamColumns method, as cmd/cqlgen generates, its
// mapping is used; otherwise R is reflected once, here. StreamRowOf applies
// WithStreamDecoder, IgnoreExtraFields and NullAsZero, and rejects other
// options.
func StreamRowOf[R any](opts ...StructOption) (StreamRowDecoder[R], error) {
	var columns []StreamOutputField[R]
	if g, ok := any(new(R)).(interface{ CQLStreamColumns() []StreamOutputField[R] }); ok {
		columns = g.CQLStreamColumns()
	}
	return generatedStreamRow(columns, opts...)
}

// MustStreamRowOf is StreamRowOf that panics on error, for package-level variables.
func MustStreamRowOf[R any](opts ...StructOption) StreamRowDecoder[R] {
	r, err := StreamRowOf[R](opts...)
	if err != nil {
		panic(err)
	}
	return r
}
