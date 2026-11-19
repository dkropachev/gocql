// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// StreamStatement pairs CQL and typed parameters with a decoder that writes rows
// into caller-owned storage. Session.PrepareStream prepares it; streaming is
// always prepared. No buffered row decoder is required.
//
// The zero StreamStatement has no CQL or decoder, which preparation rejects
// with a ValidationError.
type StreamStatement[P, R any] struct {
	// Params encodes P. It may be nil when P is cql.EmptyParams.
	Params ParamEncoder[P]
	// Rows decodes into seeded caller storage and must not be nil.
	Rows StreamRowDecoder[R]
	CQL  string
	// Kind classifies ordinary and conditional statements for safe execution.
	Kind StatementKind
}

// StreamRowOf decodes streamed rows into struct R by column name, mapped like
// cql.RowOf but with the streaming decoders, which have their own Go types:
// text, ascii, blob and varint columns decode into []byte (blob by default;
// pick another with `cql:"name,stream=Text"`), and slices decode lists
// (`stream=Set` for sets, `stream=Vector` for vectors). A [][]byte field
// decodes a list of blobs, or with `stream=Text`, `stream=ASCII` or
// `stream=Varint` a list of those. Storage of []byte and
// slice fields is reused across rows. Every column needs a field.
//
// When R or *R has a CQLStreamColumns method, as cmd/cqlgen generates, its
// mapping is used; otherwise R is reflected once, here. Of the
// StructOption values, StreamRowOf applies WithStreamCodec and ignores
// WithCodec and ReuseBuffers.
func StreamRowOf[R any](opts ...StructOption[R]) (StreamRowDecoder[R], error) {
	var columns []StreamOutputField[R]
	if g, ok := any(new(R)).(interface{ CQLStreamColumns() []StreamOutputField[R] }); ok {
		columns = g.CQLStreamColumns()
	}
	return generatedStreamRow(columns, opts...)
}

// MustStreamRowOf is StreamRowOf that panics on error, for package-level variables.
func MustStreamRowOf[R any](opts ...StructOption[R]) StreamRowDecoder[R] {
	r, err := StreamRowOf(opts...)
	if err != nil {
		panic(err)
	}
	return r
}
