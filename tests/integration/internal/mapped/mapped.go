// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package mapped holds structs whose mappings cmd/cqlgen generates, for the
// struct mapping integration tests to run against a cluster.
package mapped

import "github.com/scylladb/gocql/v2/cql"

//go:generate go run github.com/scylladb/gocql/v2/cmd/cqlgen -type=Event,StreamEvent

// Audit is embedded in Event and StreamEvent.
type Audit struct {
	CreatedAt cql.Timestamp
	UpdatedAt cql.Timestamp
}

// Event maps a row of the struct mapping test table.
type Event struct { //nolint:govet // field order mirrors the table, not memory layout
	Bucket int32
	ID     cql.UUID `cql:"id"`
	Name   string
	Blob   []byte
	Big    int64
	Maybe  cql.Nullable[int32]
	Tags   []string
	Scores []int32 `cql:"scores,codec=SetCodec"`
	Audit
	Note string `cql:"-"`
}

// StreamEvent is Event in the streaming decoders' types.
type StreamEvent struct { //nolint:govet // field order mirrors the table, not memory layout
	Bucket int32
	ID     cql.UUID `cql:"id"`
	Name   []byte   `cql:"name,stream=StreamText"`
	Blob   []byte
	Big    int64
	Maybe  cql.Nullable[int32]
	Scores []int32 `cql:"scores,codec=SetCodec,stream=StreamSet"`
	Audit
}
