// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type ResponseData struct {
	Owner *wire.Lease
	// Extras holds fields that only PREPARE, USE, schema-change, traced,
	// warned or payload-carrying responses have. It is nil for others.
	Extras                  *ResponseExtras
	Rows                    wire.Chain
	Columns                 cql.Columns
	PagingState, MetadataID []byte
	State                   cql.PageState
	Result                  cql.Result
	// InternalParseCursor is the response parser's cursor. It lives in the
	// already heap-allocated response so that parsing allocates no cursor.
	InternalParseCursor wire.Cursor
	Kind                int
	ColumnCount         int
	RowCount            int32
	MetadataChanged     bool
	Lwt                 bool
	NoMetadata          bool
}

// ResponseExtras are the rarely present parts of a response.
type ResponseExtras struct {
	Keyspace     string
	PreparedID   []byte
	BindColumns  cql.Columns
	PkIndexes    []int
	TraceID      []byte
	Payload      []cql.PayloadEntry
	Warnings     []string
	SchemaChange bool
}

// Extra returns the response extras, allocating them on first use.
func (r *ResponseData) Extra() *ResponseExtras {
	if r.Extras == nil {
		r.Extras = new(ResponseExtras)
	}
	return r.Extras
}

// The accessors below read extras without allocating.

func (r *ResponseData) Keyspace() string {
	if r.Extras == nil {
		return ""
	}
	return r.Extras.Keyspace
}
func (r *ResponseData) SchemaChange() bool { return r.Extras != nil && r.Extras.SchemaChange }
func (r *ResponseData) PreparedID() []byte {
	if r.Extras == nil {
		return nil
	}
	return r.Extras.PreparedID
}
func (r *ResponseData) BindColumns() cql.Columns {
	if r.Extras == nil {
		return cql.Columns{}
	}
	return r.Extras.BindColumns
}
func (r *ResponseData) PkIndexes() []int {
	if r.Extras == nil {
		return nil
	}
	return r.Extras.PkIndexes
}

// CQLTraceID returns the CQL trace session the response names, if any.
func (r *ResponseData) CQLTraceID() cql.Option[cql.UUID] {
	var id cql.Option[cql.UUID]
	if r.Extras != nil && len(r.Extras.TraceID) == len(id.Value) {
		copy(id.Value[:], r.Extras.TraceID)
		id.Set = true
	}
	return id
}
func (r *ResponseData) Payload() []cql.PayloadEntry {
	if r.Extras == nil {
		return nil
	}
	return r.Extras.Payload
}
func (r *ResponseData) Warnings() []string {
	if r.Extras == nil {
		return nil
	}
	return r.Extras.Warnings
}
