// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type ResponseData struct {
	MetadataChanged         bool
	Lwt                     bool
	NoMetadata              bool
	RowCount                int32
	Kind                    int
	ColumnCount             int
	Owner                   *wire.Lease
	Rows                    wire.Chain
	Columns                 Columns
	PagingState, MetadataID []byte
	State                   PageState
	Result                  Result
	// Extras holds fields that only PREPARE, USE, schema-change, traced,
	// warned or payload-carrying responses have. It is nil for others.
	Extras *ResponseExtras
	// InternalParseCursor is the response parser's cursor. It lives in the
	// already heap-allocated response so that parsing allocates no cursor.
	InternalParseCursor wire.Cursor
}

// ResponseExtras are the rarely present parts of a response.
type ResponseExtras struct {
	Keyspace     string
	SchemaChange bool
	PreparedID   []byte
	BindColumns  Columns
	PkIndexes    []int
	TraceID      []byte
	Payload      []PayloadEntry
	Warnings     []string
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
func (r *ResponseData) BindColumns() Columns {
	if r.Extras == nil {
		return Columns{}
	}
	return r.Extras.BindColumns
}
func (r *ResponseData) PkIndexes() []int {
	if r.Extras == nil {
		return nil
	}
	return r.Extras.PkIndexes
}
func (r *ResponseData) TraceID() []byte {
	if r.Extras == nil {
		return nil
	}
	return r.Extras.TraceID
}
func (r *ResponseData) Payload() []PayloadEntry {
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
