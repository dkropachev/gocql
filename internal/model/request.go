// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	"time"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type PayloadEntry struct {
	Key   string
	Value cqltype.Nullable[cqltype.Bytes]
}

type EncodedCell struct {
	Typ   cqltype.DataType
	Body  wire.Chain
	State cqltype.CellState
}
type EncodedValues struct {
	Columns cqltype.Columns
	Cells   []EncodedCell
	Framed  wire.Chain
}

// attemptRequest contains only immutable protocol inputs. The transport retains
// lease until every accepted writer has stopped accessing its spans.
type AttemptRequest struct {
	Lease        *wire.Lease
	NowInSeconds *int32
	Batch        *TypedBatchRequest
	// Params, when it matches the request, is its pre-encoded parameters
	// block; the encoder copies it instead of encoding each option.
	Params                         *ParamsTemplate
	Statement                      string
	Keyspace                       string
	PreparedID, ResultMetadataID   []byte
	PagingState                    []byte
	Payload                        []PayloadEntry
	Values                         wire.Chain
	ValueCount                     int
	PageSize                       int
	Timestamp                      int64
	Timeout                        time.Duration
	Consistency, SerialConsistency cqltype.Consistency
	TabletVersionBlock             byte
	DefaultTimestamp, SkipMetadata bool
	Trace                          bool
	Prepare                        bool
}

// ParamsTemplate is a query parameters block encoded once for the options
// that stay fixed across a query's executions. Each attempt adds only what
// varies: the consistency, the values, the skip-metadata and paging bits of
// the flags, the paging state and the timestamp value. A template is
// immutable once built, so attempts running at once share it.
type ParamsTemplate struct {
	Keyspace string
	// Tail is everything after the values: the page size, then, after where
	// a paging state goes, the serial consistency, a timestamp placeholder,
	// the keyspace and now_in_seconds.
	Tail     []byte
	PageSize int
	// PagingAt is the offset in Tail where a paging state goes.
	PagingAt int
	// TimestampAt is the offset in Tail of the timestamp, or -1.
	TimestampAt  int
	NowInSeconds int32
	// Flags holds the flags of the fixed options.
	Flags             uint32
	SerialConsistency cqltype.Consistency
	// The options the template encodes, compared by Matches.
	Protocol         byte
	DefaultTimestamp bool
	HasNowInSeconds  bool
}

// Matches reports whether t encodes r's fixed options for protocol.
func (t *ParamsTemplate) Matches(r *AttemptRequest, protocol byte) bool {
	if t == nil || t.Protocol != protocol || t.PageSize != r.PageSize || t.SerialConsistency != r.SerialConsistency || t.DefaultTimestamp != r.DefaultTimestamp || t.Keyspace != r.Keyspace {
		return false
	}
	if r.NowInSeconds == nil {
		return !t.HasNowInSeconds
	}
	return t.HasNowInSeconds && t.NowInSeconds == *r.NowInSeconds
}

type TypedBatchRequest struct {
	Items []TypedBatchItem
	Kind  cqltype.BatchKind
}
type TypedBatchItem struct {
	Statement  string
	PreparedID []byte
	Values     wire.Chain
	ValueCount int
}
