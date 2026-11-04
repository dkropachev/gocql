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
	State cqltype.CellState
	Body  wire.Chain
}
type EncodedValues struct {
	Columns cqltype.Columns
	Cells   []EncodedCell
	Framed  wire.Chain
}

// attemptRequest contains only immutable protocol inputs. The transport retains
// lease until every accepted writer has stopped accessing its spans.
type AttemptRequest struct {
	TabletVersionBlock             byte
	Statement                      string
	PreparedID, ResultMetadataID   []byte
	Values                         wire.Chain
	ValueCount                     int
	Lease                          *wire.Lease
	Consistency, SerialConsistency cqltype.Consistency
	PageSize                       int
	PagingState                    []byte
	Timestamp                      int64
	DefaultTimestamp, SkipMetadata bool
	Keyspace                       string
	NowInSeconds                   *int32
	Trace                          bool
	Timeout                        time.Duration
	Prepare                        bool
	Batch                          *TypedBatchRequest
	Payload                        []PayloadEntry
	// Params, when it matches the request, is its pre-encoded parameters
	// block; the encoder copies it instead of encoding each option.
	Params *ParamsTemplate
}

// ParamsTemplate is a query parameters block encoded once for the options
// that stay fixed across a query's executions. Each attempt adds only what
// varies: the consistency, the values, the skip-metadata and paging bits of
// the flags, the paging state and the timestamp value. A template is
// immutable once built, so attempts running at once share it.
type ParamsTemplate struct {
	// The options the template encodes, compared by Matches.
	Protocol          byte
	PageSize          int
	SerialConsistency cqltype.Consistency
	DefaultTimestamp  bool
	Keyspace          string
	HasNowInSeconds   bool
	NowInSeconds      int32

	// Flags holds the flags of the fixed options.
	Flags uint32
	// Tail is everything after the values: the page size, then, after where
	// a paging state goes, the serial consistency, a timestamp placeholder,
	// the keyspace and now_in_seconds.
	Tail []byte
	// PagingAt is the offset in Tail where a paging state goes.
	PagingAt int
	// TimestampAt is the offset in Tail of the timestamp, or -1.
	TimestampAt int
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
	Kind  cqltype.BatchKind
	Items []TypedBatchItem
}
type TypedBatchItem struct {
	Statement  string
	PreparedID []byte
	Values     wire.Chain
	ValueCount int
}
