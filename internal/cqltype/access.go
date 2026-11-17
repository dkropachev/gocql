// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// The engine reads and builds the unexported state of package cql's value
// types through these mirrors: structs with the same layout, converted with
// link.Mirror. TestMirrorLayouts proves each pair matches.

// bytesRep mirrors cql.Bytes.
type bytesRep struct {
	lease wire.Ref
	chain wire.Chain
}

// columnsRep mirrors cql.Columns.
type columnsRep struct{ columns []cql.ColumnSpec }

// dataTypeRep mirrors cql.DataType.
type dataTypeRep struct{ descriptor *DataTypeDescriptor }

// DataTypeDescriptor mirrors the descriptor a cql.DataType points to. The
// engine reads its exported fields and uses its address as an identity.
type DataTypeDescriptor struct {
	Element, Key   cql.DataType
	Keyspace, Name string
	Fields         cql.Columns
	Kind           cql.Type
	Dimension      uint16
	frozen         bool
	frozenKnown    bool
}

// pageStateRep mirrors cql.PageState.
type pageStateRep struct {
	token []byte
	kind  uint8
}

// The kinds of a cql.PageState: the zero value starts paging.
const (
	pageMore     uint8 = 1
	pageComplete uint8 = 2
)

// BytesChain returns the wire chain b views.
func BytesChain(b cql.Bytes) wire.Chain { return link.Mirror[bytesRep](&b).chain }

// BytesLease returns the lease that keeps b's storage alive, if any.
func BytesLease(b cql.Bytes) wire.Ref { return link.Mirror[bytesRep](&b).lease }

// BytesView returns bytes viewing c, kept alive by l.
func BytesView(c wire.Chain, l wire.Ref) cql.Bytes {
	var b cql.Bytes
	*link.Mirror[bytesRep](&b) = bytesRep{lease: l, chain: c}
	return b
}

// CheckBytesAlive reports cql.ErrReleased when b's storage has been released.
func CheckBytesAlive(b cql.Bytes) error {
	if r := link.Mirror[bytesRep](&b); !r.lease.IsZero() && !r.lease.Alive() {
		return cql.ErrReleased
	}
	return nil
}

// EachBytes calls fn for each span of b, after checking b is alive.
func EachBytes(b cql.Bytes, fn func(wire.Span) error) error {
	if err := CheckBytesAlive(b); err != nil {
		return err
	}
	return link.Mirror[bytesRep](&b).chain.Each(fn)
}

// ColumnSlice returns c's columns without copying. Do not modify them.
func ColumnSlice(c cql.Columns) []cql.ColumnSpec { return link.Mirror[columnsRep](&c).columns }

// ColumnsOwned wraps columns, which the caller no longer modifies, without
// copying.
func ColumnsOwned(columns []cql.ColumnSpec) cql.Columns {
	var c cql.Columns
	link.Mirror[columnsRep](&c).columns = columns
	return c
}

// DescriptorOf returns t's descriptor, nil for the zero DataType.
func DescriptorOf(t cql.DataType) *DataTypeDescriptor { return link.Mirror[dataTypeRep](&t).descriptor }

// PageStateOwned returns a continuation state holding token, which the caller
// no longer modifies.
func PageStateOwned(token []byte) cql.PageState {
	var p cql.PageState
	*link.Mirror[pageStateRep](&p) = pageStateRep{token: token, kind: pageMore}
	return p
}

// PageToken returns p's continuation token without copying. Do not modify it.
func PageToken(p cql.PageState) []byte { return link.Mirror[pageStateRep](&p).token }

// CompletePageState returns the state of a result with no more pages.
func CompletePageState() cql.PageState {
	var p cql.PageState
	link.Mirror[pageStateRep](&p).kind = pageComplete
	return p
}
