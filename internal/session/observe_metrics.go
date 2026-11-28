// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"encoding/binary"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// tracked reports whether l counts in the session's metrics: every
// execution but those whose options leave it out (OptionsStorage's
// InternalUntracked), such as package cqltrace's reads.
func (l *logicalExecution) tracked() bool { return l == nil || !l.untracked }

// countAttempt counts an attempt request sends on conn, in the session's
// metrics and its host's.
func (rt *InternalSessionRuntime) countAttempt(request *InternalExecutionRequest, conn *transport.Conn) {
	if !request.InternalLogical.tracked() {
		return
	}
	rt.attempts.Add(1)
	if c := pool.CountersOf(conn); c != nil {
		c.Attempts.Add(1)
	}
}

// countAttemptEnd counts a failed attempt of request on conn in its host's
// metrics; a nil err counts nothing.
func (rt *InternalSessionRuntime) countAttemptEnd(request *InternalExecutionRequest, conn *transport.Conn, err error) {
	if err == nil || !request.InternalLogical.tracked() {
		return
	}
	if c := pool.CountersOf(conn); c != nil {
		c.Errors.Add(1)
		if cqlerr.KindOf(err) == cqlerr.KindTimeout {
			c.Timeouts.Add(1)
		}
	}
}

// countRetry counts a retry of request.
func (rt *InternalSessionRuntime) countRetry(request *InternalExecutionRequest) {
	if request.InternalLogical.tracked() {
		rt.retries.Add(1)
	}
}

// recordPage records latency, from sending a page's first attempt to the
// response that served it, in the session's latency percentiles. Failed
// pages are not recorded: a burst of timeouts would otherwise pin the
// percentiles at the timeout, and a speculation policy reading them would
// wait that long before speculating.
func (rt *InternalSessionRuntime) recordPage(request *InternalExecutionRequest, latency time.Duration) {
	if l := request.InternalLogical; l != nil && !l.untracked {
		rt.RecordV2Latency(l.id, latency)
	}
}

// sampleCQLTrace asks the profile's sampler whether to trace the request
// info describes, on l's log context, so that a panic in it is logged and
// leaves the request untraced.
func sampleCQLTrace(rt *InternalSessionRuntime, l *logicalExecution, sample func(routing2.Info) bool, info routing2.Info) bool {
	traced := false
	rt.ObserveSafely(l.logContext(), func() { traced = sample(info) })
	return traced
}

// requestPageSize is the page size request asks for, cql.Unpaged for a
// request that asks for its whole result in one page.
func requestPageSize(request *InternalExecutionRequest) int {
	if size := request.options.InternalProfile.PageSize; size > 0 {
		return int(size)
	}
	return cql.Unpaged
}

// newBoundValues builds an observe.BoundValues.
var newBoundValues = link.Get[func(cql.Columns, int, any, func(any, int) cql.ValueReader) observe.BoundValues](link.NewBoundValues)

// encodedBoundValues views values, which stay the request's until it ends.
func encodedBoundValues(values *model.EncodedValues) observe.BoundValues {
	return newBoundValues(values.Columns, len(values.Cells), values, encodedValueAt)
}

func encodedValueAt(src any, i int) cql.ValueReader {
	cell := &src.(*model.EncodedValues).Cells[i]
	return codec.NewValueReader(cell.Typ, cell.State, cell.Body, nil)
}

// batchStatementValues is one batch statement's values: the framed values
// [start, end) of the batch's storage.
type batchStatementValues struct {
	values  []byte
	columns cql.Columns
}

// boundStatements returns the batch's statements, each with a view of its
// values.
func (b *batchState) boundStatements() []observe.BatchStatement {
	statements := make([]observe.BatchStatement, len(b.entries))
	views := make([]batchStatementValues, len(b.entries))
	for i, entry := range b.entries {
		views[i] = batchStatementValues{values: b.values[entry.start:entry.end], columns: entry.columns}
		statements[i] = observe.BatchStatement{Statement: entry.statement, Values: newBoundValues(entry.columns, entry.count, &views[i], batchValueAt)}
	}
	return statements
}

// batchValueAt reads the i-th of a batch statement's framed values: each is
// a [int] length, -1 for NULL and -2 for UNSET, then that many bytes.
func batchValueAt(src any, i int) cql.ValueReader {
	v := src.(*batchStatementValues)
	b := v.values
	var column cql.ColumnSpec
	column, _ = v.columns.At(i)
	for k := 0; ; k++ {
		if len(b) < 4 {
			return codec.NewValueReader(column.Type, cql.CellNull, wire.Chain{}, nil)
		}
		n := int32(binary.BigEndian.Uint32(b))
		b = b[4:]
		size := max(int(n), 0)
		if size > len(b) {
			size = len(b)
		}
		if k < i {
			b = b[size:]
			continue
		}
		switch n {
		case -1:
			return codec.NewValueReader(column.Type, cql.CellNull, wire.Chain{}, nil)
		case -2:
			return codec.NewValueReader(column.Type, cql.CellUnset, wire.Chain{}, nil)
		}
		return codec.NewValueReader(column.Type, cql.CellPresent, wire.ChainOf(b[:size]), nil)
	}
}
