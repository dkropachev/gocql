// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"encoding/binary"
	"fmt"
	"math"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
)

// NewParamsTemplate encodes the fixed options of r, a QUERY or EXECUTE
// request, for protocol: the flags they set and everything after the values
// except the paging state, with the timestamp left as a placeholder.
func NewParamsTemplate(r *model.AttemptRequest, protocol byte) (*model.ParamsTemplate, error) {
	t := new(model.ParamsTemplate)
	if _, err := AppendParamsTemplate(t, nil, r, protocol); err != nil {
		return nil, err
	}
	return t, nil
}

// AppendParamsTemplate encodes r's template into t like NewParamsTemplate,
// appending its tail to buf, and returns buf. Templates encoded together can
// share one buffer: a tail is never appended to once encoded.
func AppendParamsTemplate(t *model.ParamsTemplate, buf []byte, r *model.AttemptRequest, protocol byte) ([]byte, error) {
	if r.Batch != nil || r.Prepare {
		return buf, fmt.Errorf("gocql: parameter templates encode QUERY and EXECUTE only")
	}
	if len(r.Keyspace) > math.MaxUint16 {
		return buf, fmt.Errorf("gocql: oversized Protocol string")
	}
	*t = model.ParamsTemplate{Protocol: protocol, PageSize: r.PageSize, SerialConsistency: r.SerialConsistency, DefaultTimestamp: r.DefaultTimestamp, Keyspace: r.Keyspace, TimestampAt: -1}
	if r.HasNowInSeconds {
		t.HasNowInSeconds, t.NowInSeconds = true, r.NowInSeconds
	}
	if r.PageSize > 0 {
		t.Flags |= frm.FlagPageSize
	}
	if r.SerialConsistency != 0 {
		t.Flags |= frm.FlagWithSerialConsistency
	}
	if r.DefaultTimestamp {
		t.Flags |= frm.FlagDefaultTimestamp
	}
	if r.Keyspace != "" {
		t.Flags |= frm.FlagWithKeyspace
	}
	if r.HasNowInSeconds {
		t.Flags |= frm.FlagWithNowInSeconds
	}
	start := len(buf)
	tail := buf
	if r.PageSize > 0 {
		tail = binary.BigEndian.AppendUint32(tail, uint32(r.PageSize))
	}
	t.PagingAt = len(tail) - start
	if r.SerialConsistency != 0 {
		tail = binary.BigEndian.AppendUint16(tail, uint16(r.SerialConsistency))
	}
	if r.DefaultTimestamp {
		t.TimestampAt = len(tail) - start
		tail = binary.BigEndian.AppendUint64(tail, 0)
	}
	if r.Keyspace != "" {
		tail = binary.BigEndian.AppendUint16(tail, uint16(len(r.Keyspace)))
		tail = append(tail, r.Keyspace...)
	}
	if r.HasNowInSeconds {
		tail = binary.BigEndian.AppendUint32(tail, uint32(r.NowInSeconds))
	}
	t.Tail = tail[start:len(tail):len(tail)]
	return tail, nil
}

// writeTemplatedParams writes r's parameters block from its template, which
// matches r: the consistency, the flags and the value count in one scalar
// run, the values, then the template's tail with the paging state and the
// timestamp filled in. It writes the same bytes as WriteTypedParams.
func (c *Settings) writeTemplatedParams(e *TypedEnvelope, r *model.AttemptRequest) {
	t := r.Params
	flags := t.Flags
	if r.ValueCount > 0 {
		flags |= frm.FlagValues
	}
	if r.SkipMetadata {
		flags |= frm.FlagSkipMetaData
	}
	if r.PagingState != nil {
		flags |= frm.FlagWithPagingState
	}
	n := 3
	if c.Protocol >= 5 {
		n = 6
	}
	if r.ValueCount > 0 {
		n += 2
	}
	p, err := e.b.ScalarBytes(n)
	if err != nil {
		e.check(err)
		return
	}
	binary.BigEndian.PutUint16(p, uint16(r.Consistency))
	if c.Protocol >= 5 {
		binary.BigEndian.PutUint32(p[2:], flags)
		p = p[6:]
	} else {
		p[2] = byte(flags)
		p = p[3:]
	}
	if r.ValueCount > 0 {
		binary.BigEndian.PutUint16(p, uint16(r.ValueCount))
		e.Chain(r.Values)
	}
	tail, at := t.Tail, 0
	if r.PagingState != nil {
		e.copyTail(tail[:t.PagingAt], -1, 0)
		e.Bytes(r.PagingState, false)
		tail, at = tail[t.PagingAt:], t.PagingAt
	}
	ts := -1
	if t.TimestampAt >= 0 {
		ts = t.TimestampAt - at
	}
	e.copyTail(tail, ts, r.Timestamp)
}

// copyTail appends b, writing timestamp at offset ts when ts is not -1.
func (e *TypedEnvelope) copyTail(b []byte, ts int, timestamp int64) {
	if len(b) == 0 {
		return
	}
	p, err := e.b.ScalarBytes(len(b))
	if err != nil {
		e.check(err)
		return
	}
	copy(p, b)
	if ts >= 0 {
		binary.BigEndian.PutUint64(p[ts:], uint64(timestamp))
	}
}
