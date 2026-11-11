// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func (s *InternalSessionRuntime) Trace(ctx context.Context, id cqltype.UUID) (implCluster.TraceInfo, error) {
	result := implCluster.TraceInfo{ID: id}
	if ctx == nil {
		return result, cqltype.TypeValidation("trace context", "nil context")
	}
	if s == nil || s.Closed() {
		return result, runtimeerrors.ErrClosed
	}
	profile := settings.InternalDefaultProfile()
	if s.InternalProfile != nil {
		profile = *s.InternalProfile
	}
	if profile.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, profile.Timeout)
		defer cancel()
	}
	uuidType := cqltype.UUIDType
	params := cqltype.NewColumns(cqltype.ColumnSpec{Name: "session_id", Type: uuidType})
	sessionStatement := binding.Statement[cqltype.UUID, traceSessionRow]{CQL: "SELECT coordinator, started_at, duration FROM system_traces.sessions WHERE session_id = ?", Params: codec.OneParam(codec.UUIDCodec), Rows: codec.Row(
		codec.Column("coordinator", codec.NullableCodec(codec.InetCodec), func(r *traceSessionRow) *cqltype.Nullable[netip.Addr] { return &r.InternalCoordinator }),
		codec.Column("started_at", codec.NullableCodec(codec.TimestampCodec), func(r *traceSessionRow) *cqltype.Nullable[codec.Timestamp] { return &r.InternalStarted }),
		codec.Column("duration", codec.NullableCodec(codec.Int32), func(r *traceSessionRow) *cqltype.Nullable[int32] { return &r.InternalDuration }),
	)}
	bound, err := BindUnpreparedOwned(sessionStatement, params, id)
	if err != nil {
		return result, err
	}
	defer func() { bound.Close(); _ = bound.Wait(ctx) }()
	options, err := NewCompiledOptions(s.Core, traceValues)
	if err != nil {
		return result, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rows, err := s.run(ctx, bound, options)
		if err != nil {
			return result, err
		}
		ready := false
		var row traceSessionRow
		if rows.NextInto(&row) {
			if row.InternalCoordinator.Valid && row.InternalStarted.Valid && row.InternalDuration.Valid {
				result.Coordinator = row.InternalCoordinator.Value
				result.Started = time.UnixMilli(int64(row.InternalStarted.Value))
				result.Duration = time.Duration(row.InternalDuration.Value) * time.Microsecond
				ready = true
			}
			if rows.NextInto(&row) {
				_ = rows.Close()
				return result, &cqltype.ProtocolError{Stage: "trace", Err: fmt.Errorf("duplicate tracing session")}
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return result, err
		}
		if ready {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	eventsStatement := binding.Statement[cqltype.UUID, traceActivityRow]{CQL: "SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?", Params: codec.OneParam(codec.UUIDCodec), Rows: codec.Row(
		codec.Column("event_id", codec.TimeUUIDCodec, func(r *traceActivityRow) *codec.TimeUUID { return &r.InternalId }),
		codec.Column("activity", codec.TextCodec, func(r *traceActivityRow) *cqltype.Text { return &r.InternalActivity }),
		codec.Column("source", codec.InetCodec, func(r *traceActivityRow) *netip.Addr { return &r.InternalSource }),
		codec.Column("source_elapsed", codec.NullableCodec(codec.Int32), func(r *traceActivityRow) *cqltype.Nullable[int32] { return &r.InternalElapsed }),
		codec.Column("thread", codec.TextCodec, func(r *traceActivityRow) *cqltype.Text { return &r.InternalThread }),
	)}
	eventsBound, err := BindUnpreparedOwned(eventsStatement, params, id)
	if err != nil {
		return result, err
	}
	defer func() { eventsBound.Close(); _ = eventsBound.Wait(ctx) }()
	rows, err := s.run(ctx, eventsBound, options)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	var r traceActivityRow
	for rows.NextInto(&r) {
		e := implCluster.TraceEvent{ID: cqltype.UUID(r.InternalId), Activity: r.InternalActivity.String(), Source: r.InternalSource, Thread: r.InternalThread.String()}
		if r.InternalElapsed.Valid {
			e.Elapsed = time.Duration(r.InternalElapsed.Value) * time.Microsecond
		}
		result.Events = append(result.Events, e)
	}
	return result, rows.Err()
}

// traceValues read trace events at ONE, idempotently, under the caller's
// context deadline only.
var traceValues = Options{Consistency: cqltype.Option[cqltype.Consistency]{Value: cqltype.One, Set: true}, Idempotent: true, Timeout: cqltype.Option[time.Duration]{Set: true}}
