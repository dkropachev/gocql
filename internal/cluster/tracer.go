// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"fmt"
	"io"
	"net/netip"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	"time"
)

type TraceWriter struct {
	privateStorage TraceWriterStorage
}

// NewTraceWriter returns a simple Tracer implementation that outputs
// the event log in a textual format.
func NewTraceWriter(session *Core, w io.Writer) *TraceWriter {
	return func() *TraceWriter {
		opaqueStorageValue := new(TraceWriter)
		opaqueStorageValue.privateStorage.session = session
		opaqueStorageValue.privateStorage.w = w
		opaqueStorageValue.privateStorage.maxAttempts = 5
		opaqueStorageValue.privateStorage.sleepInterval = 3 * time.Millisecond
		return opaqueStorageValue
	}()
}

func (t *TraceWriter) SetMaxAttempts(maxAttempts int) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	t.privateStorage.maxAttempts = maxAttempts
}

func (t *TraceWriter) SetSleepInterval(sleepInterval time.Duration) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	t.privateStorage.sleepInterval = sleepInterval
}

func (t *TraceWriter) Trace(traceId []byte) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	tracer := NewTracer(t.privateStorage.session)
	attempts := t.privateStorage.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	ready := false
	for i := 0; i < attempts; i++ {
		var err error
		ready, err = tracer.IsReady(traceId)
		if err != nil {
			fmt.Fprintln(t.privateStorage.w, "Error:", err)
			return
		}
		if ready || i == attempts-1 {
			break
		}
		timer := time.NewTimer(t.privateStorage.sleepInterval)
		ctx := metadata.InternalV2SchemaContext(t.privateStorage.session.NewSchemaSource())
		select {
		case <-ctx.Done():
			timer.Stop()
			fmt.Fprintln(t.privateStorage.w, "Error:", ctx.Err())
			return
		case <-timer.C:
		}
	}
	if !ready {
		fmt.Fprintln(t.privateStorage.w, "Error: failed to wait tracing to complete. !!! Tracing is incomplete !!!")
	}
	coordinator, duration, err := tracer.GetCoordinatorTime(traceId)
	if err != nil {
		fmt.Fprintln(t.privateStorage.w, "Error:", err)
		return
	}
	fmt.Fprintf(t.privateStorage.w, "Tracing session %016x (coordinator: %s, duration: %v):\n", traceId, coordinator, duration)
	activities, err := tracer.GetActivities(traceId)
	if err != nil {
		fmt.Fprintln(t.privateStorage.w, "Error:", err)
		return
	}
	for _, e := range activities {
		fmt.Fprintf(t.privateStorage.w, "%s: %s [%s] (source: %s, elapsed: %d)\n", e.Timestamp.Format("2006/01/02 15:04:05.999999"), e.Activity, e.Thread, e.Source, e.Elapsed)
	}

}

type TracerEnhanced struct {
	privateStorage TracerEnhancedStorage
}

func NewTracer(session *Core) *TracerEnhanced {
	return func() *TracerEnhanced {
		opaqueStorageValuex := new(TracerEnhanced)
		opaqueStorageValuex.privateStorage.session = session
		return opaqueStorageValuex
	}()
}

func (t *TracerEnhanced) Trace(traceId []byte) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	t.privateStorage.traceIDs = append(t.privateStorage.traceIDs, traceId)
}

func (t *TracerEnhanced) AllTraceIDs() [][]byte {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	return t.privateStorage.traceIDs
}

func (t *TracerEnhanced) IsReady(traceId []byte) (bool, error) {
	rows, err := readV2TraceRows(t.privateStorage.session, traceId, "SELECT duration FROM system_traces.sessions WHERE session_id = ?", codec.OneColumn(codec.NullableCodec(codec.Int32)))
	if err != nil {
		return false, err
	}
	return len(rows) > 0 && rows[0].Valid && rows[0].Value > 0, nil
}

func (t *TracerEnhanced) GetCoordinatorTime(traceId []byte) (string, time.Duration, error) {
	type row struct {
		coordinator netip.Addr
		duration    cqltype.Nullable[int32]
	}
	decoder := codec.Row(codec.Column("coordinator", codec.InetCodec, func(r *row) *netip.Addr { return &r.coordinator }), codec.Column("duration", codec.NullableCodec(codec.Int32), func(r *row) *cqltype.Nullable[int32] { return &r.duration }))
	rows, err := readV2TraceRows(t.privateStorage.session, traceId, "SELECT coordinator, duration FROM system_traces.sessions WHERE session_id = ?", decoder)
	if err != nil {
		return "", 0, err
	}
	if len(rows) == 0 {
		return "", 0, runtimeerrors.ErrNoRows
	}
	return rows[0].coordinator.String(), time.Duration(rows[0].duration.Value) * time.Microsecond, nil
}

type TraceEntry struct {
	Timestamp time.Time
	Activity  string
	Source    string
	Thread    string
	Elapsed   int
}

func (t *TracerEnhanced) GetActivities(traceId []byte) ([]TraceEntry, error) {
	decoder := codec.Row(codec.Column("event_id", codec.TimeUUIDCodec, func(r *model.TraceActivityRow) *codec.TimeUUID { return &r.InternalId }), codec.Column("activity", codec.TextCodec, func(r *model.TraceActivityRow) *cqltype.Text { return &r.InternalActivity }), codec.Column("source", codec.InetCodec, func(r *model.TraceActivityRow) *netip.Addr { return &r.InternalSource }), codec.Column("source_elapsed", codec.NullableCodec(codec.Int32), func(r *model.TraceActivityRow) *cqltype.Nullable[int32] { return &r.InternalElapsed }), codec.Column("thread", codec.TextCodec, func(r *model.TraceActivityRow) *cqltype.Text { return &r.InternalThread }))
	rows, err := readV2TraceRows(t.privateStorage.session, traceId, "SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?", decoder)
	if err != nil {
		return nil, err
	}
	out := make([]TraceEntry, len(rows))
	for i, r := range rows {
		out[i] = TraceEntry{Timestamp: cqltype.UUID(r.InternalId).Time(), Activity: r.InternalActivity.String(), Source: r.InternalSource.String(), Thread: r.InternalThread.String(), Elapsed: int(r.InternalElapsed.Value)}
	}
	return out, nil
}

func readV2TraceRows[R any](s *Core, traceID []byte, cql string, decoder codec.RowDecoder[R]) ([]R, error) {
	if s == nil || s.Closed() {
		return nil, runtimeerrors.ErrClosed
	}
	if len(traceID) != 16 {
		return nil, cqltype.TypeValidation("trace ID", "must contain 16 bytes")
	}
	if s.privateStorage.InternalControl == nil {
		return nil, metadata.InternalErrNoControl
	}
	backend, ok := s.privateStorage.InternalControl.(v2SystemQueryer)
	if !ok {
		return nil, cqltype.TypeValidation("tracing", "control connection does not support typed requests")
	}
	var id cqltype.UUID
	copy(id[:], traceID)
	typ := cqltype.UUIDType
	ctx := metadata.InternalV2SchemaContext(s.NewSchemaSource())
	timeout := s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return readV2SystemRowsVia(ctx, 4, timeout, backend.InternalQueryV2System, binding.Statement[cqltype.UUID, R]{CQL: cql, Params: codec.OneParam(codec.UUIDCodec), Rows: decoder}, cqltype.NewColumns(cqltype.ColumnSpec{Name: "session_id", Type: typ}), id)
}
