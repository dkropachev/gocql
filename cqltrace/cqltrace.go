// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cqltrace reads CQL trace sessions: the server-side query traces a
// cluster records in system_traces for executions run with
// exec.Options.CQLTrace. Result.CQLTraces names the sessions to read, with the
// attempt each one records; a failed execution's Result lists its failed
// attempts' sessions too.
//
// CQL tracing is unrelated to client-side tracing such as OpenTelemetry,
// which instruments the driver through config.Observer. An observer can link
// the two through config.AttemptEvent.CQLTraceID.
package cqltrace

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
)

// Info is one CQL trace session: a row of system_traces.sessions and its
// events.
type Info struct {
	// Started is when the coordinator began the request.
	Started     time.Time
	Coordinator netip.Addr
	// Client is the address the request came from.
	Client netip.Addr
	// Parameters are the request's parameters as the coordinator recorded
	// them, such as its query text, consistency level and page size.
	Parameters map[string]string
	// Command is the request kind, such as QUERY.
	Command string
	// Request describes the request, such as "Execute CQL3 query".
	Request string
	Events  []Event
	// Duration is how long the coordinator took to answer.
	Duration time.Duration
	// RequestSize and ResponseSize are the request and response sizes in
	// bytes. ScyllaDB only.
	RequestSize  cql.Option[int32]
	ResponseSize cql.Option[int32]
	ID           cql.UUID
}

// Event is one activity a node recorded in a trace session.
type Event struct {
	Source   netip.Addr
	Activity string
	Thread   string
	// Elapsed is the time since the source node began its part of the request.
	Elapsed time.Duration
	// ID identifies the event; ID.Time is when it was recorded.
	ID cql.TimeUUID
}

// Nodes returns the coordinator and every event source, without repeats, in
// the order they first appear.
func (i Info) Nodes() []netip.Addr {
	nodes := make([]netip.Addr, 0, 1+len(i.Events))
	if i.Coordinator.IsValid() {
		nodes = append(nodes, i.Coordinator)
	}
	for _, e := range i.Events {
		if e.Source.IsValid() && !slices.Contains(nodes, e.Source) {
			nodes = append(nodes, e.Source)
		}
	}
	return nodes
}

// String formats the trace session as cqlsh prints one: a header, then a line
// per event with its activity, time, source and elapsed microseconds.
func (i Info) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Tracing session: %s\n", i.ID)
	fmt.Fprintf(&b, "coordinator %s, client %s, started %s, duration %s\n", addr(i.Coordinator), addr(i.Client), i.Started.UTC().Format(time.RFC3339Nano), i.Duration)
	for _, e := range i.Events {
		fmt.Fprintf(&b, " %s | %s | %s | %d\n", e.Activity, e.ID.Time().UTC().Format("2006-01-02 15:04:05.000000"), addr(e.Source), e.Elapsed.Microseconds())
	}
	return b.String()
}

func addr(a netip.Addr) string {
	if !a.IsValid() {
		return "-"
	}
	return a.String()
}

// Fetcher reads CQL trace sessions. Its zero value reads at consistency ONE
// and polls every 100ms.
type Fetcher struct {
	// Poll is the wait between readiness checks. Zero means 100ms.
	Poll time.Duration
	// Settle, when positive, keeps reading a ready trace session's events
	// until their count stays the same for Settle, so that events written
	// late are included. Zero returns as soon as the session is ready. If
	// ctx ends while settling, Fetch returns the events read so far and no
	// error.
	Settle time.Duration
	// Consistency is the consistency the trace tables are read at. Zero
	// (cql.Any, which cannot read) means cql.One.
	Consistency cql.Consistency
}

// ErrNotReady is returned when ctx ends before the trace session is ready.
// errors.Is also matches the error that ended the wait, such as
// context.DeadlineExceeded.
var ErrNotReady = errors.New("gocql: CQL trace session not ready")

// Fetch reads the trace session id with a zero Fetcher.
func Fetch[D gocql.Dialect](ctx context.Context, s *gocql.Session[D], id cql.UUID) (Info, error) {
	return Fetcher{}.Fetch(ctx, s, id)
}

// Fetch reads the trace session id, polling until it is ready or ctx ends.
//
// The cluster writes a trace session asynchronously, after it answers the
// traced request. A session is ready once its row records a duration and at
// least one event is visible. Events can still arrive after that; set Settle
// to wait for them. Only ctx bounds the wait; each read also uses the
// session's default profile timeout.
func (f Fetcher) Fetch[D gocql.Dialect](ctx context.Context, s *gocql.Session[D], id cql.UUID) (Info, error) {
	info := Info{ID: id}
	if s == nil {
		return info, errors.New("gocql: nil session")
	}
	poll := f.Poll
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	consistency := f.Consistency
	if consistency == cql.Any {
		consistency = cql.One
	}
	options := exec.Options{Consistency: cql.Option[cql.Consistency]{Value: consistency, Set: true}, Idempotent: true}
	_, scylla := any(*new(D)).(gocql.ScyllaV4)
	sessionStatement := sessionStatement(scylla)
	for {
		var row sessionRow
		found, _, err := s.UnpreparedOne(ctx, sessionStatement, id, &row, options)
		if err != nil {
			return info, notReady(ctx, err)
		}
		if found && row.Duration.Valid {
			row.fill(&info)
			events, err := readEvents(ctx, s, id, options)
			if err != nil {
				return info, notReady(ctx, err)
			}
			if len(events) > 0 {
				info.Events = events
				break
			}
		}
		if err := wait(ctx, poll); err != nil {
			return info, err
		}
	}
	if f.Settle <= 0 {
		return info, nil
	}
	for {
		if err := wait(ctx, f.Settle); err != nil {
			// The events read so far stand; only waiting for more failed.
			return info, nil
		}
		events, err := readEvents(ctx, s, id, options)
		if err != nil {
			if ctx.Err() != nil {
				// As above: the session is ready, ctx only cut the wait short.
				return info, nil
			}
			return info, err
		}
		if len(events) <= len(info.Events) {
			return info, nil
		}
		info.Events = events
	}
}

// notReady wraps err in ErrNotReady when ctx ended, so that a deadline
// reports the same error whether it struck during a read or a wait.
func notReady(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrNotReady, err)
	}
	return err
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return notReady(ctx, ctx.Err())
	case <-timer.C:
		return nil
	}
}

var sessionIDMarker = cql.NewColumns(cql.ColumnSpec{Name: "session_id", Type: cql.UUIDType})

type sessionRow struct {
	Coordinator  cql.Nullable[netip.Addr]
	Client       cql.Nullable[netip.Addr]
	Parameters   cql.Nullable[map[string]string]
	Command      cql.Nullable[string]
	Request      cql.Nullable[string]
	Started      cql.Nullable[cql.Timestamp]
	Duration     cql.Nullable[int32]
	RequestSize  cql.Nullable[int32]
	ResponseSize cql.Nullable[int32]
}

func (r *sessionRow) fill(info *Info) {
	info.Coordinator = r.Coordinator.Value
	info.Client = r.Client.Value
	if r.Started.Valid {
		info.Started = time.UnixMilli(int64(r.Started.Value))
	}
	info.Duration = time.Duration(r.Duration.Value) * time.Microsecond
	info.Command, info.Request = r.Command.Value, r.Request.Value
	info.Parameters = r.Parameters.Value
	info.RequestSize = cql.Option[int32]{Value: r.RequestSize.Value, Set: r.RequestSize.Valid}
	info.ResponseSize = cql.Option[int32]{Value: r.ResponseSize.Value, Set: r.ResponseSize.Valid}
}

// sessionStatement reads a trace session row. ScyllaDB's table has columns
// Cassandra's lacks, and selecting a missing column fails.
func sessionStatement(scylla bool) cql.Statement[cql.UUID, sessionRow] {
	fields := []cql.OutputField[sessionRow]{
		cql.Column("coordinator", cql.NullableCodec(cql.InetCodec), func(r *sessionRow) *cql.Nullable[netip.Addr] { return &r.Coordinator }),
		cql.Column("client", cql.NullableCodec(cql.InetCodec), func(r *sessionRow) *cql.Nullable[netip.Addr] { return &r.Client }),
		cql.Column("started_at", cql.NullableCodec(cql.TimestampCodec), func(r *sessionRow) *cql.Nullable[cql.Timestamp] { return &r.Started }),
		cql.Column("duration", cql.NullableCodec(cql.Int32Codec), func(r *sessionRow) *cql.Nullable[int32] { return &r.Duration }),
		cql.Column("command", cql.NullableCodec(cql.StringCodec), func(r *sessionRow) *cql.Nullable[string] { return &r.Command }),
		cql.Column("request", cql.NullableCodec(cql.StringCodec), func(r *sessionRow) *cql.Nullable[string] { return &r.Request }),
		cql.Column("parameters", cql.NullableCodec(cql.GoMapCodec(cql.StringCodec, cql.StringCodec)), func(r *sessionRow) *cql.Nullable[map[string]string] { return &r.Parameters }),
	}
	columns := "coordinator, client, started_at, duration, command, request, parameters"
	if scylla {
		fields = append(fields,
			cql.Column("request_size", cql.NullableCodec(cql.Int32Codec), func(r *sessionRow) *cql.Nullable[int32] { return &r.RequestSize }),
			cql.Column("response_size", cql.NullableCodec(cql.Int32Codec), func(r *sessionRow) *cql.Nullable[int32] { return &r.ResponseSize }),
		)
		columns += ", request_size, response_size"
	}
	return cql.Statement[cql.UUID, sessionRow]{
		CQL:         "SELECT " + columns + " FROM system_traces.sessions WHERE session_id = ?",
		Params:      cql.OneParam(cql.UUIDCodec),
		Rows:        cql.Row(fields...),
		BindMarkers: sessionIDMarker,
	}
}

type eventRow struct {
	Source   cql.Nullable[netip.Addr]
	Activity cql.Nullable[string]
	Thread   cql.Nullable[string]
	Elapsed  cql.Nullable[int32]
	ID       cql.TimeUUID
}

var eventStatement = cql.Statement[cql.UUID, eventRow]{
	CQL:    "SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?",
	Params: cql.OneParam(cql.UUIDCodec),
	Rows: cql.Row(
		cql.Column("event_id", cql.TimeUUIDCodec, func(r *eventRow) *cql.TimeUUID { return &r.ID }),
		cql.Column("activity", cql.NullableCodec(cql.StringCodec), func(r *eventRow) *cql.Nullable[string] { return &r.Activity }),
		cql.Column("source", cql.NullableCodec(cql.InetCodec), func(r *eventRow) *cql.Nullable[netip.Addr] { return &r.Source }),
		cql.Column("source_elapsed", cql.NullableCodec(cql.Int32Codec), func(r *eventRow) *cql.Nullable[int32] { return &r.Elapsed }),
		cql.Column("thread", cql.NullableCodec(cql.StringCodec), func(r *eventRow) *cql.Nullable[string] { return &r.Thread }),
	),
	BindMarkers: sessionIDMarker,
}

func readEvents[D gocql.Dialect](ctx context.Context, s *gocql.Session[D], id cql.UUID, options exec.Options) ([]Event, error) {
	var events []Event
	var row eventRow
	_, err := s.UnpreparedEach(ctx, eventStatement, id, &row, func(r *eventRow) error {
		events = append(events, Event{
			ID: r.ID, Activity: r.Activity.Value, Thread: r.Thread.Value, Source: r.Source.Value,
			Elapsed: time.Duration(r.Elapsed.Value) * time.Microsecond,
		})
		return nil
	}, options)
	return events, err
}
