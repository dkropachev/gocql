// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cqltrace reads CQL trace sessions: the server-side query traces a
// cluster records in system_traces for executions run with
// execopt.Options.CQLTrace. cql.Result.CQLTraces names the sessions to read, with the
// attempt each one records; a failed execution's Result lists its failed
// attempts' sessions too.
//
// CQL tracing is unrelated to client-side tracing such as OpenTelemetry,
// which instruments the driver through observe.Hooks. An Attempt hook can link
// the two through routing.AttemptEvent.CQLTrace, and read the session with
// Fetch; hooks receive no session, so one that reads traces binds the session
// once Connect returns it, as Example_slowQueries shows.
//
// The reads of a trace session are the package's own: the session's
// observe.Hooks do not see them, and its metrics do not count them, so
// reading traces does not show up as application traffic.
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
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/execopt"
	"github.com/scylladb/gocql/v2/internal/link"
)

// Info is one CQL trace session: a row of system_traces.sessions and its
// events.
//
// The driver fills it; its zero value is empty.
type Info struct {
	// Started is when the coordinator began the request.
	Started time.Time
	// Coordinator is the address of the node that coordinated the request.
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
	// Events lists the events the nodes recorded, in the order they were
	// recorded.
	Events []Event
	// Duration is how long the coordinator took to answer.
	Duration time.Duration
	// RequestSize is the request size in bytes. ScyllaDB only.
	RequestSize cql.Option[int32]
	// ResponseSize is the response size in bytes. ScyllaDB only.
	ResponseSize cql.Option[int32]
	// ID is the trace session ID.
	ID cql.UUID
	// Settled reports that Events stopped changing for Fetcher.Settle. It is
	// false when Settle is negative or ctx ended first: nodes may still have
	// been writing events, so Events may be incomplete.
	Settled bool
}

// Event is one activity a node recorded in a trace session.
//
// The driver fills it; its zero value is empty.
type Event struct {
	// Source is the address of the node that recorded the event.
	Source netip.Addr
	// Activity describes what the node did.
	Activity string
	// Thread names the node's thread that recorded the event.
	Thread string
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

// Fetcher reads CQL trace sessions. Its zero value reads at consistency ONE,
// polls every 100ms, settles for 200ms and waits at most 10s.
type Fetcher struct {
	// MaxWait bounds how long Fetch waits for a trace session, settling
	// included, so that a session the cluster never writes, as when it
	// sampled the request out or tracing failed, does not keep Fetch polling
	// until ctx ends. Zero means 10s; negative leaves only ctx to bound the
	// wait. A session not ready within it fails with ErrNotReady, which
	// errors.Is also matches with context.DeadlineExceeded.
	MaxWait time.Duration
	// Poll is the wait between readiness checks. Zero means 100ms.
	Poll time.Duration
	// Settle is how long a ready trace session's event count must stay the
	// same before Fetch returns, so that events nodes write late are
	// included. Zero means 200ms; negative returns as soon as the session is
	// ready. If ctx ends while settling, Fetch returns the events read so far
	// and no error, with Info.Settled false.
	Settle time.Duration
	// Consistency is the consistency the trace tables are read at. Zero
	// (cql.Any, which cannot read) means cql.One.
	Consistency cql.Consistency
}

// ErrNotReady is returned when ctx ends, or Fetcher.MaxWait passes, before
// the trace session is ready. errors.Is also matches the error that ended
// the wait, such as context.DeadlineExceeded.
var ErrNotReady = errors.New("gocql: CQL trace session not ready")

// Fetch reads the trace session ref names with a zero Fetcher.
func Fetch(ctx context.Context, s *gocql.Session, ref cql.CQLTraceRef) (Info, error) {
	return Fetcher{}.Fetch(ctx, s, ref)
}

// FetchAll reads the trace sessions refs name with a zero Fetcher.
func FetchAll(ctx context.Context, s *gocql.Session, refs []cql.CQLTraceRef) ([]Info, error) {
	return Fetcher{}.FetchAll(ctx, s, refs)
}

// FetchAll reads the trace session of every reference in refs, in order, as
// Fetch reads one: the sessions cql.Result.CQLTraces lists, or a failed
// execution's cqlerr.ExecutionError.Result.CQLTraces. It returns one Info per
// reference, also for a session it could not read, which holds only its ID,
// and the errors of those sessions joined, each naming its session; a
// session that fails does not keep the later ones from being read. Once ctx
// ends, the remaining sessions fail with ErrNotReady; f.MaxWait bounds each
// session's wait apart.
func (f Fetcher) FetchAll(ctx context.Context, s *gocql.Session, refs []cql.CQLTraceRef) ([]Info, error) {
	return fetchAll(ctx, refs, func(ctx context.Context, ref cql.CQLTraceRef) (Info, error) { return f.Fetch(ctx, s, ref) })
}

// fetchAll is FetchAll reading each session with fetch.
func fetchAll(ctx context.Context, refs []cql.CQLTraceRef, fetch func(context.Context, cql.CQLTraceRef) (Info, error)) ([]Info, error) {
	infos := make([]Info, len(refs))
	var errs []error
	for i, ref := range refs {
		info, err := fetch(ctx, ref)
		if err != nil {
			info = Info{ID: ref.ID}
			errs = append(errs, fmt.Errorf("trace session %s: %w", ref.ID, err))
		}
		infos[i] = info
	}
	return infos, errors.Join(errs...)
}

// compileUntracked is gocql.Session.CompileOptions for reads the session's
// metrics leave out.
var compileUntracked = link.Get[func(*gocql.Session, execopt.Options) (execopt.CompiledOptions, error)](link.CompileUntracked)

// Fetch reads the trace session ref names, polling until it is ready or ctx
// ends.
//
// The cluster writes a trace session asynchronously, after it answers the
// traced request. A session is ready once its row records a duration and at
// least one event is visible. Events can still arrive after that, so Fetch
// then waits for them to settle; Info.Settled reports whether they did. ctx
// and f.MaxWait bound the wait; each read also uses the session's default
// profile timeout.
//
// The reads go to ref.HostID, the coordinator of the traced attempt, which
// writes the session's row and its own events first, so that a read at a
// low consistency finds them soonest. Once a read sent there fails, as when
// the host is down or has no connection, the reads that follow are routed
// as the session's default profile routes them, to any replica; a ref
// without a HostID is routed that way from the start. The reads run without
// the session's observe.Hooks and are not counted in its metrics.
func (f Fetcher) Fetch(ctx context.Context, s *gocql.Session, ref cql.CQLTraceRef) (Info, error) {
	id := ref.ID
	if s == nil {
		return Info{ID: id}, &cql.ValidationError{Field: "session", Err: errors.New("nil session")}
	}
	consistency := f.Consistency
	if consistency == cql.Any {
		consistency = cql.One
	}
	base := execopt.Options{Consistency: cql.Some(consistency), Idempotent: cql.Some(true), NoSessionObserver: true}
	anywhere, err := compileUntracked(s, base)
	if err != nil {
		return Info{ID: id}, err
	}
	options := anywhere
	pinned := !ref.HostID.IsZero()
	if pinned {
		base.HostID = cql.Some(ref.HostID)
		if options, err = compileUntracked(s, base); err != nil {
			return Info{ID: id}, err
		}
	}
	// read runs one read with options and, when a read pinned to the
	// coordinator fails, unpins the rest and runs it again.
	read := func(ctx context.Context, run func(execopt.CompiledOptions) error) error {
		err := run(options)
		if err != nil && pinned && ctx.Err() == nil {
			pinned, options = false, anywhere
			err = run(options)
		}
		return err
	}
	statement := sessionStatement(s.Dialect() == config.ScyllaV4)
	return f.fetch(ctx, id, func(ctx context.Context) (sessionRow, bool, error) {
		var row sessionRow
		var found bool
		err := read(ctx, func(options execopt.CompiledOptions) error {
			var err error
			row = sessionRow{}
			found, _, err = s.UnpreparedOne(ctx, statement, id, &row, options)
			return err
		})
		return row, found, err
	}, func(ctx context.Context) ([]Event, error) {
		var events []Event
		err := read(ctx, func(options execopt.CompiledOptions) error {
			var err error
			events, err = readEvents(ctx, s, id, options)
			return err
		})
		return events, err
	})
}

// fetch is Fetch reading the session row with readSession and its events
// with readEvents.
func (f Fetcher) fetch(ctx context.Context, id cql.UUID, readSession func(context.Context) (sessionRow, bool, error), readEvents func(context.Context) ([]Event, error)) (Info, error) {
	info := Info{ID: id}
	maxWait := f.MaxWait
	if maxWait == 0 {
		maxWait = 10 * time.Second
	}
	if maxWait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxWait)
		defer cancel()
	}
	poll := f.Poll
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	for {
		row, found, err := readSession(ctx)
		if err != nil {
			return info, notReady(ctx, err)
		}
		if found && row.Duration.Valid {
			row.fill(&info)
			events, err := readEvents(ctx)
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
	err := f.settle(ctx, &info, readEvents)
	return info, err
}

// settle rereads a ready session's events with read until their count stays
// the same for f.Settle, and sets info.Settled when it does.
func (f Fetcher) settle(ctx context.Context, info *Info, read func(context.Context) ([]Event, error)) error {
	d := f.Settle
	if d == 0 {
		d = 200 * time.Millisecond
	}
	if d < 0 {
		return nil
	}
	for {
		if err := wait(ctx, d); err != nil {
			// The events read so far stand; only waiting for more failed.
			return nil
		}
		events, err := read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// As above: the session is ready, ctx only cut the wait short.
				return nil
			}
			return err
		}
		switch {
		case len(events) == len(info.Events):
			info.Settled = true
			return nil
		case len(events) > len(info.Events):
			info.Events = events
		}
		// Fewer events means a replica that has not caught up answered the
		// read: keep what was read and wait again.
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
		cql.Column("parameters", cql.NullableCodec(cql.MapCodec(cql.StringCodec, cql.StringCodec)), func(r *sessionRow) *cql.Nullable[map[string]string] { return &r.Parameters }),
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

func readEvents(ctx context.Context, s *gocql.Session, id cql.UUID, options execopt.CompiledOptions) ([]Event, error) {
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
