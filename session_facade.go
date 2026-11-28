// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/execopt"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/metrics"
	"github.com/scylladb/gocql/v2/observe"
)

// Session owns cluster metadata, pooled connections and typed executions.
// Connect opens a session of the config's config.Dialect: ScyllaV4,
// CassandraV4 or CassandraV5. Session.Dialect reports which; one Session type
// serves every dialect. Sessions and their prepared statements are safe for concurrent
// use. Close the session when the application stops using it.
//
// A zero Session was never open: Closed reports true, Dialect returns the zero
// Dialect, Metadata returns nil, and its methods that do work return a
// cql.ValidationError.
type Session implSession.Session

func (s *Session) internal() *implSession.Session { return (*implSession.Session)(s) }

// Dialect returns the dialect s was opened with.
func (s *Session) Dialect() config.Dialect {
	if s.internal().Core() == nil {
		return 0
	}
	cfg := &s.internal().Data().InternalCfg
	return config.Dialect(implSettings.DialectOf(cfg.Server, cfg.ProtoVersion))
}

// Prepare prepares statement for repeated execution. The returned handle is
// immutable and safe to share between goroutines. At most one execopt.PrepareOptions
// may be passed; omitting it uses the session keyspace and any eligible host.
// Prepare is bounded by ctx and the session's default profile Timeout,
// whichever expires first. Concurrent calls for the same statement and
// keyspace share one PREPARE; a call never fails because another caller's
// context ended.
func (s *Session) Prepare[P, R any](ctx context.Context, statement cql.Statement[P, R], options ...execopt.PrepareOptions) (*Prepared[P, R], error) {
	var o implSession.PrepareOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.PrepareOptions{Keyspace: options[0].Keyspace, HostID: options[0].HostID}
	default:
		return nil, tooManyPrepareOptions()
	}
	p, err := s.internal().Prepare(ctx, internalStatement(statement), o)
	return (*Prepared[P, R])(p), err
}

// PrepareStream prepares a statement for decoding into caller-owned storage.
// The immutable handle can be shared between workers. It validates the streaming
// decoder against server metadata when available; conditional result metadata
// unavailable during preparation is checked on execution. At most one
// execopt.PrepareOptions may be supplied. Its time limits are those of Prepare.
func (s *Session) PrepareStream[P, R any](ctx context.Context, statement cql.StreamStatement[P, R], options ...execopt.PrepareOptions) (*PreparedStream[P, R], error) {
	var o implSession.PrepareOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.PrepareOptions{Keyspace: options[0].Keyspace, HostID: options[0].HostID}
	default:
		return nil, tooManyPrepareOptions()
	}
	p, err := s.internal().PrepareStream(ctx, internalStreamStatement(statement), o)
	return (*PreparedStream[P, R])(p), err
}

// CompileOptions validates options and compiles them for this session: it
// runs the checks that depend on the session's dialect now, rather than at
// each execution, resolves the overrides against the session's defaults once,
// and picks the compiled form the session's protocol uses. It is the only way
// to make an execopt.CompiledOptions other than the zero value, which uses the
// session defaults. An execopt.CompiledOptions used on another session applies
// its original overrides to that session's defaults and is resolved again on
// each call. Use context deadlines for per-request time limits. Policy and
// observer objects remain shared and must be safe for concurrent use. An
// invalid value is rejected with a cql.ValidationError, and a setting the
// dialect lacks with a cqlerr.UnsupportedSettingError.
func (s *Session) CompileOptions(options execopt.Options) (execopt.CompiledOptions, error) {
	o, err := s.internal().CompileOptions(internalOptions(options))
	return execopt.CompiledOptions(o), err
}

// MustCompileOptions is CompileOptions that panics on error, for options
// written from constants and compiled once at setup, such as the defaults
// of a Prepared.With view:
//
//	quorum := session.MustCompileOptions(execopt.Options{Consistency: cql.Some(cql.Quorum)})
func (s *Session) MustCompileOptions(options execopt.Options) execopt.CompiledOptions {
	o, err := s.CompileOptions(options)
	if err != nil {
		panic(err)
	}
	return o
}

// UnpreparedExec executes an occasional statement without preparing it or
// leaving a worker to close. It must return no result columns. Parameters are
// described by statement.BindMarkers, which a statement with parameters (P
// other than cql.EmptyParams) must set: without them the call fails with a
// cql.ValidationError before anything is sent. With cql.DynamicParams and no
// BindMarkers, the markers take the types of the values. At most one
// execopt.CompiledOptions may be passed; omitting it uses the session defaults.
func (s *Session) UnpreparedExec[P any](ctx context.Context, statement cql.Statement[P, cql.EmptyRow], value P, options ...execopt.CompiledOptions) (cql.Result, error) {
	return s.internal().UnpreparedExec(ctx, internalStatement(statement), value, internalOptionList(options)...)
}

// UnpreparedCommand executes statement, CQL text without bind markers or
// result columns such as DDL, once and unprepared. It is
// UnpreparedExec(ctx, cql.Write(statement, cql.NoParams), cql.EmptyParams{}, options...)
// without the statement and parameter values to spell out, and so a
// documented exception to the execution grid: the none cell of the throwaway
// row for a statement with nothing to bind. At most one execopt.CompiledOptions may
// be passed, as for UnpreparedExec.
func (s *Session) UnpreparedCommand(ctx context.Context, statement string, options ...execopt.CompiledOptions) (cql.Result, error) {
	return s.internal().UnpreparedExec(ctx, internalStatement(cql.Write(statement, cql.NoParams)), cql.EmptyParams{}, internalOptionList(options)...)
}

// UnpreparedOne executes an occasional statement and decodes its first row.
// It reports whether a row exists, clearing dst if none exists. Borrowed fields
// are detached before return; there is no worker or cursor to close. Parameters
// are described by statement.BindMarkers, as for UnpreparedExec. With a nil
// dst it only reports whether a row exists. At most one execopt.CompiledOptions may be
// passed; omitting it uses the session defaults.
func (s *Session) UnpreparedOne[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...execopt.CompiledOptions) (bool, cql.Result, error) {
	return s.internal().UnpreparedOne(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// UnpreparedEach executes an occasional statement, calling fn for every row
// across all pages. Borrowed fields are valid only during fn. Returning StopIteration
// from fn stops successfully; other callback errors are returned. There is no worker or cursor to close. Parameters are
// described by statement.BindMarkers, as for UnpreparedExec. At most one
// execopt.CompiledOptions may be passed; omitting it uses the session defaults. dst is decoded into as for Worker.Each: the
// same pointer for every row, or a new R for each row when dst is nil.
func (s *Session) UnpreparedEach[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, fn func(*R) error, options ...execopt.CompiledOptions) (cql.Result, error) {
	return s.internal().UnpreparedEach(ctx, internalStatement(statement), value, dst, fn, internalOptionList(options)...)
}

// UnpreparedAll executes an occasional statement and iterates every row
// across all pages, decoding each into dst, as Worker.All does. The statement's
// execution is closed when the loop ends; there is nothing to close. Parameters are
// described by statement.BindMarkers, as for UnpreparedExec. At most one
// execopt.CompiledOptions may be passed; omitting it uses the session defaults.
func (s *Session) UnpreparedAll[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...execopt.CompiledOptions) iter.Seq2[*R, error] {
	return s.internal().UnpreparedAll(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// ExecBatch sends batch as one CQL BATCH request of batch.Kind. A batch known to be
// conditional is rejected with cqlerr.ErrConditionalExec before it is sent;
// send it with ApplyBatch, or with RunConditionalBatch to read its rows. At
// most one execopt.CompiledOptions
// may be passed; paging options are ignored, because a BATCH response is a
// single page. Options are compiled once with CompileOptions and reused, for a
// one-off batch too, so that sending resolves nothing. Do not modify batch
// until the call returns. A reused batch sends without allocating.
func (s *Session) ExecBatch(ctx context.Context, batch *Batch, options ...execopt.CompiledOptions) (cql.Result, error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return tooManyOptionsResult()
	}
	return s.internal().ExecBatch(ctx, (*implSession.Batch)(batch), o)
}

// RunConditionalBatch sends a conditional batch as one CQL BATCH request of
// batch.Kind and returns its rows as CASBatchRows, whose Applied reports
// whether the batch applied. decoder is a cql.CASRows decoder, typically the
// Rows of a member's statement; it maps the previous values of the columns it
// names and skips the rest. To read [applied] alone, pass cql.CASApplied
// (X is then cql.EmptyRow); ApplyBatch does that with nothing to close. A nil
// decoder does the same, but Go cannot infer X from it, so it needs an
// explicit type argument. Which rows the server returns differs between
// Cassandra and ScyllaDB: on ScyllaDB row i belongs to statement i, on
// Cassandra rows belong to rows of the partition. A batch without a
// conditional member, declared or inferred (see cql.UnknownStatement), is
// rejected before it is sent: it has no [applied] to report, so send it with
// ExecBatch. execopt.CompiledOptions and batch ownership are as for ExecBatch.
// The rows hold a pooled query until they are closed, as
// Prepared.Run's do. Once they are closed, a batch sent again with the same X
// and decoder reuses their storage and allocates nothing.
func (s *Session) RunConditionalBatch[X any](ctx context.Context, batch *Batch, decoder cql.RowDecoder[cql.CASResult[X]], options ...execopt.CompiledOptions) (CASBatchRows[X], error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return CASBatchRows[X]{}, tooManyOptions()
	}
	rows, err := s.internal().RunConditionalBatch(ctx, (*implSession.Batch)(batch), decoder, o)
	return CASBatchRows[X](rows), err
}

// ApplyBatch sends a conditional batch as one CQL BATCH request of
// batch.Kind and reports whether it applied, as RunConditionalBatch's
// CASBatchRows.Applied does, without the rows to close: it reads only the
// [applied] column, through cql.CASApplied, and releases the response before
// it returns. The Result is that of the execution, and with an error that
// of the failed execution, as for ExecBatch. A batch whose members are all
// known not to be conditional is rejected before it is sent, as by
// RunConditionalBatch. execopt.CompiledOptions and batch ownership are as for
// ExecBatch.
func (s *Session) ApplyBatch(ctx context.Context, batch *Batch, options ...execopt.CompiledOptions) (bool, cql.Result, error) {
	rows, err := s.RunConditionalBatch(ctx, batch, cql.CASApplied, options...)
	if err != nil {
		return false, implSession.InternalExecutionErrorResult(err), err
	}
	applied, err := rows.Applied()
	result := rows.Result()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, result, err
	}
	return applied, result, nil
}

// UnpreparedAddToBatch appends an occasional statement with value to batch
// without preparing it. Parameters are described by statement.BindMarkers, as
// for UnpreparedExec. The statement is validated and its parameters compiled on
// every call, so a statement added often is prepared once and added with
// Prepared.AddToBatch. A member added this way gives the batch no routing
// token; the batch routes by its first prepared member's token. Options for
// sending the batch are compiled once with CompileOptions, as for ExecBatch.
func (s *Session) UnpreparedAddToBatch[P, R any](batch *Batch, statement cql.Statement[P, R], value P) error {
	return s.internal().UnpreparedAddToBatch((*implSession.Batch)(batch), internalStatement(statement), value)
}

// ID returns the session's random identifier, sent to nodes as text in the
// SESSION_ID startup option. It remains valid after Close and can correlate
// client observations with system.clients.client_options, where ID().String()
// appears. A zero Session's ID is the zero UUID.
func (s *Session) ID() cql.UUID { return s.internal().ID() }

// AwaitSchemaAgreement waits until every host the host filter accepts
// reports the same schema version, bounded by ctx and the configured schema
// agreement timeout (config.ConfigBuilder.WithSchemaAgreement,
// default one minute). A host the filter accepts counts even while it is
// down or has no open connection: it cannot report a version, so the wait
// lasts until it comes back or the timeout ends. Exclude a host that is
// gone for good with the host filter.
//
// A failure is a *cqlerr.SchemaAgreementError: its Versions and Unreachable
// describe the last check, and its Err is cqlerr.ErrRequestTimeout when the
// agreement timeout ran out, ctx's error when ctx ended, a
// *cqlerr.NoHostsError when the filter accepts no host, or a check's error.
// A schema change statement waits the same way, unless
// config.SchemaChangeWaitNone is set, and fails with the same error, its
// Applied set.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	return s.internal().AwaitSchemaAgreement(ctx)
}

// Close stops new work, cancels in-flight requests and joins connections,
// writers and callbacks. It is idempotent. Retained output page leases remain
// valid. Rows, CASBatchRows, Page and RowStream handles left open report
// cqlerr.ErrClosed; close them as usual. Workers and StreamWorkers stop
// with the session: their later calls return cqlerr.ErrClosed, and closing
// them afterwards does nothing more. Prepared statements have no Close.
//
// Close cannot interrupt application code: it waits for a running Each or
// All callback, or loop body, to return, because the borrowed fields that
// code may hold stay valid until then. The cancelled iteration then ends at
// its next row. A callback that blocks therefore blocks Close.
func (s *Session) Close() error { return s.internal().Close() }

// Closed reports whether session shutdown has started.
func (s *Session) Closed() bool { return s.internal().Closed() }

// Metadata returns the current immutable cluster metadata snapshot.
func (s *Session) Metadata() *metadata.Snapshot { return s.internal().Metadata().Public() }

// Observer returns the hooks the session was configured with
// (config.ConfigBuilder.WithObserver), the zero Hooks without them or for a
// zero Session. Every execution calls them unless its execopt.Options set
// NoSessionObserver.
func (s *Session) Observer() observe.Hooks { return s.internal().Observer() }

// DefaultProfile returns a copy of the session's default profile
// (config.ConfigBuilder.WithDefaultProfile), the one every execution starts
// from. Derive a per-call profile from it, so that the settings it does not
// change stay the session's:
//
//	reads := session.DefaultProfile().Builder().WithConsistency(cql.One).MustBuild()
//
// config.NewProfile, by contrast, starts from the driver's defaults. A zero
// Session returns the driver's default profile.
func (s *Session) DefaultProfile() config.Profile {
	p := s.internal().DefaultProfile()
	if p == nil {
		return implSettings.InternalProfileToPublic[config.Profile](implSettings.InternalDefaultProfile())
	}
	return implSettings.InternalProfileToPublic[config.Profile](*p)
}

// Metrics returns a copy of the session's counters; see package metrics.
func (s *Session) Metrics() metrics.Snapshot { return s.internal().Metrics() }

// HostMetrics appends a metrics.Host for each host the session has a
// connection pool for, in no particular order, to dst and returns the
// extended slice, as append does, so that a caller polling it can reuse one
// slice: hosts = session.HostMetrics(hosts[:0]). Each Host is read from
// counters its pool keeps, without stopping requests; reading the
// connections and requests in flight takes each pool's lock. Package
// cqltrace's reads are not counted. A zero Session appends nothing.
func (s *Session) HostMetrics(dst []metrics.Host) []metrics.Host {
	return s.internal().HostMetrics(dst)
}

// RefreshMetadata reloads the topology, the client routes, when the session
// uses them, and the schema Mode loads, then publishes a new Snapshot. A
// topology refresh already under way is shared rather than repeated. ctx
// bounds the wait; the queries themselves use the configured metadata
// timeout.
//
// A part that fails to reload keeps its previous metadata. A failed topology
// or client routes reload is a *cqlerr.PartialRefreshError naming it, and so
// is a keyspace whose schema failed to load. A failed schema listing returns
// the query's error. When both happen, the errors are joined: errors.As
// finds the PartialRefreshError.
// RefreshMetadata also returns ctx's error, cqlerr.ErrClosed once the
// session closes, and cqlerr.ErrNoControl when the session has no control
// connection.
//
// It returns the session's snapshot once the refresh ended, as Metadata
// would: the one published with what reloaded, also beside a
// PartialRefreshError, or an earlier one when the refresh did not run to its
// publication. A zero Session returns nil.
func (s *Session) RefreshMetadata(ctx context.Context) (*metadata.Snapshot, error) {
	err := s.internal().RefreshMetadata(ctx)
	return s.Metadata(), err
}

// Subscribe subscribes to matching session events. Close the subscription when
// done consuming it.
//
// Subscribe before reading Session.Metadata, so that no change falls between
// the snapshot and the first event. Each event is delivered after the
// metadata publication that reflects it, so a snapshot whose Generation is at
// least the event's already holds the change, as far as the session could
// load it: a schema refresh that failed is retried and published later,
// without a second event. When the queue overflows, the queued events are
// discarded and one KindOverflow event takes their place; the subscription
// stays open, and reading Session.Metadata again catches up.
func (s *Session) Subscribe(filter event.Filter) (*event.Subscription, error) {
	sub, err := s.internal().Subscribe(filter)
	return sub.Public(), err
}
