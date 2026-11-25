// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/metrics"
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
// immutable and safe to share between goroutines. At most one exec.PrepareOptions
// may be passed; omitting it uses the session keyspace and any eligible host.
// Prepare is bounded by ctx and the session's default profile Timeout,
// whichever expires first. Concurrent calls for the same statement and
// keyspace share one PREPARE; a call never fails because another caller's
// context ended.
func (s *Session) Prepare[P, R any](ctx context.Context, statement cql.Statement[P, R], options ...exec.PrepareOptions) (*Prepared[P, R], error) {
	var o implSession.PrepareOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.PrepareOptions{Keyspace: options[0].Keyspace, HostID: options[0].HostID, CQLTrace: options[0].CQLTrace}
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
// exec.PrepareOptions may be supplied. Its time limits are those of Prepare.
func (s *Session) PrepareStream[P, R any](ctx context.Context, statement cql.StreamStatement[P, R], options ...exec.PrepareOptions) (*PreparedStream[P, R], error) {
	var o implSession.PrepareOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.PrepareOptions{Keyspace: options[0].Keyspace, HostID: options[0].HostID, CQLTrace: options[0].CQLTrace}
	default:
		return nil, tooManyPrepareOptions()
	}
	p, err := s.internal().PrepareStream(ctx, internalStreamStatement(statement), o)
	return (*PreparedStream[P, R])(p), err
}

// CompileOptions validates and snapshots overrides for reuse by one-shot calls.
// Omit options on those calls to use the session defaults. An exec.CompiledOptions used
// on another session applies its original overrides to that session's defaults
// and is resolved again on each call. Use context deadlines for per-request
// time limits. Policy and observer objects remain shared and must be safe for
// concurrent use.
func (s *Session) CompileOptions(options exec.Options) (exec.CompiledOptions, error) {
	o, err := s.internal().CompileOptions(internalOptions(options))
	return exec.CompiledOptions(o), err
}

// UnpreparedExec executes an occasional statement without preparing it or
// leaving a worker to close. It must return no result columns. Parameters are
// described by statement.BindMarkers, which a statement with parameters (P
// other than cql.EmptyParams) must set: without them the call fails with a
// cql.ValidationError before anything is sent. With cql.DynamicParams and no
// BindMarkers, the markers take the types of the values. At most one
// exec.Options may be passed; the call validates and resolves it, and
// omitting it uses the session defaults.
func (s *Session) UnpreparedExec[P any](ctx context.Context, statement cql.Statement[P, cql.EmptyRow], value P, options ...exec.Options) (cql.Result, error) {
	return s.internal().UnpreparedExec(ctx, internalStatement(statement), value, internalOptionList(options)...)
}

// UnpreparedCommand executes statement, CQL text without bind markers or
// result columns such as DDL, once and unprepared. It is
// UnpreparedExec(ctx, cql.Command(statement), cql.EmptyParams{}, options...)
// without the statement and parameter values to spell out, and so a
// documented exception to the execution grid: the none cell of the throwaway
// row for a statement with nothing to bind. At most one exec.Options may be
// passed, as for UnpreparedExec.
func (s *Session) UnpreparedCommand(ctx context.Context, statement string, options ...exec.Options) (cql.Result, error) {
	return s.internal().UnpreparedExec(ctx, internalStatement(cql.Command(statement)), cql.EmptyParams{}, internalOptionList(options)...)
}

// UnpreparedOne executes an occasional statement and decodes its first row.
// It reports whether a row exists, clearing dst if none exists. Borrowed fields
// are detached before return; there is no worker or cursor to close. Parameters
// are described by statement.BindMarkers, as for UnpreparedExec. With a nil
// dst it only reports whether a row exists. At most one exec.Options may be passed;
// the call validates and resolves it, and omitting it uses the session defaults.
func (s *Session) UnpreparedOne[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...exec.Options) (bool, cql.Result, error) {
	return s.internal().UnpreparedOne(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// UnpreparedEach executes an occasional statement, calling fn for every row
// across all pages. Borrowed fields are valid only during fn. Returning StopIteration
// from fn stops successfully; other callback errors are returned. There is no worker or cursor to close. Parameters are
// described by statement.BindMarkers, as for UnpreparedExec. At most one
// exec.Options may be passed; the call validates and resolves it, and omitting
// it uses the session defaults. dst is decoded into as for Worker.Each: the
// same pointer for every row, or a new R for each row when dst is nil.
func (s *Session) UnpreparedEach[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, fn func(*R) error, options ...exec.Options) (cql.Result, error) {
	return s.internal().UnpreparedEach(ctx, internalStatement(statement), value, dst, fn, internalOptionList(options)...)
}

// UnpreparedAll executes an occasional statement and iterates every row
// across all pages, decoding each into dst, as Worker.All does. The statement's
// execution is closed when the loop ends; there is nothing to close. Parameters are
// described by statement.BindMarkers, as for UnpreparedExec. At most one
// exec.Options may be passed; the call validates and resolves it, and
// omitting it uses the session defaults.
func (s *Session) UnpreparedAll[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...exec.Options) iter.Seq2[*R, error] {
	return s.internal().UnpreparedAll(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// ExecBatch sends batch as one CQL BATCH request of batch.Kind. A batch known to be
// conditional is rejected with cqlerr.ErrConditionalExec before it is sent;
// send it with ApplyBatch, or with RunConditionalBatch to read its rows. At
// most one exec.CompiledOptions
// may be passed; paging options are ignored, because a BATCH response is a
// single page. Options are compiled once with CompileOptions and reused, for a
// one-off batch too, so that sending resolves nothing. Do not modify batch
// until the call returns. A reused batch sends without allocating.
func (s *Session) ExecBatch(ctx context.Context, batch *Batch, options ...exec.CompiledOptions) (cql.Result, error) {
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
// names and skips the rest. Which rows the server returns differs between
// Cassandra and ScyllaDB: on ScyllaDB row i belongs to statement i, on
// Cassandra rows belong to rows of the partition. A batch without a
// conditional member, declared or inferred (see cql.UnknownStatement), is
// rejected before it is sent: it has no [applied] to report, so send it with
// ExecBatch. exec.CompiledOptions and batch ownership are as for ExecBatch.
// The rows hold a pooled query until they are closed, as
// Prepared.Run's do. Once they are closed, a batch sent again with the same X
// and decoder reuses their storage and allocates nothing.
func (s *Session) RunConditionalBatch[X any](ctx context.Context, batch *Batch, decoder cql.RowDecoder[cql.CASResult[X]], options ...exec.CompiledOptions) (CASBatchRows[X], error) {
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
// RunConditionalBatch. exec.CompiledOptions and batch ownership are as for
// ExecBatch.
func (s *Session) ApplyBatch(ctx context.Context, batch *Batch, options ...exec.CompiledOptions) (bool, cql.Result, error) {
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

// AwaitSchemaAgreement waits for nodes' schema versions to agree, bounded by
// ctx and the configured schema agreement timeout.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	return s.internal().AwaitSchemaAgreement(ctx)
}

// Close stops new work, cancels in-flight requests and joins connections,
// writers and callbacks. It is idempotent. Retained output page leases remain
// valid. Rows, CASBatchRows, Page and RowStream handles left open report
// cqlerr.ErrClosed; close them as usual. Queries need no Close afterwards.
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

// Metrics returns a copy of the session's counters; see package metrics.
func (s *Session) Metrics() metrics.Snapshot { return s.internal().Metrics() }

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
func (s *Session) RefreshMetadata(ctx context.Context) error {
	return s.internal().RefreshMetadata(ctx)
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
