// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// Session owns cluster metadata, pooled connections and typed executions.
// Connect constructs a session. Sessions and their prepared statements are safe
// for concurrent use. Close the session when the application stops using it.
type Session implSession.Session

func (s *Session) internal() *implSession.Session { return (*implSession.Session)(s) }

// Prepare prepares statement for repeated execution. The returned handle is
// immutable and safe to share between goroutines. At most one PrepareOptions
// may be passed; omitting it uses the session keyspace and any eligible host.
func (s *Session) Prepare[P, R any](ctx context.Context, statement cql.Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	o, err := internalPrepareOptions(options)
	if err != nil {
		return nil, err
	}
	p, err := s.internal().Prepare(ctx, internalStatement(statement), o)
	return (*Prepared[P, R])(p), err
}

// PrepareStream prepares a statement for decoding into caller-owned storage.
// The immutable handle can be shared between workers. It validates the streaming
// decoder against server metadata when available; conditional result metadata
// unavailable during preparation is checked on execution. At most one
// PrepareOptions may be supplied.
func (s *Session) PrepareStream[P, R any](ctx context.Context, statement cql.StreamStatement[P, R], options ...PrepareOptions) (*PreparedStream[P, R], error) {
	o, err := internalPrepareOptions(options)
	if err != nil {
		return nil, err
	}
	p, err := s.internal().PrepareStream(ctx, internalStreamStatement(statement), o)
	return (*PreparedStream[P, R])(p), err
}

// CompileOptions validates and snapshots overrides for reuse by one-shot calls.
// Omit options on those calls to use the session defaults. A CompiledOptions used
// on another session applies its original overrides to that session's defaults
// and is resolved again on each call. Use context deadlines for per-request
// time limits. Policy and observer objects remain shared and must be safe for
// concurrent use.
func (s *Session) CompileOptions(options Options) (CompiledOptions, error) {
	o, err := s.internal().CompileOptions(internalOptions(options))
	return CompiledOptions(o), err
}

// UnpreparedExec executes an occasional statement without preparing it or
// leaving a query to close. It must return no result columns. Parameters are
// described by statement.BindMarkers. At most one Options may be passed; the
// call validates and resolves it, and omitting it uses the session defaults.
func (s *Session) UnpreparedExec[P any](ctx context.Context, statement cql.Statement[P, cql.EmptyRow], value P, options ...Options) (Result, error) {
	return s.internal().UnpreparedExec(ctx, internalStatement(statement), value, internalOptionList(options)...)
}

// UnpreparedOne executes an occasional statement and decodes its first row.
// It reports whether a row exists, clearing dst if none exists. Borrowed fields
// are detached before return; there is no query or cursor to close. Parameters
// are described by statement.BindMarkers. At most one Options may be passed;
// the call validates and resolves it, and omitting it uses the session defaults.
func (s *Session) UnpreparedOne[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...Options) (bool, Result, error) {
	return s.internal().UnpreparedOne(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// UnpreparedEach executes an occasional statement, calling fn for every row
// across all pages. Borrowed fields are valid only during fn. Returning StopIteration
// from fn stops successfully; other callback errors are returned. There is no query or cursor to close. Parameters are
// described by statement.BindMarkers. At most one Options may be passed; the
// call validates and resolves it, and omitting it uses the session defaults.
// dst is decoded into for every row. With cql.ReuseBuffers, or a RowInto reset
// that keeps slices, []byte fields reuse their capacity, so a slice kept from
// one call of fn aliases the next row's data: keep bytes.Clone(row.Field).
func (s *Session) UnpreparedEach[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, fn func(*R) error, options ...Options) (Result, error) {
	return s.internal().UnpreparedEach(ctx, internalStatement(statement), value, dst, fn, internalOptionList(options)...)
}

// UnpreparedAll executes an occasional statement and iterates every row
// across all pages, decoding each into dst, as Query.All does. The statement's
// query is closed when the loop ends; there is nothing to close. Parameters are
// described by statement.BindMarkers. At most one Options may be passed; the
// call validates and resolves it, and omitting it uses the session defaults.
func (s *Session) UnpreparedAll[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...Options) iter.Seq2[*R, error] {
	return s.internal().UnpreparedAll(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// ExecBatch sends batch as one CQL BATCH request. Its statements must return no
// columns; use RunBatch for conditional results. At most one CompiledOptions may be
// passed. Do not modify batch until the call returns.
func (s *Session) ExecBatch(ctx context.Context, kind cql.BatchKind, batch *Batch, options ...CompiledOptions) (Result, error) {
	o, err := internalCompiledOptions(options)
	if err != nil {
		return implSession.InternalExecutionErrorResult(err), err
	}
	return s.internal().ExecBatch(ctx, kind, (*implSession.Batch)(batch), o)
}

// RunBatch executes a conditional batch and returns its result page, decoded
// with decoder. Close the page when done. Options and batch ownership are as for
// ExecBatch.
func (s *Session) RunBatch[R any](ctx context.Context, kind cql.BatchKind, batch *Batch, decoder cql.RowDecoder[R], options ...CompiledOptions) (*Page[R], error) {
	o, err := internalCompiledOptions(options)
	if err != nil {
		return nil, err
	}
	page, err := s.internal().RunBatch(ctx, kind, (*implSession.Batch)(batch), decoder, o)
	return (*Page[R])(page), err
}

// ID returns the session identifier sent to nodes in the SESSION_ID startup
// option. It remains valid after Close and can correlate client observations
// with system.clients.client_options.
func (s *Session) ID() string { return s.internal().ID() }

// AwaitSchemaAgreement waits for nodes' schema versions to agree, bounded by
// ctx and the configured schema agreement timeout.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	return s.internal().AwaitSchemaAgreement(ctx)
}

// Close stops new work, cancels in-flight requests and joins connections,
// writers and callbacks. Retained output page leases remain valid.
func (s *Session) Close() error { return s.internal().Close() }

// Closed reports whether session shutdown has started.
func (s *Session) Closed() bool { return s.internal().Closed() }

// Metadata returns the current immutable cluster metadata snapshot.
func (s *Session) Metadata() *metadata.Snapshot { return s.internal().Metadata() }

// Metrics returns the session's current metrics snapshot.
func (s *Session) Metrics() routing.MetricsSnapshot { return s.internal().Metrics() }

// RefreshMetadata waits for a shared topology refresh under ctx. The queries
// performing that refresh use the configured metadata timeout.
func (s *Session) RefreshMetadata(ctx context.Context) error {
	return s.internal().RefreshMetadata(ctx)
}

// Trace reads a trace's events, polling for readiness within ctx.
func (s *Session) Trace(ctx context.Context, id cql.UUID) (TraceInfo, error) {
	return s.internal().Trace(ctx, id)
}

// Subscribe subscribes to matching session events. Close the subscription when
// done consuming it.
func (s *Session) Subscribe(filter EventFilter) (*Subscription, error) {
	return s.internal().Subscribe(filter)
}
