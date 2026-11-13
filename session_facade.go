// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/cqltrace"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/exec"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/metrics"
)

// Session owns cluster metadata, pooled connections and typed executions.
// Connect constructs a session of a Dialect: ScyllaV4, CassandraV4 or
// CassandraV5. The dialect is part of the type, so code that works with any
// cluster is generic over it. Prepared statements, queries and batches do not
// carry it. Sessions and their prepared statements are safe for concurrent
// use. Close the session when the application stops using it.
type Session[D Dialect] implSession.Session

func (s *Session[D]) internal() *implSession.Session { return (*implSession.Session)(s) }

// Prepare prepares statement for repeated execution. The returned handle is
// immutable and safe to share between goroutines. At most one exec.PrepareOptions
// may be passed; omitting it uses the session keyspace and any eligible host.
func (s *Session[D]) Prepare[P, R any](ctx context.Context, statement cql.Statement[P, R], options ...exec.PrepareOptions) (*Prepared[P, R], error) {
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
// exec.PrepareOptions may be supplied.
func (s *Session[D]) PrepareStream[P, R any](ctx context.Context, statement stream.Statement[P, R], options ...exec.PrepareOptions) (*PreparedStream[P, R], error) {
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
func (s *Session[D]) CompileOptions(options exec.Options) (exec.CompiledOptions, error) {
	o, err := s.internal().CompileOptions(internalOptions(options))
	return exec.CompiledOptions(o), err
}

// UnpreparedExec executes an occasional statement without preparing it or
// leaving a query to close. It must return no result columns. Parameters are
// described by statement.BindMarkers. At most one exec.Options may be passed; the
// call validates and resolves it, and omitting it uses the session defaults.
func (s *Session[D]) UnpreparedExec[P any](ctx context.Context, statement cql.Statement[P, cql.EmptyRow], value P, options ...exec.Options) (Result, error) {
	return s.internal().UnpreparedExec(ctx, internalStatement(statement), value, internalOptionList(options)...)
}

// UnpreparedOne executes an occasional statement and decodes its first row.
// It reports whether a row exists, clearing dst if none exists. Borrowed fields
// are detached before return; there is no query or cursor to close. Parameters
// are described by statement.BindMarkers. At most one exec.Options may be passed;
// the call validates and resolves it, and omitting it uses the session defaults.
func (s *Session[D]) UnpreparedOne[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...exec.Options) (bool, Result, error) {
	return s.internal().UnpreparedOne(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// UnpreparedEach executes an occasional statement, calling fn for every row
// across all pages. Borrowed fields are valid only during fn. Returning StopIteration
// from fn stops successfully; other callback errors are returned. There is no query or cursor to close. Parameters are
// described by statement.BindMarkers. At most one exec.Options may be passed; the
// call validates and resolves it, and omitting it uses the session defaults.
// dst is decoded into for every row. With cql.ReuseBuffers, or a RowInto reset
// that keeps slices, []byte fields reuse their capacity, so a slice kept from
// one call of fn aliases the next row's data: keep bytes.Clone(row.Field).
func (s *Session[D]) UnpreparedEach[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, fn func(*R) error, options ...exec.Options) (Result, error) {
	return s.internal().UnpreparedEach(ctx, internalStatement(statement), value, dst, fn, internalOptionList(options)...)
}

// UnpreparedAll executes an occasional statement and iterates every row
// across all pages, decoding each into dst, as Query.All does. The statement's
// query is closed when the loop ends; there is nothing to close. Parameters are
// described by statement.BindMarkers. At most one exec.Options may be passed; the
// call validates and resolves it, and omitting it uses the session defaults.
func (s *Session[D]) UnpreparedAll[P, R any](ctx context.Context, statement cql.Statement[P, R], value P, dst *R, options ...exec.Options) iter.Seq2[*R, error] {
	return s.internal().UnpreparedAll(ctx, internalStatement(statement), value, dst, internalOptionList(options)...)
}

// ExecBatch sends batch as one CQL BATCH request of batch.Kind. A batch known to be
// conditional is rejected with cqlerr.ErrConditionalExec before it is sent;
// read its result with RunBatch. At most one exec.CompiledOptions may be passed;
// paging options are ignored, because a BATCH response is a single page. Do
// not modify batch until the call returns. A reused batch sends without
// allocating.
func (s *Session[D]) ExecBatch(ctx context.Context, batch *Batch, options ...exec.CompiledOptions) (Result, error) {
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

// RunBatch sends batch as one CQL BATCH request of batch.Kind and returns its rows, decoded
// with decoder: the rows of a conditional batch report whether it applied.
// Which rows the server returns differs between Cassandra and ScyllaDB; the
// batch applied if and only if every row reports it. The rows hold a pooled
// query until they are closed, as Prepared.Run's do. exec.CompiledOptions and batch
// ownership are as for ExecBatch. Once its rows are closed, a batch sent again
// with the same R and decoder reuses their storage and allocates nothing.
func (s *Session[D]) RunBatch[R any](ctx context.Context, batch *Batch, decoder cql.RowDecoder[R], options ...exec.CompiledOptions) (Rows[R], error) {
	var o implSession.CompiledOptions
	switch len(options) {
	case 0:
	case 1:
		o = implSession.CompiledOptions(options[0])
	default:
		return Rows[R]{}, tooManyOptions()
	}
	rows, err := s.internal().RunBatch(ctx, (*implSession.Batch)(batch), decoder, o)
	return Rows[R](rows), err
}

// UnpreparedAddToBatch appends an occasional statement with value to batch
// without preparing it. Parameters are described by statement.BindMarkers, as
// for UnpreparedExec. The statement is validated and its parameters compiled on
// every call, so a statement added often is prepared once and added with
// Prepared.AddToBatch. A member added this way gives the batch no routing
// token; the batch routes by its first prepared member's token.
func (s *Session[D]) UnpreparedAddToBatch[P, R any](batch *Batch, statement cql.Statement[P, R], value P) error {
	return s.internal().UnpreparedAddToBatch((*implSession.Batch)(batch), internalStatement(statement), value)
}

// ID returns the session identifier sent to nodes in the SESSION_ID startup
// option. It remains valid after Close and can correlate client observations
// with system.clients.client_options.
func (s *Session[D]) ID() string { return s.internal().ID() }

// AwaitSchemaAgreement waits for nodes' schema versions to agree, bounded by
// ctx and the configured schema agreement timeout.
func (s *Session[D]) AwaitSchemaAgreement(ctx context.Context) error {
	return s.internal().AwaitSchemaAgreement(ctx)
}

// Close stops new work, cancels in-flight requests and joins connections,
// writers and callbacks. Retained output page leases remain valid.
func (s *Session[D]) Close() error { return s.internal().Close() }

// Closed reports whether session shutdown has started.
func (s *Session[D]) Closed() bool { return s.internal().Closed() }

// Metadata returns the current immutable cluster metadata snapshot.
func (s *Session[D]) Metadata() *metadata.Snapshot { return s.internal().Metadata() }

// Metrics returns a copy of the session's counters; see package metrics.
func (s *Session[D]) Metrics() metrics.Snapshot { return s.internal().Metrics() }

// RefreshMetadata waits for a shared topology refresh under ctx. The queries
// performing that refresh use the configured metadata timeout.
func (s *Session[D]) RefreshMetadata(ctx context.Context) error {
	return s.internal().RefreshMetadata(ctx)
}

// Trace reads a trace's events, polling for readiness within ctx.
func (s *Session[D]) Trace(ctx context.Context, id cql.UUID) (cqltrace.Info, error) {
	return s.internal().Trace(ctx, id)
}

// Subscribe subscribes to matching session events. Close the subscription when
// done consuming it.
func (s *Session[D]) Subscribe(filter event.Filter) (*event.Subscription, error) {
	return s.internal().Subscribe(filter)
}
