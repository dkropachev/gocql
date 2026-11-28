// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metrics"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// Session owns cluster metadata, pooled connections and typed executions.
// It is safe for concurrent use. Connect constructs validated sessions.
type Session struct {
	core *implCluster.Core
}

// InternalWrapSession returns the Session for core, or nil for a nil core.
func InternalWrapSession(core *implCluster.Core) *Session {
	if core == nil {
		return nil
	}
	return &Session{core: core}
}

// Core returns the cluster state s wraps, or nil for a nil s.
func (s *Session) Core() *implCluster.Core {
	if s == nil {
		return nil
	}
	return s.core
}

// Data returns the storage of the cluster state s wraps.
func (s *Session) Data() *implCluster.SessionStorage {
	return implCluster.SessionData(s.Core())
}

// Prepare prepares statement on s. The zero PrepareOptions prepares with the
// session's keyspace on any host.
func (s *Session) Prepare[P, R any](ctx context.Context, statement binding.Statement[P, R], options PrepareOptions) (*Prepared[P, R], error) {
	return Prepare(ctx, s.Core(), statement, options)
}

// CompileOptions validates options and resolves them for s into CompiledOptions,
// for one-shot executions; the zero Options gives the zero CompiledOptions. The
// custom payload and the profile are copied, so later changes to options do
// not reach the result. CompiledOptions executed on another session are resolved
// again for it on each execution.
func (s *Session) CompileOptions(options Options) (CompiledOptions, error) {
	if s == nil {
		return CompiledOptions{}, cqltype.TypeValidation("session", "nil session")
	}
	if s.core == nil {
		return CompiledOptions{}, errZeroSession()
	}
	return InternalCompileOptions(s.Core(), options)
}

// ExecBatch sends batch as one CQL BATCH of batch.Kind on s. The batch's
// statements must produce no result columns; RunConditionalBatch reads the rows of a
// conditional batch. The zero CompiledOptions uses the session's default
// profile. ExecBatch is safe for concurrent use with other session calls, but
// a Batch must not be changed while it is sent.
func (s *Session) ExecBatch(ctx context.Context, batch *Batch, options CompiledOptions) (cql.Result, error) {
	if s == nil {
		err := notSent(cqltype.TypeValidation("session", "nil session"))
		return InternalExecutionErrorResult(err), err
	}
	core := s.Core()
	q, err := beginBatch(ctx, core, batch)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	batch.queryExec(&batch.exec.privateStorage.InternalRequest, &q.state)
	result, err := q.state.runtime.exec(ctx, &batch.exec, options)
	q.state.owner.InternalRelease()
	return result, err
}

// RunBatch sends batch as one CQL BATCH of batch.Kind on s and returns its rows,
// decoded with decoder. A conditional batch returns rows reporting whether it
// applied; which rows depends on the server (docs/server-differences.md). A
// BATCH response is a single page, so paging options are ignored. Options are
// otherwise as for ExecBatch. The rows hold a pooled query until they are
// closed, as Prepared.Run's do. A batch sent again once its rows are closed
// reuses their storage, and allocates nothing while R and decoder stay the
// same.
func (s *Session) RunBatch[R any](ctx context.Context, batch *Batch, decoder cql.RowDecoder[R], options CompiledOptions) (Rows[R], error) {
	if s == nil {
		return Rows[R]{}, notSent(cqltype.TypeValidation("session", "nil session"))
	}
	if decoder == nil {
		return Rows[R]{}, notSent(cqltype.TypeValidation("batch rows", "nil decoder"))
	}
	_, rows, err := sendBatchRows(ctx, s.Core(), batch, decoder, options)
	return rows, err
}

// UnpreparedAddToBatch appends statement with value to batch without
// preparing it. Parameters are described by statement.BindMarkers, as for
// UnpreparedExec. The parameter plan is compiled on every call; a statement
// added often is prepared once and added with Prepared.AddToBatch. A member
// added this way carries no routing token.
func (s *Session) UnpreparedAddToBatch[P, R any](batch *Batch, statement binding.Statement[P, R], value P) error {
	if s == nil {
		return cqltype.TypeValidation("session", "nil session")
	}
	if batch == nil {
		return cqltype.TypeValidation("batch", "nil batch")
	}
	core := s.Core()
	if core == nil {
		return errZeroSession()
	}
	if core.Closed() {
		return cqlerr.ErrClosed
	}
	if err := binding.ValidateStatement(&statement); err != nil {
		return err
	}
	if err := unpreparedMarkers(&statement, value); err != nil {
		return err
	}
	plan, err := codec.CompileParams(statement.Params, statement.BindMarkers)
	if err != nil {
		return err
	}
	routing := routing2.Info{Kind: statement.Kind}
	return batch.addValue(core, cqlText{}, statement.CQL, &routing, statement.Idempotent, plan, statement.BindMarkers, value, true)
}

// ID returns the globally unique identifier of this session. The driver sends
// it to every node in the SESSION_ID startup option, where it appears in the
// system.clients.client_options column. Logging it, recording it in a support
// bundle or attaching it as a metric label is what allows client-side
// observations to be matched against those rows, instead of correlating by
// address and port.
//
// The id is assigned when the session is created and never changes, so it is
// safe to call concurrently and safe to read after the session is closed.
func (s *Session) ID() cql.UUID {
	if s.Core() == nil {
		return cql.UUID{}
	}
	return s.core.ID()
}

// errZeroSession is the error of a zero Session's methods.
func errZeroSession() error {
	return cqltype.TypeValidation("session", "zero Session; open one with gocql.Connect")
}

// AwaitSchemaAgreement waits until schema versions across all nodes in the
// cluster are the same, as seen from the control connection. The
// MaxWaitSchemaAgreement setting (default: 60s) bounds the wait; when it
// elapses with versions still differing, AwaitSchemaAgreement returns an
// error.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	if s.Core() == nil {
		return errZeroSession()
	}
	return s.core.AwaitSchemaAgreement(ctx)
}

// Close closes the session, its connections and its in-flight executions.
func (s *Session) Close() error {
	if s.Core() == nil {
		return errZeroSession()
	}
	return s.core.Close()
}

// Closed reports whether Close has been called. A zero Session was never
// open, so it reports true.
func (s *Session) Closed() bool { return s.Core() == nil || s.core.Closed() }

// Metadata returns the current cluster metadata snapshot, or nil for a zero
// Session.
func (s *Session) Metadata() *routing.MetadataSnapshot {
	if s.Core() == nil {
		return nil
	}
	return s.core.Metadata()
}

// Metrics returns a snapshot of the session's metrics.
func (s *Session) Metrics() metrics.Snapshot { return s.Core().Metrics() }

// HostMetrics appends a metrics.Host for each host the session has a pool
// for to dst and returns the extended slice.
func (s *Session) HostMetrics(dst []metrics.Host) []metrics.Host { return s.Core().HostMetrics(dst) }

// RefreshMetadata waits for the shared topology refresh under the caller's
// context. Metadata queries themselves use the session metadata timeout.
func (s *Session) RefreshMetadata(ctx context.Context) error {
	if s.Core() == nil {
		return errZeroSession()
	}
	return s.core.RefreshMetadata(ctx)
}

// Subscribe returns a subscription to the session's events that match
// filter.
func (s *Session) Subscribe(filter event.Filter) (*implCluster.Subscription, error) {
	if s.Core() == nil {
		return nil, errZeroSession()
	}
	return s.core.Subscribe(filter)
}
