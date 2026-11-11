// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"

	"github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"
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

// Prepare prepares statement on s. At most one PrepareOptions may be given;
// none prepares with the session's keyspace on any host.
func (s *Session) Prepare[P, R any](ctx context.Context, statement binding.Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	var o PrepareOptions
	switch len(options) {
	case 0:
	case 1:
		o = options[0]
	default:
		return nil, errors.New("gocql: at most one PrepareOptions")
	}
	return Prepare(ctx, s.Core(), statement, o)
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
	return InternalCompileOptions(s.Core(), options)
}

// ExecBatch sends batch as one CQL BATCH of kind on s. The batch's
// statements must produce no result columns; RunBatch reads the rows of a
// conditional batch. At most one CompiledOptions may be given; none uses the
// session's default profile. ExecBatch is safe for concurrent use with other
// session calls, but a Batch must not be changed while it is sent.
func (s *Session) ExecBatch(ctx context.Context, kind cqltype.BatchKind, batch *Batch, options ...CompiledOptions) (model.Result, error) {
	if s == nil {
		err := cqltype.TypeValidation("session", "nil session")
		return InternalExecutionErrorResult(err), err
	}
	core := s.Core()
	q, o, err := beginBatch(ctx, core, kind, batch, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	batch.queryExec(&batch.exec.privateStorage.InternalRequest, &q.state, kind)
	result, err := q.state.runtime.exec(ctx, &batch.exec, o)
	q.state.owner.InternalRelease()
	return result, err
}

// RunBatch sends batch as one CQL BATCH of kind on s and returns its rows,
// decoded with decoder. A conditional batch returns one row per conditional
// statement, or one row reporting that it was not applied. Options are as for
// ExecBatch. Close the page when done with it.
func (s *Session) RunBatch[R any](ctx context.Context, kind cqltype.BatchKind, batch *Batch, decoder codec.RowDecoder[R], options ...CompiledOptions) (*Page[R], error) {
	if s == nil {
		return nil, cqltype.TypeValidation("session", "nil session")
	}
	if decoder == nil {
		return nil, cqltype.TypeValidation("batch rows", "nil decoder")
	}
	core := s.Core()
	q, o, err := beginBatch(ctx, core, kind, batch, options)
	if err != nil {
		return nil, err
	}
	bound := &Bound[R]{privateStorage: BoundStorage[R]{InternalRows: decoder}}
	batch.queryExec(&bound.privateStorage.InternalRequest, &q.state, kind)
	page, err := q.state.runtime.runSinglePage(ctx, bound, model.StartPage(), 0, o)
	q.state.owner.InternalRelease()
	return page, err
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
func (s *Session) ID() string { return s.core.ID() }

// AwaitSchemaAgreement waits until schema versions across all nodes in the
// cluster are the same, as seen from the control connection. The
// MaxWaitSchemaAgreement setting (default: 60s) bounds the wait; when it
// elapses with versions still differing, AwaitSchemaAgreement returns an
// error.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	return s.core.AwaitSchemaAgreement(ctx)
}

// Close closes the session, its connections and its in-flight executions.
func (s *Session) Close() error { return s.core.Close() }

// Closed reports whether Close has been called.
func (s *Session) Closed() bool { return s.core.Closed() }

// Metadata returns the current cluster metadata snapshot.
func (s *Session) Metadata() *routing.MetadataSnapshot { return s.core.Metadata() }

// Metrics returns a snapshot of the session's metrics.
func (s *Session) Metrics() routing.MetricsSnapshot { return s.Core().Metrics() }

// RefreshMetadata waits for the shared topology refresh under the caller's
// context. Metadata queries themselves use the session metadata timeout.
func (s *Session) RefreshMetadata(ctx context.Context) error { return s.core.RefreshMetadata(ctx) }

// Trace reads tracing tables through the same typed QUERY/codec path as user
// requests. Readiness polling is governed by one context across every round.
func (s *Session) Trace(ctx context.Context, id cqltype.UUID) (implCluster.TraceInfo, error) {
	return s.core.Trace(ctx, id)
}

// Subscribe returns a subscription to the session's events that match
// filter.
func (s *Session) Subscribe(filter implCluster.EventFilter) (*implCluster.Subscription, error) {
	return s.core.Subscribe(filter)
}
