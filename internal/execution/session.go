// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
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

// InternalCore returns the cluster state s wraps, or nil for a nil s.
func InternalCore(s *Session) *implCluster.Core {
	if s == nil {
		return nil
	}
	return s.core
}

// InternalSessionData returns the storage of the cluster state s wraps.
func InternalSessionData(s *Session) *implCluster.SessionStorage {
	return implCluster.SessionData(InternalCore(s))
}

// Prepare prepares statement on s. At most one PrepareOptions may be given;
// none prepares with the session's keyspace on any host.
func (s *Session) Prepare[P, R any](ctx context.Context, statement Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	var o PrepareOptions
	switch len(options) {
	case 0:
	case 1:
		o = options[0]
	default:
		return nil, errors.New("gocql: at most one PrepareOptions")
	}
	return Prepare(ctx, InternalCore(s), statement, o)
}

// Query returns a reusable query that runs statement on s without server
// preparation. It compiles parameters once against statement.BindMarkers and
// captures each execution's input in query-owned storage, including values
// passed to custom codecs.
func (s *Session) Query[P, R any](statement Statement[P, R], options OptionValues) (*Query[P, R], error) {
	return NewUnpreparedQuery(InternalCore(s), statement, options)
}

// StreamQuery returns a reusable query that runs statement on s without
// server preparation and streams each row into decoder.
func (s *Session) StreamQuery[P, B, R any](statement Statement[P, B], decoder StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	return NewUnpreparedStreamQuery(InternalCore(s), statement, decoder, options)
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
func (s *Session) Metadata() *implCluster.MetadataSnapshot { return s.core.Metadata() }

// Metrics returns a snapshot of the session's metrics.
func (s *Session) Metrics() MetricsSnapshot { return InternalCore(s).Metrics() }

// RefreshMetadata waits for the shared topology refresh under the caller's
// context. Metadata queries themselves use the session metadata timeout.
func (s *Session) RefreshMetadata(ctx context.Context) error { return s.core.RefreshMetadata(ctx) }

// Trace reads tracing tables through the same typed QUERY/codec path as user
// requests. Readiness polling is governed by one context across every round.
func (s *Session) Trace(ctx context.Context, id UUID) (implCluster.TraceInfo, error) {
	return s.core.Trace(ctx, id)
}

// Subscribe returns a subscription to the session's events that match
// filter.
func (s *Session) Subscribe(filter implCluster.EventFilter) (*implCluster.Subscription, error) {
	return s.core.Subscribe(filter)
}
