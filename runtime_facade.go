// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// Batch collects statements, each with its values, to send as one CQL
// BATCH request: Query.AddToBatch appends a statement, Session.ExecBatch and
// Session.RunBatch send the batch, and Reset empties it for reuse. Once a
// batch has grown to its working size, appending allocates nothing. The
// zero Batch is ready to use; a Batch is not safe for concurrent use and
// must not be copied.
type Batch implSession.Batch

// StopIteration may be returned by an Each callback to stop successfully.
// A single-error wrapper has the same effect; joined errors remain failures.
// The execution is recorded as successful and all response storage is released.
// Other callback errors are returned unchanged. Borrowed callback fields still
// expire when the callback returns.
var StopIteration = implSession.StopIteration

// Len returns the number of statements in the batch.
func (b *Batch) Len() int { return (*implSession.Batch)(b).Len() }

// Reset clears statements while preserving the batch's reusable capacity.
func (b *Batch) Reset() { (*implSession.Batch)(b).Reset() }

const BatchSizeMaximum = implCluster.BatchSizeMaximum

const ClientRouteEvent = implCluster.ClientRouteEvent

// Connect opens a session with config, which must come from
// ConfigBuilder.Build.
func Connect(ctx context.Context, config Config) (*Session, error) {
	settings, err := config.resolved()
	if err != nil {
		return nil, err
	}
	session, err := implSession.Connect(ctx, settings)
	return (*Session)(session), err
}

const ControlEvent = implCluster.ControlEvent

type Event = implCluster.Event
type EventFilter = implCluster.EventFilter
type EventKind = implCluster.EventKind

// OpenSingleHost opens a session that uses only the node hostID.
func OpenSingleHost(ctx context.Context, config Config, hostID cql.UUID) (*Session, error) {
	settings, err := config.resolved()
	if err != nil {
		return nil, err
	}
	session, err := implSession.OpenSingleHost(ctx, settings, hostID)
	return (*Session)(session), err
}

const OverflowEvent = implCluster.OverflowEvent

// Prepare prepares statement on session. At most one PrepareOptions may be
// given; none prepares with the session's keyspace on any host.
//
// Deprecated: use (*Session).Prepare.
func Prepare[P any, R any](ctx context.Context, session *Session, statement cql.Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	return session.Prepare(ctx, statement, options...)
}

type PreparedInfo = implSession.PreparedInfo

const SchemaEvent = implCluster.SchemaEvent

const StatusEvent = implCluster.StatusEvent

// StreamInfo reports delivered rows, pages and execution progress.
type StreamInfo = implSession.StreamInfo

type Subscription = implCluster.Subscription

const TopologyEvent = implCluster.TopologyEvent

type TraceEntry = implCluster.TraceEntry
type TraceEvent = implCluster.TraceEvent
type TraceInfo = implCluster.TraceInfo
