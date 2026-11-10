// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// Batch collects statements, each with its values, to send as one CQL
// BATCH request: Query.AddToBatch appends a statement, Query.ExecBatch and
// Query.RunBatch send the batch, and Reset empties it for reuse. Once a
// batch has grown to its working size, appending allocates nothing. The
// zero Batch is ready to use; a Batch is not safe for concurrent use and
// must not be copied.
type Batch = implExecution.Batch

const BatchSizeMaximum = implCluster.BatchSizeMaximum

const ClientRouteEvent = implCluster.ClientRouteEvent

// Connect opens a session with config, which must come from
// ConfigBuilder.Build.
func Connect(ctx context.Context, config Config) (*Session, error) {
	settings, err := config.resolved()
	if err != nil {
		return nil, err
	}
	return implExecution.Connect(ctx, settings)
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
	return implExecution.OpenSingleHost(ctx, settings, hostID)
}

// Options lists the settings of an execution. The zero Options uses the
// session's default profile. Query constructors take Options and validate and
// resolve them for their session once, so query executions do no per-call
// work; one-shot executions take BoundOptions from Session.BindOptions
// instead.
type Options = implExecution.Options

// BoundOptions are Options validated, resolved against one session's default
// profile and keyspace, and encoded once by Session.BindOptions, for one-shot
// executions such as Prepared.Exec and Session.ExecBatch. They are immutable
// and safe to share between goroutines. The zero BoundOptions uses the
// executing session's default profile.
type BoundOptions = implExecution.BoundOptions

const OverflowEvent = implCluster.OverflowEvent

type Page[R any] = implExecution.Page[R]
type PageLease = implExecution.PageLease

// Prepare prepares statement on session. At most one PrepareOptions may be
// given; none prepares with the session's keyspace on any host.
//
// Deprecated: use (*Session).Prepare.
func Prepare[P any, R any](ctx context.Context, session *Session, statement cql.Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	return session.Prepare(ctx, statement, options...)
}

type PrepareOptions = implExecution.PrepareOptions
type Prepared[P any, R any] = implExecution.Prepared[P, R]
type PreparedInfo = implExecution.PreparedInfo

type RowStream[R any] = implExecution.RowStream[R]
type Rows[R any] = implExecution.Rows[R]

const SchemaEvent = implCluster.SchemaEvent

type Session = implExecution.Session

const StatusEvent = implCluster.StatusEvent

type StreamInfo = implExecution.StreamInfo
type StreamOptions = implExecution.StreamOptions
type Subscription = implCluster.Subscription

const TopologyEvent = implCluster.TopologyEvent

type TraceEntry = implCluster.TraceEntry
type TraceEvent = implCluster.TraceEvent
type TraceInfo = implCluster.TraceInfo
