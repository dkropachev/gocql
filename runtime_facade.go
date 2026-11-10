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
// BATCH request: Query.AddToBatch appends a statement, Session.ExecBatch and
// Session.RunBatch send the batch, and Reset empties it for reuse. Once a
// batch has grown to its working size, appending allocates nothing. The
// zero Batch is ready to use; a Batch is not safe for concurrent use and
// must not be copied.
type Batch implExecution.Batch

// Len returns the number of statements in the batch.
func (b *Batch) Len() int { return (*implExecution.Batch)(b).Len() }

// Reset clears statements while preserving the batch's reusable capacity.
func (b *Batch) Reset() { (*implExecution.Batch)(b).Reset() }

const BatchSizeMaximum = implCluster.BatchSizeMaximum

const ClientRouteEvent = implCluster.ClientRouteEvent

// Connect opens a session with config, which must come from
// ConfigBuilder.Build.
func Connect(ctx context.Context, config Config) (*Session, error) {
	settings, err := config.resolved()
	if err != nil {
		return nil, err
	}
	session, err := implExecution.Connect(ctx, settings)
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
	session, err := implExecution.OpenSingleHost(ctx, settings, hostID)
	return (*Session)(session), err
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
// Reuse bound options on their originating session for precomputed settings;
// another session resolves their original overrides against its own defaults.
// Values returns a copy of the original overrides for deriving another set.
type BoundOptions = implExecution.BoundOptions

const OverflowEvent = implCluster.OverflowEvent

// Prepare prepares statement on session. At most one PrepareOptions may be
// given; none prepares with the session's keyspace on any host.
//
// Deprecated: use (*Session).Prepare.
func Prepare[P any, R any](ctx context.Context, session *Session, statement cql.Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	return session.Prepare(ctx, statement, options...)
}

type PrepareOptions = implExecution.PrepareOptions
type PreparedInfo = implExecution.PreparedInfo

const SchemaEvent = implCluster.SchemaEvent

const StatusEvent = implCluster.StatusEvent

// StreamInfo reports delivered rows, pages and execution progress.
type StreamInfo = implExecution.StreamInfo

// StreamOptions chooses caller-storage decoding and its receive mode. The zero
// value buffers a whole page on a multiplexed connection. Incremental reserves
// a connection and bounds receive scratch, with protocol/compression limits.
// Both modes require seeded output capacity and disable prefetch/speculation.
type StreamOptions = implExecution.StreamOptions
type Subscription = implCluster.Subscription

const TopologyEvent = implCluster.TopologyEvent

type TraceEntry = implCluster.TraceEntry
type TraceEvent = implCluster.TraceEvent
type TraceInfo = implCluster.TraceInfo
