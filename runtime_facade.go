// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

// Batch collects statements, each with its values, to send as one CQL
// BATCH request: Prepared.AddToBatch and Query.AddToBatch append a statement,
// Session.ExecBatch and Session.RunBatch send the batch, and Reset empties it
// for reuse. Once a batch has grown to its working size, appending allocates
// nothing. A Batch is not safe for concurrent use and must not be copied.
//
// The exported field Kind cql.BatchKind selects the batch's kind and is sent
// with every execution: cql.LoggedBatch (the zero value),
// cql.UnloggedBatch or cql.CounterBatch. It belongs to the batch rather than
// to each send because the contents decide it: a counter batch holds only
// counter updates, and an unlogged batch is chosen because its members share
// a partition. Set it once when building:
//
//	batch := gocql.Batch{Kind: cql.UnloggedBatch}
//
// The zero Batch is a logged batch, ready to use.
type Batch implSession.Batch

// StopIteration may be returned by an Each callback to stop successfully.
// A single-error wrapper has the same effect; joined errors remain failures.
// The execution is recorded as successful and all response storage is released.
// Other callback errors are returned unchanged. Borrowed callback fields still
// expire when the callback returns.
var StopIteration = implSession.StopIteration

// Len returns the number of statements in the batch.
func (b *Batch) Len() int { return (*implSession.Batch)(b).Len() }

// Reset clears statements while preserving the batch's reusable capacity and
// its Kind.
func (b *Batch) Reset() { (*implSession.Batch)(b).Reset() }

// MaxBatchStatements is the most statements one Batch can hold: the native
// protocol counts a BATCH request's statements in an unsigned 16-bit field.
// AddToBatch rejects the statement after it.
const MaxBatchStatements = implCluster.MaxBatchStatements

const ClientRouteEvent = implCluster.ClientRouteEvent

// Connect opens a session of dialect D with config, which must come from
// ConfigBuilder.Build. D fixes the protocol version and the database:
// Connect fails when the cluster does not speak that version, or is not that
// database (cqlerr.ServerMismatchError).
func Connect[D Dialect](ctx context.Context, config Config) (*Session[D], error) {
	settings, err := dialectSettings[D](config)
	if err != nil {
		return nil, err
	}
	session, err := implSession.Connect(ctx, settings)
	return (*Session[D])(session), err
}

const ControlEvent = implCluster.ControlEvent

type Event = implCluster.Event
type EventFilter = implCluster.EventFilter
type EventKind = implCluster.EventKind

const OverflowEvent = implCluster.OverflowEvent

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
