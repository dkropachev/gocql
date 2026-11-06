// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// Batch collects statements, each with its values, to send as one CQL
// BATCH request: Query.AddTo appends a statement, Query.ExecBatch and
// Query.RunBatch send the batch, and Reset empties it for reuse. Once a
// batch has grown to its working size, appending allocates nothing. The
// zero Batch is ready to use; a Batch is not safe for concurrent use and
// must not be copied.
type Batch = implExecution.Batch

const BatchSizeMaximum = implCluster.BatchSizeMaximum

const ClientRouteEvent = implCluster.ClientRouteEvent

func Connect(a0 context.Context, a1 Config) (*Session, error) { return implExecution.Connect(a0, a1) }

const ControlEvent = implCluster.ControlEvent

var ErrHostQueryFailed = implCluster.ErrHostQueryFailed
var ErrNoConnectionsStarted = implCluster.ErrNoConnectionsStarted
var ErrNoHosts = implCluster.ErrNoHosts

type Error = implCluster.Error
type Event = implCluster.Event
type EventFilter = implCluster.EventFilter
type EventKind = implCluster.EventKind

type ExecutionError = implExecution.ExecutionError

func OpenSingleHost(a0 context.Context, a1 Config, a2 cql.UUID) (*Session, error) {
	return implExecution.OpenSingleHost(a0, a1, a2)
}

// Options lists the settings of an execution. The zero Options uses the
// session's default profile. Query constructors take Options and validate and
// resolve them for their session once, so query executions do no per-call
// work; one-shot executions take SessionOptions from BindOptions instead.
type Options = implExecution.OptionValues

// SessionOptions are Options validated, resolved against one session's
// default profile and keyspace, and encoded once by BindOptions, for one-shot
// executions such as Prepared.Exec, Query.ExecBatch and Query.RunBatch. They are
// immutable and safe to share between goroutines. Executions use them as they
// are, without checking which session runs them: using them on another
// session applies the binding session's default profile and keyspace. The
// zero SessionOptions uses the executing session's default profile.
type SessionOptions = implExecution.Options

// BindOptions validates options and resolves them for session into
// SessionOptions; the zero Options gives the zero SessionOptions. The custom
// payload and the profile are copied, so later changes to options do not
// reach the result.
func BindOptions(session *Session, options Options) (SessionOptions, error) {
	if session == nil {
		return SessionOptions{}, errors.New("gocql: BindOptions requires a session")
	}
	return implExecution.InternalBindOptions(session, options)
}

const OverflowEvent = implCluster.OverflowEvent

type Page[R any] = implExecution.Page[R]
type PageLease = implExecution.PageLease

// Prepare prepares statement on session. At most one PrepareOptions may be
// given; none prepares with the session's keyspace on any host.
func Prepare[P any, R any](ctx context.Context, session *Session, statement cql.Statement[P, R], options ...PrepareOptions) (*Prepared[P, R], error) {
	o, err := onePrepareOptions(options)
	if err != nil {
		return nil, err
	}
	return implExecution.Prepare[P, R](ctx, session, statement, o)
}

// PrepareQuery prepares statement and returns a query that executes it with
// options, for the common case of one query per prepared statement. Use
// Prepare and NewQuery to run several queries from one preparation.
func PrepareQuery[P any, R any](ctx context.Context, session *Session, statement cql.Statement[P, R], options Options, prepareOptions ...PrepareOptions) (*Query[P, R], error) {
	prepared, err := Prepare(ctx, session, statement, prepareOptions...)
	if err != nil {
		return nil, err
	}
	return implExecution.NewQuery(prepared, options)
}

func onePrepareOptions(options []PrepareOptions) (PrepareOptions, error) {
	switch len(options) {
	case 0:
		return PrepareOptions{}, nil
	case 1:
		return options[0], nil
	default:
		return PrepareOptions{}, errors.New("gocql: at most one PrepareOptions")
	}
}

type PrepareOptions = implExecution.PrepareOptions
type Prepared[P any, R any] = implExecution.Prepared[P, R]
type PreparedInfo = implExecution.PreparedInfo

type RowStream[R any] = implExecution.RowStream[R]
type Rows[R any] = implExecution.Rows[R]

const SchemaEvent = implCluster.SchemaEvent

type Session = implCluster.Session

const StatusEvent = implCluster.StatusEvent

type StreamInfo = implExecution.StreamInfo
type StreamOptions = implExecution.StreamOptions
type Subscription = implCluster.Subscription

const TopologyEvent = implCluster.TopologyEvent

type TraceEntry = implCluster.TraceEntry
type TraceEvent = implCluster.TraceEvent
type TraceInfo = implCluster.TraceInfo
