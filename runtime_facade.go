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

type BatchItem = implExecution.BatchItem

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

func ExecBatch(a0 context.Context, a1 *Session, a2 cql.BatchKind, a3 []BatchItem, a4 Options) (Result, error) {
	return implExecution.ExecOwnedBatch(a0, a1, a2, a3, a4)
}

type ExecutionError = implExecution.ExecutionError

func OpenSingleHost(a0 context.Context, a1 Config, a2 cql.UUID) (*Session, error) {
	return implExecution.OpenSingleHost(a0, a1, a2)
}

type Options = implExecution.Options

// OptionValues lists the settings of an execution; NewOptions freezes them.
type OptionValues = implExecution.OptionValues

// NewOptions validates values and freezes them into Options. A Query
// resolves its Options against its session once, when it is created, so
// executions pay nothing for them. The zero Options uses the session's
// default profile.
func NewOptions(values OptionValues) (Options, error) {
	return implExecution.NewOptions(nil, values)
}

// NewOptionsFor is NewOptions resolved for session up front, for one-shot
// executions such as ExecBatch and RunBatch that have no Query to resolve
// them once. Used on another session, they are resolved on each execution
// unless they name their own Profile.
func NewOptionsFor(session *Session, values OptionValues) (Options, error) {
	return implExecution.NewOptions(session, values)
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

func RunBatch[R any](a0 context.Context, a1 *Session, a2 cql.BatchKind, a3 []BatchItem, a4 cql.RowDecoder[R], a5 Options) (*Page[R], error) {
	return implExecution.RunOwnedBatch[R](a0, a1, a2, a3, a4, a5)
}

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
