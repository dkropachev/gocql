// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"

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

// NewOptions validates values and freezes them into Options resolved for
// session, which then executes them with no per-call work. Options built
// with a nil session, or used on another session, are resolved on each
// execution unless they name their own Profile. The zero Options uses the
// session's default profile.
func NewOptions(session *Session, values OptionValues) (Options, error) {
	return implExecution.NewOptions(session, values)
}

const OverflowEvent = implCluster.OverflowEvent

type Page[R any] = implExecution.Page[R]
type PageLease = implExecution.PageLease

func Prepare[P any, R any](a0 context.Context, a1 *Session, a2 cql.Statement[P, R], a3 PrepareOptions) (*Prepared[P, R], error) {
	return implExecution.Prepare[P, R](a0, a1, a2, a3)
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
