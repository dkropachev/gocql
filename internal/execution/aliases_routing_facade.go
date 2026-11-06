// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"

	model "github.com/scylladb/gocql/v2/internal/model"

	routing "github.com/scylladb/gocql/v2/internal/routing"
)

var ErrClosed = runtimeerrors.ErrClosed

type AttemptEvent = routing.AttemptEvent

var ErrKeyspaceDoesNotExist = runtimeerrors.ErrKeyspaceDoesNotExist

var ErrNoConnections = runtimeerrors.ErrNoConnections

var ErrNoRows = runtimeerrors.ErrNoRows

var ErrPagingComplete = runtimeerrors.ErrPagingComplete

type HostSnapshot = model.HostSnapshot

type MetricsSnapshot = routing.MetricsSnapshot

type QueryError = runtimeerrors.QueryError
type QueryPlan = routing.QueryPlan
type Route = routing.Route

type RoutingInfo = model.RoutingInfo

type SchemaChangedError = runtimeerrors.SchemaChangedError

type Target = model.Target
type TokenPartitioner = routing.TokenPartitioner

func computeRoutingToken(a0 []encodedCell, a1 []int, a2 TokenPartitioner) (Option[int64], error) {
	return routing.InternalComputeRoutingToken(a0, a1, a2)
}

type tabletFeedbackStamp = routing.InternalTabletFeedbackStamp
