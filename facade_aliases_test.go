// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// Facade aliases referenced only by tests.

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"

	logging "github.com/scylladb/gocql/v2/internal/logging"

	model "github.com/scylladb/gocql/v2/internal/model"
	paging "github.com/scylladb/gocql/v2/internal/paging"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	routing "github.com/scylladb/gocql/v2/internal/routing"

	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"

	"github.com/scylladb/gocql/v2/internal/wirecompress"

	"github.com/scylladb/gocql/v2/tablets"
)

type hostPrepared = implExecution.InternalHostPrepared

func newConnectionHooks(a0 *Session) implTransport.InternalConnectionHooks {
	return implCluster.InternalNewConnectionHooks(a0)
}

type preparedState = implExecution.InternalPreparedState

type resultPlanCache[R any] = implExecution.InternalResultPlanCache[R]

type ringDescriber = implCluster.InternalRingDescriber

func runtimeFor(a0 *Session) *sessionRuntime { return implExecution.InternalRuntimeFor(a0) }

type sessionRuntime = implExecution.InternalSessionRuntime

type driverConn = implTransport.Conn

const qrySystemLocal = implTransport.InternalQrySystemLocal

type responsePage = implTransport.InternalResponsePage

type typedResultMetadata = model.ResultSchemaIdentity

type attemptRequest = model.AttemptRequest

type pagingHistory = paging.InternalPagingHistory

type pagingHistoryEntry = paging.InternalPagingHistoryEntry

type defaultConnPicker = pool.InternalDefaultConnPicker

type hostConnPool = pool.InternalHostConnPool

func newDefaultConnPicker(a0 int) *defaultConnPicker { return pool.InternalNewDefaultConnPicker(a0) }

type policyConnPool = pool.InternalPolicyConnPool

type retiredCompressor = protocol.Compressor

type retiredSegmentCompressor = protocol.SegmentCompressor

type framer = protocol.Framer

const protoDirectionMask = protocol.ProtoDirectionMask

const protoVersion4 = protocol.ProtoVersion4

func validateShortBytes(a0 string, a1 []byte) error { return protocol.ValidateShortBytes(a0, a1) }

type retiredHostInfo = implEndpoint.HostInfo

const retiredNodeUp = implEndpoint.NodeUp

func retiredScyllaGetSourcePort(a0 context.Context) uint16 {
	return implEndpoint.ScyllaGetSourcePort(a0)
}

type retiredScyllaShardAwareDialer = implEndpoint.ScyllaShardAwareDialer

type cassVersion = implEndpoint.InternalCassVersion

func defaultProfile() Profile { return implSettings.InternalDefaultProfile() }

type nopLogger = logging.InternalNopLogger

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

type retiredSnappyCompressor = wirecompress.SnappyCompressor

type responseData = model.ResponseData

func buildRoutingHostIndex(a0 []HostSnapshot) *routingHostIndex {
	return routing.InternalBuildRoutingHostIndex(a0)
}

func comparePlacementTable(a0 metadataTable, a1 metadataTable) int {
	return routing.InternalComparePlacementTable(a0, a1)
}

type metadataTable = routing.InternalMetadataTable

func placementPut[K any, V any](a0 *placementTree[K, V], a1 K, a2 V, a3 func(K, K) int) *placementTree[K, V] {
	return routing.InternalPlacementPut[K, V](a0, a1, a2, a3)
}

type routingDecisions = routing.InternalRoutingDecisions

type routingPolicy = routing.InternalRoutingPolicy

type tabletPlacement = routing.InternalTabletPlacement

type tabletRangeTree = routing.InternalTabletRangeTree

func updateTabletRange(a0 *tabletRangeTree, a1 tablets.TabletInfo) (*tabletRangeTree, bool) {
	return routing.InternalUpdateTabletRange(a0, a1)
}

type routingHostIndex = routing.InternalRoutingHostIndex

type placementTree[K, V any] = routing.InternalPlacementTree[K, V]
