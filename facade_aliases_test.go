// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// Facade aliases referenced only by tests.

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	routing "github.com/scylladb/gocql/v2/routing"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"

	logging "github.com/scylladb/gocql/v2/internal/logging"

	model "github.com/scylladb/gocql/v2/internal/model"
	paging "github.com/scylladb/gocql/v2/internal/paging"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"

	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"

	"github.com/scylladb/gocql/v2/internal/wirecompress"

	"github.com/scylladb/gocql/v2/internal/tablets"
)

type hostPrepared = implExecution.InternalHostPrepared

func newConnectionHooks(a0 *Session) implTransport.InternalConnectionHooks {
	return implExecution.InternalCore((*implExecution.Session)(a0)).NewConnectionHooks()
}

type preparedState = implExecution.InternalPreparedState

type resultPlanCache[R any] = implExecution.InternalResultPlanCache[R]

type ringDescriber = implCluster.InternalRingDescriber

func runtimeFor(a0 *Session) *sessionRuntime {
	return implExecution.InternalRuntimeFor(implExecution.InternalCore((*implExecution.Session)(a0)))
}

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

func defaultProfile() Profile { return publicProfile(implSettings.InternalDefaultProfile()) }

type nopLogger = logging.InternalNopLogger

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

type retiredSnappyCompressor = wirecompress.SnappyCompressor

type responseData = model.ResponseData

func buildRoutingHostIndex(a0 []routing.HostSnapshot) *routingHostIndex {
	return implRouting.InternalBuildRoutingHostIndex(a0)
}

func comparePlacementTable(a0 metadataTable, a1 metadataTable) int {
	return implRouting.InternalComparePlacementTable(a0, a1)
}

type metadataTable = implRouting.InternalMetadataTable

func placementPut[K any, V any](a0 *placementTree[K, V], a1 K, a2 V, a3 func(K, K) int) *placementTree[K, V] {
	return implRouting.InternalPlacementPut[K, V](a0, a1, a2, a3)
}

type routingDecisions = implRouting.InternalRoutingDecisions

type routingPolicy = implRouting.InternalRoutingPolicy

type tabletPlacement = implRouting.InternalTabletPlacement

type tabletRangeTree = implRouting.InternalTabletRangeTree

func updateTabletRange(a0 *tabletRangeTree, a1 tablets.TabletInfo) (*tabletRangeTree, bool) {
	return implRouting.InternalUpdateTabletRange(a0, a1)
}

type routingHostIndex = implRouting.InternalRoutingHostIndex

type placementTree[K, V any] = implRouting.InternalPlacementTree[K, V]
