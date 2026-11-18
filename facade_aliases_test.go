// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// Facade aliases referenced only by tests.

import (
	"context"

	"github.com/scylladb/gocql/v2/config"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	model "github.com/scylladb/gocql/v2/internal/model"
	paging "github.com/scylladb/gocql/v2/internal/paging"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/tablets"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

type hostPrepared = implSession.InternalHostPrepared

func newConnectionHooks(a0 *testSession) implTransport.InternalConnectionHooks {
	return (*implSession.Session)(a0).Core().NewConnectionHooks()
}

type preparedState = implSession.InternalPreparedState

type resultPlanCache[R any] = implSession.InternalResultPlanCache[R]

type ringDescriber = implCluster.InternalRingDescriber

func runtimeFor(a0 *testSession) *sessionRuntime {
	return implSession.InternalRuntimeFor((*implSession.Session)(a0).Core())
}

type sessionRuntime = implSession.InternalSessionRuntime

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
	return implTransport.ScyllaGetSourcePort(a0)
}

type retiredScyllaShardAwareDialer = implTransport.ScyllaShardAwareDialer

type cassVersion = implEndpoint.InternalCassVersion

func defaultProfile() config.Profile {
	return implSettings.InternalProfileToPublic[config.Profile](implSettings.InternalDefaultProfile())
}

type scyllaSourcePortCtx = implTransport.InternalScyllaSourcePortCtx

type retiredSnappyCompressor = wirecompress.SnappyCompressor

type responseData = model.ResponseData

func buildRoutingHostIndex(a0 []metadata.HostSnapshot) *routingHostIndex {
	return implRouting.InternalBuildRoutingHostIndex(a0)
}

type metadataTable = implRouting.InternalMetadataTable

type routingDecisions = implRouting.InternalRoutingDecisions

type routingPolicy = implRouting.InternalRoutingPolicy

type tabletPlacement = implRouting.InternalTabletPlacement

type tabletRangeTree = implRouting.InternalTabletRangeTree

func updateTabletRange(a0 *tabletRangeTree, a1 tablets.TabletInfo) (*tabletRangeTree, bool) {
	return implRouting.InternalUpdateTabletRange(a0, a1)
}

type routingHostIndex = implRouting.InternalRoutingHostIndex

// builtinPolicy returns the session's default policy behind p.
func builtinPolicy(p routing.Policy) *routingPolicy {
	b, ok := implRouting.InternalBuiltin(p)
	if !ok {
		panic("not a built-in routing policy")
	}
	return b
}
