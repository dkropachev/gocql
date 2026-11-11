// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//go:build integration

package integration

// Facade aliases referenced only by tests.

import (
	"context"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	binding "github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	routing "github.com/scylladb/gocql/v2/routing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implSession "github.com/scylladb/gocql/v2/internal/session"

	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"

	pool "github.com/scylladb/gocql/v2/internal/pool"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"

	implTransport "github.com/scylladb/gocql/v2/internal/transport"

	"github.com/scylladb/gocql/v2/internal/tablets"
)

func admitExecution[R any](a0 context.Context, a1 *gocql.Session, a2 *implSession.Bound[R], a3 gocql.CompiledOptions) (context.Context, context.CancelFunc, *executionRequest, error) {
	return implSession.InternalAdmitExecution[R](a0, (*implSession.Session)(a1).Core(), a2, implSession.CompiledOptions(a3))
}

type clientRoute = implCluster.InternalClientRoute

type controlConnection = implCluster.InternalControlConnection

type driverConfigReport = implCluster.InternalDriverConfigReport

const driverConfigStartupKey = implCluster.InternalDriverConfigStartupKey

const driverConfigVersion = implCluster.InternalDriverConfigVersion

type executionRequest = implSession.InternalExecutionRequest

func getHostPortMappingForConnectionIDs(a0 controlConnection, a1 string, a2 []string, a3 bool) ([]clientRoute, error) {
	return implCluster.InternalGetHostPortMappingForConnectionIDs(a0, a1, a2, a3)
}

func getHostPortMappingForPairs(a0 controlConnection, a1 string, a2 []pair, a3 bool) ([]clientRoute, error) {
	return implCluster.InternalGetHostPortMappingForPairs(a0, a1, a2, a3)
}

type hostPrepared = implSession.InternalHostPrepared

func newPage[R any](a0 *responsePage, a1 cql.RowDecoder[R], a2 func()) (*gocql.Page[R], error) {
	result, err := implSession.InternalNewPage[R](a0, a1, a2)
	return (*gocql.Page[R])(result), err
}

func newSchemaSource(a0 *gocql.Session) implMetadata.Source {
	return (*implSession.Session)(a0).Core().NewSchemaSource()
}

type pair = implCluster.InternalPair

func readV2SystemRows[P any, R any](a0 context.Context, a1 *driverConn, a2 cql.Statement[P, R], a3 cql.Columns, a4 P) ([]R, error) {
	return implCluster.InternalReadV2SystemRows[P, R](a0, a1, fixtureStatement(a2), a3, a4)
}

func resolveOptions(a0 *gocql.Session, a1 gocql.Options) (resolvedOptions, error) {
	o, err := a0.CompileOptions(a1)
	if err != nil {
		return resolvedOptions{}, err
	}
	return implSession.InternalResolveFor((*implSession.Session)(a0).Core(), implSession.CompiledOptions(o)), nil
}

type resolvedOptions = implSession.InternalResolvedOptions

func runtimeFor(a0 *gocql.Session) *sessionRuntime {
	return implSession.InternalRuntimeFor((*implSession.Session)(a0).Core())
}

type sessionRuntime = implSession.InternalSessionRuntime

var errNoControl = implMetadata.InternalErrNoControl

func getV2CreateStatements(a0 context.Context, a1 implMetadata.Source, a2 string) ([]byte, error) {
	return implMetadata.InternalGetV2CreateStatements(a0, a1, a2)
}

type driverConn = implTransport.Conn

func decodeStreamRow[R any](a0 *streamResponse, a1 cql.StreamRowPlan[R], a2 *R) (bool, error) {
	return implTransport.InternalDecodeStreamRow[R](a0, a1, a2)
}

func parseTypedTabletPayload(a0 cql.Bytes, a1 string, a2 string, a3 bool) (tablets.TabletInfo, error) {
	return implTransport.InternalParseTypedTabletPayload(a0, a1, a2, a3)
}

type responsePage = implTransport.InternalResponsePage

const sessionIDStartupKey = implTransport.InternalSessionIDStartupKey

type streamResponse = implTransport.InternalStreamResponse

type attemptRequest = model.AttemptRequest

func dataTypeFromInfo(a0 cqltype.TypeInfo) (cql.DataType, error) { return cqltype.DataTypeFromInfo(a0) }

type encodedCell = model.EncodedCell

type defaultConnPicker = pool.InternalDefaultConnPicker

type hostConnPool = pool.InternalHostConnPool

const poolReady = pool.InternalPoolReady

func scyllaShardForSourcePort(a0 uint16, a1 int) int {
	return implTransport.InternalScyllaShardForSourcePort(a0, a1)
}

type typedConnPicker = pool.InternalTypedConnPicker

type connReader = implTransport.InternalConnReader

func firstCQLWord(a0 string) string { return binding.InternalFirstCQLWord(a0) }

type retiredCompressor = protocol.Compressor

const apacheCassandraTypePrefix = cqltype.ApacheCassandraTypePrefix

type frameWriterFunc = protocol.FrameWriterFunc

type framer = protocol.Framer

const headSize = protocol.HeadSize

func newFramer(a0 retiredCompressor, a1 byte) *framer { return protocol.NewFramer(a0, a1) }

const protoVersion3 = protocol.ProtoVersion3

const protoVersion4 = protocol.ProtoVersion4

const protoVersion5 = protocol.ProtoVersion5

const protoVersionMask = protocol.ProtoVersionMask

func validateShortBytes(a0 string, a1 []byte) error { return protocol.ValidateShortBytes(a0, a1) }

const retiredNodeDown = implEndpoint.NodeDown

const retiredNodeUp = implEndpoint.NodeUp

func retiredScyllaGetSourcePort(a0 context.Context) uint16 {
	return implEndpoint.ScyllaGetSourcePort(a0)
}

type retiredScyllaShardAwareDialer = implEndpoint.ScyllaShardAwareDialer

type cassVersion = implEndpoint.InternalCassVersion

type legacyDialer = implEndpoint.InternalLegacyDialer

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

func newPartitionKey(a0 []encodedCell, a1 []int) (routing.PartitionKey, bool, error) {
	return implRouting.InternalNewPartitionKey(a0, a1)
}
