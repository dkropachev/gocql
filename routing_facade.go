package gocql

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

const ASC = implMetadata.ASC

type AggregateMetadata = implMetadata.AggregateMetadata
type AttemptEvent = routing.AttemptEvent

var CDCTokenPartitioner = routing.CDCTokenPartitioner

const ColumnClusteringKey = implMetadata.ColumnClusteringKey
const ColumnCompact = implMetadata.ColumnCompact

type ColumnIndexMetadata = implMetadata.ColumnIndexMetadata
type ColumnKind = implMetadata.ColumnKind
type ColumnMetadata = implMetadata.ColumnMetadata
type ColumnOrder = implMetadata.ColumnOrder

const ColumnPartitionKey = implMetadata.ColumnPartitionKey
const ColumnRegular = implMetadata.ColumnRegular
const ColumnStatic = implMetadata.ColumnStatic
const ColumnUnkownKind = implMetadata.ColumnUnkownKind
const DESC = implMetadata.DESC

var ErrClosed = runtimeerrors.ErrClosed
var ErrConnectionClosed = runtimeerrors.ErrConnectionClosed
var ErrHostDown = runtimeerrors.ErrHostDown
var ErrKeyspaceDoesNotExist = runtimeerrors.ErrKeyspaceDoesNotExist
var ErrMultipleRows = runtimeerrors.ErrMultipleRows
var ErrNoConnections = runtimeerrors.ErrNoConnections
var ErrNoConnectionsInPool = runtimeerrors.ErrNoConnectionsInPool
var ErrNoKeyspace = runtimeerrors.ErrNoKeyspace
var ErrNoMetadata = runtimeerrors.ErrNoMetadata
var ErrNoPool = runtimeerrors.ErrNoPool
var ErrNoRows = runtimeerrors.ErrNoRows
var ErrNoStreams = runtimeerrors.ErrNoStreams
var ErrNoTable = runtimeerrors.ErrNoTable
var ErrNotFound = runtimeerrors.ErrNotFound
var ErrPagingComplete = runtimeerrors.ErrPagingComplete
var ErrPagingNoProgress = runtimeerrors.ErrPagingNoProgress
var ErrPagingStateLimit = runtimeerrors.ErrPagingStateLimit
var ErrQueryArgLength = runtimeerrors.ErrQueryArgLength
var ErrSessionClosed = runtimeerrors.ErrSessionClosed
var ErrSessionNotReady = runtimeerrors.ErrSessionNotReady
var ErrTabletsNotUsed = runtimeerrors.ErrTabletsNotUsed
var ErrTimeoutNoResponse = runtimeerrors.ErrTimeoutNoResponse
var ErrTooManyStmts = runtimeerrors.ErrTooManyStmts
var ErrTooManyTimeouts = runtimeerrors.ErrTooManyTimeouts
var ErrUnavailable = runtimeerrors.ErrUnavailable
var ErrUnsupported = runtimeerrors.ErrUnsupported
var ErrUseStmt = runtimeerrors.ErrUseStmt

type FunctionMetadata = implMetadata.FunctionMetadata
type HostSnapshot = model.HostSnapshot

const IndexKindCustom = implMetadata.IndexKindCustom

type IndexMetadata = implMetadata.IndexMetadata
type KeyspaceMetadata = implMetadata.KeyspaceMetadata
type LatencyOptions = routing.LatencyOptions
type MetadataSnapshot = routing.MetadataSnapshot
type MetricsSnapshot = routing.MetricsSnapshot

var Murmur3TokenPartitioner = routing.Murmur3TokenPartitioner

func NewRoutingPolicy(a0 RoutingOptions) (RoutingPolicy, error) { return routing.NewRoutingPolicy(a0) }

type PartitionKey = routing.PartitionKey
type QueryError = runtimeerrors.QueryError
type QueryPlan = routing.QueryPlan

// NewQueryPlan returns the plan a request follows under policy: the route
// policy.Pick gives, then the routes of policy.Fallback except that one.
func NewQueryPlan(policy RoutingPolicy, info RoutingInfo, m *MetadataSnapshot) QueryPlan {
	return routing.NewPlan(policy, info, m)
}

type Route = routing.Route

const AnyShard = routing.AnyShard

type RoutingInfo = model.RoutingInfo
type RoutingOptions = routing.RoutingOptions
type RoutingPolicy = routing.RoutingPolicy
type SchemaChangedError = runtimeerrors.SchemaChangedError

func SingleTargetRouting(a0 Target, a1 bool) RoutingPolicy {
	return routing.SingleTargetRouting(a0, a1)
}

type TableMetadata = implMetadata.TableMetadata
type TableMetadataOptions = implMetadata.TableMetadataOptions
type Target = model.Target
type TokenPartitioner = routing.TokenPartitioner
type TokenRange = routing.TokenRange
type TypeMetadata = implMetadata.TypeMetadata
type ViewMetadata = implMetadata.ViewMetadata
