package gocql

import (
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

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

type QueryError = runtimeerrors.QueryError

type SchemaChangedError = runtimeerrors.SchemaChangedError
