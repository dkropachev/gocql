package gocql

import (
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Deprecated: use cqlerr.ErrClosed.
var ErrClosed = cqlerr.ErrClosed

// Deprecated: use cqlerr.ErrConnectionClosed.
var ErrConnectionClosed = cqlerr.ErrConnectionClosed

// Deprecated: use cqlerr.ErrHostDown.
var ErrHostDown = cqlerr.ErrHostDown

// Deprecated: use cqlerr.ErrKeyspaceDoesNotExist.
var ErrKeyspaceDoesNotExist = cqlerr.ErrKeyspaceDoesNotExist

// Deprecated: use cqlerr.ErrMultipleRows.
var ErrMultipleRows = cqlerr.ErrMultipleRows

// Deprecated: use cqlerr.ErrNoConnections.
var ErrNoConnections = cqlerr.ErrNoConnections

// Deprecated: use cqlerr.ErrNoConnectionsInPool.
var ErrNoConnectionsInPool = cqlerr.ErrNoConnectionsInPool

// Deprecated: use cqlerr.ErrNoKeyspace.
var ErrNoKeyspace = cqlerr.ErrNoKeyspace

// Deprecated: use cqlerr.ErrNoMetadata.
var ErrNoMetadata = cqlerr.ErrNoMetadata

// Deprecated: use cqlerr.ErrNoPool.
var ErrNoPool = cqlerr.ErrNoPool

// Deprecated: use cqlerr.ErrNoRows.
var ErrNoRows = cqlerr.ErrNoRows

// Deprecated: use cqlerr.ErrNoStreams.
var ErrNoStreams = cqlerr.ErrNoStreams

// Deprecated: use cqlerr.ErrNoTable.
var ErrNoTable = cqlerr.ErrNoTable

// Deprecated: use cqlerr.ErrNotFound.
var ErrNotFound = cqlerr.ErrNotFound

// Deprecated: use cqlerr.ErrPagingComplete.
var ErrPagingComplete = cqlerr.ErrPagingComplete

// Deprecated: use cqlerr.ErrPagingNoProgress.
var ErrPagingNoProgress = cqlerr.ErrPagingNoProgress

// Deprecated: use cqlerr.ErrPagingStateLimit.
var ErrPagingStateLimit = cqlerr.ErrPagingStateLimit

// Deprecated: use cqlerr.ErrQueryArgLength.
var ErrQueryArgLength = cqlerr.ErrQueryArgLength

// Deprecated: use cqlerr.ErrSessionClosed.
var ErrSessionClosed = cqlerr.ErrSessionClosed

// Deprecated: use cqlerr.ErrSessionNotReady.
var ErrSessionNotReady = cqlerr.ErrSessionNotReady

// Deprecated: use cqlerr.ErrTabletsNotUsed.
var ErrTabletsNotUsed = cqlerr.ErrTabletsNotUsed

// Deprecated: use cqlerr.ErrTimeoutNoResponse.
var ErrTimeoutNoResponse = cqlerr.ErrTimeoutNoResponse

// Deprecated: use cqlerr.ErrTooManyStmts.
var ErrTooManyStmts = cqlerr.ErrTooManyStmts

// Deprecated: use cqlerr.ErrTooManyTimeouts.
var ErrTooManyTimeouts = cqlerr.ErrTooManyTimeouts

// Deprecated: use cqlerr.ErrUnavailable.
var ErrUnavailable = cqlerr.ErrUnavailable

// Deprecated: use cqlerr.ErrUnsupported.
var ErrUnsupported = cqlerr.ErrUnsupported

// Deprecated: use cqlerr.ErrUseStmt.
var ErrUseStmt = cqlerr.ErrUseStmt

// Deprecated: use cqlerr.QueryError.
type QueryError = cqlerr.QueryError

// Deprecated: use cqlerr.SchemaChangedError.
type SchemaChangedError = cqlerr.SchemaChangedError
