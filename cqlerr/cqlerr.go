// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cqlerr holds the errors the driver returns.
//
// A failed request returns an *ExecutionError. Its ExecutionStatus reports
// whether the request may have changed data, which decides whether replaying
// it is safe:
//
//	switch cqlerr.ExecutionStatusOf(err) {
//	case cqlerr.NotExecuted:       // nothing was applied; safe to retry
//	case cqlerr.MaybeExecuted:     // unknown; retry only idempotent requests
//	case cqlerr.PartiallyExecuted: // applied in part; reconcile before retrying
//	}
//
// KindOf groups any error by what a caller can do about it (timeout,
// overloaded, invalid, connection, ...), and Kind.Transient reports whether
// trying again can help. ExecutionError.Attempts lists every failed attempt
// with the host it went to.
//
// Server errors are typed by their CQL error code (RequestErrWriteTimeout,
// RequestErrUnavailable, ...) and all implement RequestError; find them with
// errors.As, or match a code with errors.Is(err, ErrServerOverloaded).
// Driver conditions are sentinels (ErrNoRows, ErrNoConnections, ...); test
// them with errors.Is.
package cqlerr

import (
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// Execution status.

// ExecutionStatus reports what a failed request may have done on the
// cluster. States are ordered by severity.
type ExecutionStatus = cqltype.ExecutionStatus

const (
	// NotExecuted guarantees the request had no effect: it never reached a
	// server, or the server rejected it before applying anything.
	NotExecuted = cqltype.NotExecuted
	// MaybeExecuted means the request reached a server and may or may not
	// have been applied.
	MaybeExecuted = cqltype.MaybeExecuted
	// PartiallyExecuted means the request was applied at least in part.
	PartiallyExecuted = cqltype.PartiallyExecuted
)

// ExecutionStatusOf reports what the request that failed with err may have
// done on the cluster, across all of its attempts. It returns NotExecuted for
// a nil error and MaybeExecuted for an error the driver cannot attribute.
func ExecutionStatusOf(err error) ExecutionStatus { return runtimeerrors.ExecutionStatusOf(err) }

// ServerErrorExecutionStatus classifies a server error by what the failed
// request may have done.
func ServerErrorExecutionStatus(err RequestError) ExecutionStatus {
	return protocol.ServerErrorExecutionStatus(err)
}

// Classification.

// Kind groups errors by what a caller can do about them.
type Kind = runtimeerrors.Kind

const (
	KindUnknown      = runtimeerrors.KindUnknown
	KindTimeout      = runtimeerrors.KindTimeout
	KindUnavailable  = runtimeerrors.KindUnavailable
	KindOverloaded   = runtimeerrors.KindOverloaded
	KindRateLimited  = runtimeerrors.KindRateLimited
	KindInvalid      = runtimeerrors.KindInvalid
	KindUnauthorized = runtimeerrors.KindUnauthorized
	KindConnection   = runtimeerrors.KindConnection
	KindCanceled     = runtimeerrors.KindCanceled
	KindClosed       = runtimeerrors.KindClosed
	KindProtocol     = runtimeerrors.KindProtocol
	KindServer       = runtimeerrors.KindServer
)

// KindOf classifies err. For a request that failed after several attempts it
// classifies the reason the request stopped. Kind.Transient reports whether
// trying again can help; ExecutionStatusOf reports whether it is safe.
func KindOf(err error) Kind { return runtimeerrors.KindOf(err) }

// Request errors.

// ExecutionError is returned when a request fails after all its attempts.
type ExecutionError = runtimeerrors.ExecutionError

// AttemptFailure records one failed attempt of a request; see
// ExecutionError.Attempts.
type AttemptFailure = runtimeerrors.AttemptFailure

// QueryError is the failure of a single attempt on one connection.
type QueryError = runtimeerrors.QueryError

// SchemaChangedError reports that a prepared statement's bound parameters
// changed between executions.
type SchemaChangedError = runtimeerrors.SchemaChangedError

// Server errors.

// RequestError is implemented by every error returned by a server.
type RequestError = protocol.RequestError

// ErrorCode is a CQL server error code. Each ErrServer sentinel is one; as an
// error it matches, through errors.Is, every server error with that code.
type ErrorCode = protocol.ErrorCode

// Server error code sentinels, matched with errors.Is.
var (
	ErrServerError           error = ErrorCode(ErrCodeServer)
	ErrServerProtocol        error = ErrorCode(ErrCodeProtocol)
	ErrServerCredentials     error = ErrorCode(ErrCodeCredentials)
	ErrServerUnavailable     error = ErrorCode(ErrCodeUnavailable)
	ErrServerOverloaded      error = ErrorCode(ErrCodeOverloaded)
	ErrServerBootstrapping   error = ErrorCode(ErrCodeBootstrapping)
	ErrServerTruncate        error = ErrorCode(ErrCodeTruncate)
	ErrServerWriteTimeout    error = ErrorCode(ErrCodeWriteTimeout)
	ErrServerReadTimeout     error = ErrorCode(ErrCodeReadTimeout)
	ErrServerReadFailure     error = ErrorCode(ErrCodeReadFailure)
	ErrServerFunctionFailure error = ErrorCode(ErrCodeFunctionFailure)
	ErrServerWriteFailure    error = ErrorCode(ErrCodeWriteFailure)
	ErrServerCDCWriteFailure error = ErrorCode(ErrCodeCDCWriteFailure)
	ErrServerCASWriteUnknown error = ErrorCode(ErrCodeCASWriteUnknown)
	ErrServerSyntax          error = ErrorCode(ErrCodeSyntax)
	ErrServerUnauthorized    error = ErrorCode(ErrCodeUnauthorized)
	ErrServerInvalid         error = ErrorCode(ErrCodeInvalid)
	ErrServerConfig          error = ErrorCode(ErrCodeConfig)
	ErrServerAlreadyExists   error = ErrorCode(ErrCodeAlreadyExists)
	ErrServerUnprepared      error = ErrorCode(ErrCodeUnprepared)
)

type (
	RequestErrAlreadyExists    = protocol.RequestErrAlreadyExists
	RequestErrCASWriteUnknown  = protocol.RequestErrCASWriteUnknown
	RequestErrCDCWriteFailure  = protocol.RequestErrCDCWriteFailure
	RequestErrFunctionFailure  = protocol.RequestErrFunctionFailure
	RequestErrRateLimitReached = protocol.RequestErrRateLimitReached
	RequestErrServer           = protocol.RequestErrServer
	RequestErrReadFailure      = protocol.RequestErrReadFailure
	RequestErrReadTimeout      = protocol.RequestErrReadTimeout
	RequestErrUnavailable      = protocol.RequestErrUnavailable
	RequestErrUnprepared       = protocol.RequestErrUnprepared
	RequestErrWriteFailure     = protocol.RequestErrWriteFailure
	RequestErrWriteTimeout     = protocol.RequestErrWriteTimeout
	UnknownServerError         = protocol.UnknownServerError
	ErrorMap                   = protocol.ErrorMap
	OpType                     = protocol.OpType
)

const (
	OpTypeRead  = protocol.OpTypeRead
	OpTypeWrite = protocol.OpTypeWrite
)

// CQL error codes, as reported by RequestError.GetCode.
const (
	ErrCodeServer          = protocol.ErrCodeServer
	ErrCodeProtocol        = protocol.ErrCodeProtocol
	ErrCodeCredentials     = protocol.ErrCodeCredentials
	ErrCodeUnavailable     = protocol.ErrCodeUnavailable
	ErrCodeOverloaded      = protocol.ErrCodeOverloaded
	ErrCodeBootstrapping   = protocol.ErrCodeBootstrapping
	ErrCodeTruncate        = protocol.ErrCodeTruncate
	ErrCodeWriteTimeout    = protocol.ErrCodeWriteTimeout
	ErrCodeReadTimeout     = protocol.ErrCodeReadTimeout
	ErrCodeReadFailure     = protocol.ErrCodeReadFailure
	ErrCodeFunctionFailure = protocol.ErrCodeFunctionFailure
	ErrCodeWriteFailure    = protocol.ErrCodeWriteFailure
	ErrCodeCDCWriteFailure = protocol.ErrCodeCDCWriteFailure
	ErrCodeCASWriteUnknown = protocol.ErrCodeCASWriteUnknown
	ErrCodeSyntax          = protocol.ErrCodeSyntax
	ErrCodeUnauthorized    = protocol.ErrCodeUnauthorized
	ErrCodeInvalid         = protocol.ErrCodeInvalid
	ErrCodeConfig          = protocol.ErrCodeConfig
	ErrCodeAlreadyExists   = protocol.ErrCodeAlreadyExists
	ErrCodeUnprepared      = protocol.ErrCodeUnprepared
)

// Driver errors.

type (
	CodecError              = cqltype.CodecError
	ProtocolError           = cqltype.ProtocolError
	ValidationError         = cqltype.ValidationError
	StreamCapabilityError   = codec.StreamCapabilityError
	StreamCapacityError     = codec.StreamCapacityError
	UnsupportedSettingError = runtimeerrors.UnsupportedSettingError
	NoHostsError            = metadata.NoHostsError
	PartialRefreshError     = metadata.PartialRefreshError
	ErrProtocol             = protocol.ErrProtocol
	ErrSchemaMismatch       = transport.ErrSchemaMismatch
	// ConnectError reports that no host was reachable while a session was
	// connecting; it matches ErrNoConnectionsStarted.
	ConnectError = runtimeerrors.ConnectError
	HostError    = runtimeerrors.HostError
)

// Sentinels.

var (
	ErrBusy                         = runtimeerrors.ErrBusy
	ErrClosed                       = runtimeerrors.ErrClosed
	ErrConnectionClosed             = runtimeerrors.ErrConnectionClosed
	ErrFrameTooBig                  = protocol.ErrFrameTooBig
	ErrHostDown                     = runtimeerrors.ErrHostDown
	ErrHostQueryFailed              = runtimeerrors.ErrHostQueryFailed
	ErrKeyspaceDoesNotExist         = runtimeerrors.ErrKeyspaceDoesNotExist
	ErrMultipleRows                 = runtimeerrors.ErrMultipleRows
	ErrNoConnections                = runtimeerrors.ErrNoConnections
	ErrNoConnectionsInPool          = runtimeerrors.ErrNoConnectionsInPool
	ErrNoConnectionsStarted         = runtimeerrors.ErrNoConnectionsStarted
	ErrNoHosts                      = runtimeerrors.ErrNoHosts
	ErrNoKeyspace                   = runtimeerrors.ErrNoKeyspace
	ErrNoMetadata                   = runtimeerrors.ErrNoMetadata
	ErrNoPool                       = runtimeerrors.ErrNoPool
	ErrNoRows                       = runtimeerrors.ErrNoRows
	ErrNoStreams                    = runtimeerrors.ErrNoStreams
	ErrNoTable                      = runtimeerrors.ErrNoTable
	ErrNotFound                     = runtimeerrors.ErrNotFound
	ErrPagingComplete               = runtimeerrors.ErrPagingComplete
	ErrPagingNoProgress             = runtimeerrors.ErrPagingNoProgress
	ErrPagingStateLimit             = runtimeerrors.ErrPagingStateLimit
	ErrQueryArgLength               = runtimeerrors.ErrQueryArgLength
	ErrReadHeaderTimeout            = protocol.ErrReadHeaderTimeout
	ErrReleased                     = cqltype.ErrReleased
	ErrScyllaSourcePortAlreadyInUse = transport.ErrScyllaSourcePortAlreadyInUse
	ErrSessionClosed                = runtimeerrors.ErrSessionClosed
	ErrSessionNotReady              = runtimeerrors.ErrSessionNotReady
	ErrStaleRows                    = runtimeerrors.ErrStaleRows
	ErrTabletsNotUsed               = runtimeerrors.ErrTabletsNotUsed
	ErrTimeoutNoResponse            = runtimeerrors.ErrTimeoutNoResponse
	ErrTooManyStmts                 = runtimeerrors.ErrTooManyStmts
	ErrTooManyTimeouts              = runtimeerrors.ErrTooManyTimeouts
	ErrUnavailable                  = runtimeerrors.ErrUnavailable
	ErrUnsupported                  = runtimeerrors.ErrUnsupported
	ErrUseStmt                      = runtimeerrors.ErrUseStmt
)
