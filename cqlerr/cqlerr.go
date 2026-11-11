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
// Server errors are typed by their CQL error code (WriteTimeoutError,
// UnavailableError, ...) and all implement ServerError; find them with
// errors.As, or match a code with errors.Is(err, CodeOverloaded).
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

// Classification.

// Kind groups errors by what a caller can do about them. Server errors and
// the driver's error types report theirs with a Kind method; KindOf
// classifies any error.
type Kind = cqltype.Kind

const (
	KindUnknown      = cqltype.KindUnknown
	KindTimeout      = cqltype.KindTimeout
	KindUnavailable  = cqltype.KindUnavailable
	KindOverloaded   = cqltype.KindOverloaded
	KindRateLimited  = cqltype.KindRateLimited
	KindInvalid      = cqltype.KindInvalid
	KindUnauthorized = cqltype.KindUnauthorized
	KindConnection   = cqltype.KindConnection
	KindCanceled     = cqltype.KindCanceled
	KindClosed       = cqltype.KindClosed
	KindProtocol     = cqltype.KindProtocol
	KindServer       = cqltype.KindServer
)

// KindOf classifies err. For a request that failed after several attempts it
// classifies the reason the request stopped. Kind.Transient reports whether
// trying again can help; ExecutionStatusOf reports whether it is safe.
//
// When err wraps several errors, a cancellation wins over a deadline, a
// deadline over a server error, and a server error over the driver's own
// errors, so a ConnectError whose host rejected the credentials reports
// KindUnauthorized.
func KindOf(err error) Kind { return runtimeerrors.KindOf(err) }

// IsCASWriteUnknown reports whether err says that a conditional (LWT) write
// timed out without knowing whether it applied. Cassandra reports this as
// CAS_WRITE_UNKNOWN on protocol 5 and as a write timeout of write type CAS on
// protocol 4; ScyllaDB reports the write timeout. ExecutionStatusOf reports
// MaybeExecuted for all of them. Read the row back before retrying a write
// that is not idempotent.
func IsCASWriteUnknown(err error) bool { return runtimeerrors.IsCASWriteUnknown(err) }

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

// ServerError is implemented by every error a server returns. Find one with
// errors.As, or a specific kind with its concrete type:
//
//	var timeout *cqlerr.WriteTimeoutError
//	if errors.As(err, &timeout) { ... }
type ServerError = protocol.ServerError

// ErrorCode is the code of a server error. As an error it is the code's
// name, and every ServerError matches its own code through errors.Is:
//
//	if errors.Is(err, cqlerr.CodeOverloaded) { ... }
type ErrorCode = protocol.ErrorCode

// Server error codes, with their native protocol values.
const (
	CodeServer          = protocol.CodeServer
	CodeProtocol        = protocol.CodeProtocol
	CodeBadCredentials  = protocol.CodeBadCredentials
	CodeUnavailable     = protocol.CodeUnavailable
	CodeOverloaded      = protocol.CodeOverloaded
	CodeBootstrapping   = protocol.CodeBootstrapping
	CodeTruncate        = protocol.CodeTruncate
	CodeWriteTimeout    = protocol.CodeWriteTimeout
	CodeReadTimeout     = protocol.CodeReadTimeout
	CodeReadFailure     = protocol.CodeReadFailure
	CodeFunctionFailure = protocol.CodeFunctionFailure
	CodeWriteFailure    = protocol.CodeWriteFailure
	CodeCDCWriteFailure = protocol.CodeCDCWriteFailure
	CodeCASWriteUnknown = protocol.CodeCASWriteUnknown
	CodeSyntax          = protocol.CodeSyntax
	CodeUnauthorized    = protocol.CodeUnauthorized
	CodeInvalid         = protocol.CodeInvalid
	CodeConfig          = protocol.CodeConfig
	CodeAlreadyExists   = protocol.CodeAlreadyExists
	CodeUnprepared      = protocol.CodeUnprepared
	// CodeRateLimitReached is ScyllaDB's rate limit error. Its wire value is
	// negotiated per connection; the driver reports it with this code.
	CodeRateLimitReached = protocol.CodeRateLimitReached
)

// Server error types. Codes without fields of their own are reported as
// OtherServerError.
type (
	AlreadyExistsError    = protocol.AlreadyExistsError
	CASWriteUnknownError  = protocol.CASWriteUnknownError
	CDCWriteFailureError  = protocol.CDCWriteFailureError
	FunctionFailureError  = protocol.FunctionFailureError
	OtherServerError      = protocol.OtherServerError
	RateLimitReachedError = protocol.RateLimitReachedError
	ReadFailureError      = protocol.ReadFailureError
	ReadTimeoutError      = protocol.ReadTimeoutError
	UnavailableError      = protocol.UnavailableError
	UnpreparedError       = protocol.UnpreparedError
	WriteFailureError     = protocol.WriteFailureError
	WriteTimeoutError     = protocol.WriteTimeoutError
	ErrorMap              = protocol.ErrorMap
	OpType                = protocol.OpType
)

const (
	OpTypeRead  = protocol.OpTypeRead
	OpTypeWrite = protocol.OpTypeWrite
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
	// ConnectError reports that no host was reachable while a session was
	// connecting; it matches ErrNoConnectionsStarted.
	ConnectError = runtimeerrors.ConnectError
	HostError    = runtimeerrors.HostError
	// ServerMismatchError reports that Connect reached a database other than
	// the one its dialect names.
	ServerMismatchError = runtimeerrors.ServerMismatchError
)

// Sentinels.

var (
	ErrBusy   = runtimeerrors.ErrBusy
	ErrClosed = runtimeerrors.ErrClosed
	// ErrConditionalExec reports an Exec or ExecBatch call for a statement or
	// batch known to be conditional. Nothing was sent.
	ErrConditionalExec              = runtimeerrors.ErrConditionalExec
	ErrConnectionClosed             = runtimeerrors.ErrConnectionClosed
	ErrFrameTooBig                  = protocol.ErrFrameTooBig
	ErrKeyspaceDoesNotExist         = runtimeerrors.ErrKeyspaceDoesNotExist
	ErrNoConnections                = runtimeerrors.ErrNoConnections
	ErrNoConnectionsStarted         = runtimeerrors.ErrNoConnectionsStarted
	ErrNoHosts                      = runtimeerrors.ErrNoHosts
	ErrNoKeyspace                   = runtimeerrors.ErrNoKeyspace
	ErrNoRows                       = runtimeerrors.ErrNoRows
	ErrNoStreams                    = runtimeerrors.ErrNoStreams
	ErrNoTable                      = runtimeerrors.ErrNoTable
	ErrNotFound                     = runtimeerrors.ErrNotFound
	ErrPagingComplete               = runtimeerrors.ErrPagingComplete
	ErrPagingNoProgress             = runtimeerrors.ErrPagingNoProgress
	ErrPagingStateLimit             = runtimeerrors.ErrPagingStateLimit
	ErrReadHeaderTimeout            = protocol.ErrReadHeaderTimeout
	ErrReleased                     = cqltype.ErrReleased
	ErrScyllaSourcePortAlreadyInUse = transport.ErrScyllaSourcePortAlreadyInUse
	ErrSessionClosed                = runtimeerrors.ErrSessionClosed
	ErrSessionNotReady              = runtimeerrors.ErrSessionNotReady
	ErrStaleRows                    = runtimeerrors.ErrStaleRows
	ErrTabletsNotUsed               = runtimeerrors.ErrTabletsNotUsed
	ErrTimeoutNoResponse            = runtimeerrors.ErrTimeoutNoResponse
	ErrUseStmt                      = runtimeerrors.ErrUseStmt
)
