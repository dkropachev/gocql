// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// Kind groups errors by what a caller can do about them.
type Kind uint8

const (
	// KindUnknown is an error the driver does not classify.
	KindUnknown Kind = iota
	// KindTimeout: the server or the driver gave up waiting, or the caller's
	// deadline passed.
	KindTimeout
	// KindUnavailable: too few replicas were alive to try the request.
	KindUnavailable
	// KindOverloaded: the coordinator was overloaded or still bootstrapping.
	KindOverloaded
	// KindRateLimited: the request exceeded a per-partition rate limit.
	KindRateLimited
	// KindInvalid: the request or its values are wrong; retrying cannot help.
	KindInvalid
	// KindUnauthorized: authentication failed or permission was denied.
	KindUnauthorized
	// KindConnection: no connection could carry the request, or it broke.
	KindConnection
	// KindCanceled: the caller canceled the request.
	KindCanceled
	// KindClosed: the session or query was closed.
	KindClosed
	// KindProtocol: a peer violated the CQL protocol.
	KindProtocol
	// KindServer: the server failed while executing the request.
	KindServer
)

func (k Kind) String() string {
	switch k {
	case KindTimeout:
		return "timeout"
	case KindUnavailable:
		return "unavailable"
	case KindOverloaded:
		return "overloaded"
	case KindRateLimited:
		return "rate limited"
	case KindInvalid:
		return "invalid"
	case KindUnauthorized:
		return "unauthorized"
	case KindConnection:
		return "connection"
	case KindCanceled:
		return "canceled"
	case KindClosed:
		return "closed"
	case KindProtocol:
		return "protocol"
	case KindServer:
		return "server"
	default:
		return "unknown"
	}
}

// Transient reports whether the same request can succeed later, possibly on
// another host. Whether replaying it is safe is a separate question; see
// ExecutionStatusOf.
func (k Kind) Transient() bool {
	switch k {
	case KindTimeout, KindUnavailable, KindOverloaded, KindRateLimited, KindConnection:
		return true
	default:
		return false
	}
}

// KindOf classifies err. For a request that failed after several attempts it
// classifies the reason the request stopped.
func KindOf(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	var request *ExecutionError
	if errors.As(err, &request) {
		err = request.Err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return KindCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return KindTimeout
	}
	var server protocol.RequestError
	if errors.As(err, &server) {
		return serverKind(server)
	}
	var validation *cqltype.ValidationError
	var codec *cqltype.CodecError
	var schema *SchemaChangedError
	var unsupported *UnsupportedSettingError
	var protocolErr *cqltype.ProtocolError
	var frameErr protocol.ErrProtocol
	var malformed *protocol.MalformedFrameError
	var connect *ConnectError
	var network net.Error
	switch {
	case errors.Is(err, ErrClosed), errors.Is(err, ErrSessionClosed), errors.Is(err, ErrStaleRows), errors.Is(err, cqltype.ErrReleased):
		return KindClosed
	case errors.As(err, &validation), errors.As(err, &codec), errors.As(err, &schema), errors.As(err, &unsupported),
		errors.Is(err, ErrQueryArgLength), errors.Is(err, ErrUseStmt), errors.Is(err, ErrTooManyStmts),
		errors.Is(err, ErrNoKeyspace), errors.Is(err, ErrNoTable), errors.Is(err, ErrKeyspaceDoesNotExist),
		errors.Is(err, ErrBusy), errors.Is(err, ErrPagingComplete), errors.Is(err, ErrUnsupported):
		return KindInvalid
	case errors.As(err, &protocolErr), errors.As(err, &frameErr), errors.As(err, &malformed), errors.Is(err, protocol.ErrFrameTooBig):
		return KindProtocol
	case errors.Is(err, ErrTimeoutNoResponse), errors.Is(err, ErrTooManyTimeouts), errors.Is(err, protocol.ErrReadHeaderTimeout):
		return KindTimeout
	case errors.As(err, &network) && network.Timeout():
		return KindTimeout
	case errors.As(err, &connect), errors.Is(err, ErrNoConnections), errors.Is(err, ErrNoConnectionsInPool),
		errors.Is(err, ErrNoConnectionsStarted), errors.Is(err, ErrConnectionClosed), errors.Is(err, ErrHostDown),
		errors.Is(err, ErrNoPool), errors.Is(err, ErrNoStreams), errors.Is(err, ErrNoHosts), errors.Is(err, ErrSessionNotReady),
		errors.As(err, &network), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		return KindConnection
	}
	return KindUnknown
}

func serverKind(err protocol.RequestError) Kind {
	if _, ok := err.(*protocol.RequestErrRateLimitReached); ok {
		return KindRateLimited
	}
	switch err.GetCode() {
	case protocol.ErrCodeReadTimeout, protocol.ErrCodeWriteTimeout:
		return KindTimeout
	case protocol.ErrCodeUnavailable:
		return KindUnavailable
	case protocol.ErrCodeOverloaded, protocol.ErrCodeBootstrapping:
		return KindOverloaded
	case protocol.ErrCodeSyntax, protocol.ErrCodeInvalid, protocol.ErrCodeAlreadyExists, protocol.ErrCodeConfig,
		protocol.ErrCodeFunctionFailure, protocol.ErrCodeUnprepared:
		return KindInvalid
	case protocol.ErrCodeUnauthorized, protocol.ErrCodeCredentials:
		return KindUnauthorized
	case protocol.ErrCodeProtocol:
		return KindProtocol
	default:
		return KindServer
	}
}
