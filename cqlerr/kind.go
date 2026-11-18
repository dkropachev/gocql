// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import "fmt"

// Kind groups errors by what a caller can do about them. Server errors and the
// driver's error types report theirs with a Kind method; KindOf classifies any
// error. The zero Kind is KindUnknown. Later releases may add values, so a
// switch over them needs a default case.
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

// String returns the kind name, or Kind(n) for a value it does not define.
func (k Kind) String() string {
	switch k {
	case KindUnknown:
		return "unknown"
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
		return fmt.Sprintf("Kind(%d)", uint8(k))
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
