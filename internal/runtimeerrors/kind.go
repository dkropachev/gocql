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

// kinded is an error that knows its Kind: every server error and every
// error type the driver defines.
type kinded interface {
	error
	Kind() Kind
}

// rank orders the errors of one tree: when it holds several, KindOf reports
// the one of lowest rank. A cancellation explains the failure better than
// the server error of an earlier attempt, and a server error better than the
// connection the driver closed because of it.
type rank uint8

const (
	rankCanceled rank = iota
	rankDeadline
	rankServer
	rankClosed
	rankInvalid
	rankProtocol
	rankTimeout
	rankConnection
	rankNone
)

func rankOf(k Kind) rank {
	switch k {
	case KindClosed:
		return rankClosed
	case KindInvalid:
		return rankInvalid
	case KindProtocol:
		return rankProtocol
	case KindTimeout:
		return rankTimeout
	case KindConnection:
		return rankConnection
	default:
		return rankNone
	}
}

// serverKind classifies a server error. The driver's own have a Kind method;
// another implementation is classified by its code.
func serverKind(err protocol.ServerError) Kind {
	if k, ok := err.(kinded); ok {
		return k.Kind()
	}
	return protocol.CodeKind(err.Code())
}

// sentinelKind classifies an error by identity: the driver's sentinels and
// the standard library errors a broken connection reports. Every sentinel is
// a comparable value, so the comparison cannot panic.
func sentinelKind(err error) Kind {
	switch err {
	case ErrClosed, ErrSessionClosed, ErrStaleRows, cqltype.ErrReleased: //nolint:errorlint // one node of a tree walkErrors walks
		return KindClosed
	case ErrUseStmt, ErrNoKeyspace, ErrNoTable, ErrKeyspaceDoesNotExist, ErrBusy, ErrConditionalExec, ErrPagingComplete:
		return KindInvalid
	case protocol.ErrFrameTooBig:
		return KindProtocol
	case ErrTimeoutNoResponse, protocol.ErrReadHeaderTimeout:
		return KindTimeout
	case ErrNoConnections, ErrNoConnectionsStarted, ErrConnectionClosed, ErrNoStreams, ErrNoHosts, ErrSessionNotReady,
		io.EOF, io.ErrUnexpectedEOF, net.ErrClosed:
		return KindConnection
	}
	return KindUnknown
}

// kindWalk is the state of one KindOf walk over an error tree.
type kindWalk struct {
	// network is the first net.Error of the tree.
	network net.Error
	kind    Kind
	rank    rank
}

// visit classifies one error of the tree, not what it wraps, and reports
// whether the walk must go on.
func (w *kindWalk) visit(err error) bool {
	if err == context.Canceled { //nolint:errorlint // one node of a tree walkErrors walks
		w.kind, w.rank = KindCanceled, rankCanceled
		return false
	}
	if w.rank <= rankDeadline {
		return true // only a cancellation outranks a deadline
	}
	if err == context.DeadlineExceeded { //nolint:errorlint // one node of a tree walkErrors walks
		w.kind, w.rank = KindTimeout, rankDeadline
		return true
	}
	if w.rank <= rankServer {
		return true
	}
	if server, ok := err.(protocol.ServerError); ok { //nolint:errorlint // one node of a tree walkErrors walks
		// The first server error of the tree decides, as errors.As finds it.
		w.kind, w.rank = serverKind(server), rankServer
		return true
	}
	k := sentinelKind(err)
	if k == KindUnknown {
		if typed, ok := err.(kinded); ok { //nolint:errorlint // one node of a tree walkErrors walks
			k = typed.Kind()
		}
	}
	if r := rankOf(k); r < w.rank {
		w.kind, w.rank = k, r
	}
	if w.network == nil {
		w.network, _ = err.(net.Error) //nolint:errorlint // one node of a tree walkErrors walks
	}
	return true
}

// KindOf classifies err. For a request that failed after several attempts it
// classifies the reason the request stopped.
//
// It walks err's tree once, as errors.Is and errors.As do. When the tree
// holds several classified errors, a cancellation wins over a deadline, a
// deadline over a server error, and a server error over the driver's own
// errors, which rank closed, invalid, protocol, timeout, then connection. A
// net.Error that is no driver error classifies as a timeout when the first
// one of the tree times out, and as a connection failure otherwise.
func KindOf(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	if request, ok := errors.AsType[*ExecutionError](err); ok {
		err = request.Err
	}
	w := kindWalk{kind: KindUnknown, rank: rankNone}
	walkErrors(err, &w, nil)
	if w.network != nil && w.rank > rankTimeout {
		if w.network.Timeout() {
			return KindTimeout
		}
		return KindConnection
	}
	return w.kind
}

// walkErrors calls kinds.visit, or statuses.visit when kinds is nil, for err
// and every error it wraps, depth first in the order errors.Is and errors.As
// use, until visit returns false. It reports whether the walk ran to the
// end. It takes concrete walkers, not an interface or a function, so that
// they stay on the caller's stack.
func walkErrors(err error, kinds *kindWalk, statuses *statusWalk) bool {
	for err != nil {
		var more bool
		if kinds != nil {
			more = kinds.visit(err)
		} else {
			more = statuses.visit(err)
		}
		if !more {
			return false
		}
		switch wrapper := err.(type) { //nolint:errorlint // unwraps one level, as errors.Is does
		case interface{ Unwrap() error }:
			err = wrapper.Unwrap()
		case interface{ Unwrap() []error }:
			for _, inner := range wrapper.Unwrap() {
				if !walkErrors(inner, kinds, statuses) {
					return false
				}
			}
			return true
		default:
			return true
		}
	}
	return true
}
