// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"context"
	"net/netip"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

// Hooks instruments requests and their attempts from the client side; it is
// the hook for client-side tracing such as OpenTelemetry, and for metrics
// finer than package metrics keeps. Each field is one hook, and a nil field
// is a hook the driver skips, with the work of filling its event. Join
// combines several Hooks. A later release adds hooks as new fields, so
// existing Hooks keep compiling and observe what they did.
//
// The functions are shared between executions and must be safe for
// concurrent use. The driver calls Request on the caller's goroutine, and
// the end function it returns on the goroutine that ends the request, which
// may be another one, such as the one that closes its Rows. Attempt and its
// end function run on the goroutine that sends the attempt: a retry or a
// later page may run on another goroutine than the attempt before it, and
// the speculative attempts of a page run on goroutines of their own,
// concurrently with the page's first attempt and with each other, so their
// hooks run at the same time. A speculative attempt that loses can start or
// end after its request has ended, and after the request's end function
// returned.
//
// A panic in a hook, or in a function a hook returns, neither reaches the
// caller nor stops the request: the driver recovers it and logs it at error
// level with the request's context, the one Request returned or the caller's
// without a Request hook. The driver calls each end function a hook returns
// once, as its request or attempt ends, so a panic that Join recovers in a
// start hook and raises from the end function it returns is logged then.
//
// The zero Hooks observes nothing.
type Hooks struct {
	// Request is called once per request as it is admitted, with the
	// caller's context. The context it returns is passed to Attempt for each
	// of the request's attempts, so a span started for the request can
	// parent the attempts' spans; the driver uses it for nothing else and
	// never derives cancellation from it. The function it returns, if not
	// nil, is called once with the completed request.
	Request func(ctx context.Context, start RequestStart) (context.Context, func(RequestEvent))
	// Attempt is called as each attempt, retry or speculative attempt is
	// sent, with the context Request returned, or the caller's context
	// without a Request hook. The function it returns, if not nil, is called
	// once with the completed attempt; routing.AttemptEvent.CQLTraceID links
	// it to the server-side CQL trace session when the request runs with
	// CQL tracing.
	Attempt func(ctx context.Context, start AttemptStart) func(routing.AttemptEvent)
}

// IsZero reports whether h has no hook.
func (h Hooks) IsZero() bool { return h.Request == nil && h.Attempt == nil }

// AttemptStart describes an attempt as it is sent to its target.
//
// The driver fills it; its zero value is empty.
type AttemptStart struct {
	// Started is when the attempt was sent.
	Started time.Time
	// Address is the host:port of the connection the attempt is sent on, or
	// the socket path of a Unix socket connection.
	Address string
	// AddrPort is Address as an IP address and port, as an OpenTelemetry
	// span's server.address and server.port take it, and is the zero
	// AddrPort for a Unix socket connection.
	AddrPort netip.AddrPort
	// RequestID is the ID of the attempt's request.
	RequestID uint64
	// Page is the 1-based number of the page the attempt fetches within
	// its request.
	Page int
	// Attempt is the 1-based number of the attempt within its page.
	Attempt int
	// Target is the host, and shard, the attempt was sent to.
	Target metadata.Target
	// Consistency is the consistency level the attempt uses.
	Consistency cql.Consistency
	// Speculative reports a speculative attempt.
	Speculative bool
}

// RequestStart describes a request as it is admitted, before its first attempt.
//
// The driver fills it; its zero value is empty.
type RequestStart struct {
	// Started is when the request was admitted.
	Started time.Time
	// Statement is the CQL text of the request's statement, and empty for a
	// batch.
	Statement string
	// BatchStatements lists the statements of a batch, in order, and is nil
	// otherwise.
	BatchStatements []BatchStatement
	// Parameters describes the statement's bind markers.
	Parameters cql.Columns
	// Values are the values bound to the statement, and empty for a batch,
	// whose BatchStatements carry them. They are a borrowed view: see
	// BoundValues.
	Values BoundValues
	// Routing describes the request for routing. A field the driver does
	// not know is empty or unset, such as Token when it cannot compute the
	// request's partition key.
	Routing routing.Info
	// SessionID identifies the session that runs the request: the session
	// attribute of the driver's log records, and the SESSION_ID its
	// connections report to the cluster.
	SessionID cql.UUID
	// ID identifies the request within the session; its attempts carry it as
	// RequestID.
	ID uint64
	// PageSize is the rows per page the request asks for, as its options
	// resolved it; cql.Unpaged asks for the whole result in one page.
	PageSize int
	// CQLTrace reports that the request's attempts ask the cluster to record
	// CQL trace sessions, by execopt.Options.CQLTrace or by the profile's
	// config.Profile.CQLTraceSampler.
	CQLTrace bool
}

// BatchStatement is one statement of a batch request.
//
// The driver fills it; its zero value is empty.
type BatchStatement struct {
	// Statement is the statement's CQL text.
	Statement string
	// Values are the values bound to the statement, a borrowed view: see
	// BoundValues.
	Values BoundValues
}

// RequestEvent describes how a request ended. What the request ran is in
// the RequestStart its Request hook received.
//
// The driver fills it; its zero value is empty.
type RequestEvent struct {
	// Err is the request's error, or nil when it succeeded.
	Err error
	// Finished is when the request ended: when its last page arrived or
	// its rows were closed, or when it failed.
	Finished time.Time
	// Result describes the execution, also when it failed.
	Result cql.Result
	// Pages is how many pages the request fetched, the failed one included:
	// one for a statement whose result fits a page or that returns no rows.
	Pages int
}
