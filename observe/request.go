// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"context"
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
// concurrent use. A speculative attempt that loses can start or end after
// its request has ended.
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
	// exec.Options.CQLTrace.
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
	// RequestID is the ID of the attempt's request.
	RequestID uint64
	// Attempt is the 1-based number of the attempt within its request.
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
	// BatchStatements lists the CQL text of each statement of a batch, in
	// order, and is nil otherwise.
	BatchStatements []string
	// Parameters describes the statement's bind markers.
	Parameters cql.Columns
	// Routing describes the request for routing. A field the driver does
	// not know is empty or unset, such as Token when it cannot compute the
	// request's partition key.
	Routing routing.Info
	// ID identifies the request within the session; its attempts carry it as
	// RequestID.
	ID uint64
	// CQLTrace reports that the request's attempts ask the cluster to record
	// CQL trace sessions.
	CQLTrace bool
}

// RequestEvent describes how a request ended.
//
// The driver fills it; its zero value is empty.
type RequestEvent struct {
	// Err is the request's error, or nil when it succeeded.
	Err error
	// Statement is the CQL text of the request's statement, and empty for a
	// batch.
	Statement string
	// BatchStatements lists the CQL text of each statement of a batch, in
	// order, and is nil otherwise.
	BatchStatements []string
	// Parameters describes the statement's bind markers.
	Parameters cql.Columns
	// Result describes the execution, also when it failed.
	Result cql.Result
	// Routing describes the request for routing, as RequestStart.Routing
	// does.
	Routing routing.Info
	// ID identifies the request within the session; its attempts carry it as
	// RequestID.
	ID uint64
}
