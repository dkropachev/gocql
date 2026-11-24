// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/metadata"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// Observer instruments requests and their attempts from the client side; it
// is the hook for client-side tracing such as OpenTelemetry. An Observer is
// any value that implements one or more of the hook interfaces,
// RequestObserver and AttemptObserver. config.Build and
// gocql.Session.CompileOptions reject one that implements none. The driver
// finds the hooks once, when the observer is installed, and skips the call
// and the event of a hook the observer does not implement. A later release adds hooks as new interfaces, so existing
// observers keep compiling.
//
// Implementations are shared between executions and must be safe for
// concurrent use. A speculative attempt that loses can start or end after
// its request has ended. A panic in any hook is logged and otherwise
// ignored.
type Observer any

// RequestObserver is the hook of an Observer called for each request.
type RequestObserver interface {
	// StartRequest is called once per request as it is admitted, with the
	// caller's context. The context it returns is passed to StartAttempt for
	// each of the request's attempts, so a span started for the request can
	// parent the attempts' spans; the driver uses it for nothing else and
	// never derives cancellation from it. The function it returns, if not
	// nil, is called once with the completed request.
	StartRequest(context.Context, RequestStart) (context.Context, func(RequestEvent))
}

// AttemptObserver is the hook of an Observer called for each attempt.
type AttemptObserver interface {
	// StartAttempt is called as each attempt, retry or speculative attempt
	// is sent, with the context StartRequest returned, or the caller's
	// context when the observer is no RequestObserver. The function it
	// returns, if not nil, is called once with the completed attempt;
	// routing.AttemptEvent.CQLTraceID links it to the server-side CQL trace
	// session when the request runs with exec.Options.CQLTrace.
	StartAttempt(context.Context, AttemptStart) func(routing2.AttemptEvent)
}

// AttemptStart describes an attempt as it is sent to its target.
//
// The driver fills it; its zero value is empty.
type AttemptStart struct {
	// Started is when the attempt was sent.
	Started time.Time
	// RequestID is the ID of the attempt's request.
	RequestID uint64
	// Attempt is the 1-based number of the attempt within its request.
	Attempt int
	// Target is the host, and shard, the attempt was sent to.
	Target metadata.Target
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
	// Routing describes the request for routing.
	Routing routing2.Info
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
	// Routing describes the request for routing.
	Routing routing2.Info
	// ID identifies the request within the session; its attempts carry it as
	// RequestID.
	ID uint64
}
