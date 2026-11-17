// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// Observer instruments requests and their attempts from the client side; it
// is the hook for client-side tracing such as OpenTelemetry. Implementations
// are shared between executions and must be safe for concurrent use.
//
// StartRequest is called once per request as it is admitted, with the
// caller's context. The context it returns is passed to StartAttempt for
// each of the request's attempts, so a span started for the request can
// parent the attempts' spans; the driver uses it for nothing else and never
// derives cancellation from it. The function it returns, if not nil, is
// called once with the completed request.
//
// StartAttempt is called as each attempt, retry or speculative attempt is
// sent. The function it returns, if not nil, is called once with the
// completed attempt; routing.AttemptEvent.CQLTraceID links it to the server-side CQL
// trace session when the request runs with exec.Options.CQLTrace.
//
// A speculative attempt that loses can start or end after its request has
// ended. A panic in any of these calls is logged and otherwise ignored.
type Observer interface {
	StartRequest(context.Context, RequestStart) (context.Context, func(RequestEvent))
	StartAttempt(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent)
}

// RequestStart describes a request as it is admitted, before its first attempt.
type RequestStart struct {
	Started         time.Time
	Statement       string
	BatchStatements []string
	Parameters      cql.Columns
	Routing         routing2.Info
	ID              uint64
	// CQLTrace reports that the request's attempts ask the cluster to record
	// CQL trace sessions.
	CQLTrace bool
}

// RequestEvent describes how a request ended.
type RequestEvent struct {
	Err             error
	Statement       string
	BatchStatements []string
	Parameters      cql.Columns
	Result          cql.Result
	Routing         routing2.Info
	ID              uint64
}
