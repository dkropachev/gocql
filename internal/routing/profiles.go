// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	model "github.com/scylladb/gocql/v2/internal/model"
)

type Target = model.Target

// QueryPlan returns the routes a request tries, in order, until one of
// them has a connection.
type QueryPlan interface{ Next() (Route, bool) }

// RoutingPolicy chooses the routes of a request in two steps, optimized for
// the happy path where the first route serves it:
//
//   - Pick returns the first route, cheaply: without building a plan and
//     without allocating. It returns false when it has no cheap answer, and
//     the request then starts with Fallback's routes.
//   - Fallback returns every route the request may take, in order. The
//     driver calls it only when the picked route fails or Pick returned
//     false, and skips the picked route if Fallback returns it again.
//
// Observe receives the outcome of every attempt. NewPlan combines Pick and
// Fallback into the plan a request follows.
type RoutingPolicy interface {
	Validate(*MetadataSnapshot) error
	Pick(model.RoutingInfo, *MetadataSnapshot) (Route, bool)
	Fallback(model.RoutingInfo, *MetadataSnapshot) QueryPlan
	Observe(AttemptEvent)
}
type AttemptEvent struct {
	Started   time.Time
	Err       error
	RequestID uint64
	Attempt   int
	Latency   time.Duration
	Target    Target
	// CQLTraceID is the CQL trace session the attempt's response named, when
	// the execution ran with CQL tracing and a response arrived.
	CQLTraceID             cqltype.Option[cqltype.UUID]
	Cancelled, Speculative bool
}
