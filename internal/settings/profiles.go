// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/retry"
)

type Profile struct {
	Retry                retry.Policy
	Routing              routing.RoutingPolicy
	Speculation          retry.SpeculationPolicy
	Timeout, PageTimeout time.Duration
	Prefetch             int
	PageSize             int32
	SerialConsistency    cql.Option[cql.Consistency]
	Consistency          cql.Consistency
	// unbuilt marks a public Profile that ProfileBuilder.Build did not make,
	// such as a struct literal: its unset fields are zero, not the driver's
	// defaults, so validation rejects it.
	unbuilt bool
}

// InternalMarkUnbuilt records that p comes from a public Profile that
// ProfileBuilder.Build did not make.
func (p *Profile) InternalMarkUnbuilt() { p.unbuilt = true }

// InternalBuilt reports whether p was not marked by InternalMarkUnbuilt.
func (p *Profile) InternalBuilt() bool { return !p.unbuilt }

func InternalDefaultProfile() Profile {
	return Profile{Consistency: cql.LocalQuorum, SerialConsistency: cql.Option[cql.Consistency]{Value: cql.LocalSerial, Set: true}, Timeout: 30 * time.Second, PageSize: 5000, Prefetch: 1, Retry: retry.Safe{MaxAttempts: 3}}
}
func (p Profile) ValidateProfile() error {
	if p.unbuilt {
		return cqltype.TypeValidation("profile", "not built: a Profile literal leaves unset fields zero instead of the driver's defaults; build it with config.NewProfile")
	}
	if !InternalValidConsistency(p.Consistency) {
		return cqltype.TypeValidation("consistency", "invalid consistency")
	}
	if p.SerialConsistency.Set && p.SerialConsistency.Value != cql.Serial && p.SerialConsistency.Value != cql.LocalSerial {
		return cqltype.TypeValidation("serial consistency", "must be SERIAL or LOCAL_SERIAL")
	}
	if p.Timeout < 0 || p.PageTimeout < 0 {
		return cqltype.TypeValidation("timeout", "must not be negative")
	}
	if p.PageSize <= 0 {
		return cqltype.TypeValidation("page size", "must be positive")
	}
	if p.Prefetch < 0 || p.Prefetch > 1 {
		return cqltype.TypeValidation("prefetch", "must be zero or one")
	}
	return nil
}
func InternalValidConsistency(c cql.Consistency) bool {
	return c >= cql.Any && c <= cql.LocalOne
}

// SpeculationPolicy schedules additional attempts of eligible executions.
// Delay receives the number of attempts started so far and returns how long
// to wait before starting another, and whether to start one at all.

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
// completed attempt; AttemptEvent.CQLTraceID links it to the server-side CQL
// trace session when the request runs with exec.Options.CQLTrace.
//
// A speculative attempt that loses can start or end after its request has
// ended. A panic in any of these calls is logged and otherwise ignored.
