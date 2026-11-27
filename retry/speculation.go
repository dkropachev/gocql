// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

// SpeculationPolicy schedules additional attempts of eligible executions.
//
// An execution is eligible when it is idempotent (cql.Statement.Idempotent,
// or exec.Options.Idempotent when set) and its statement is
// cql.OrdinaryStatement: for a batch, every member's. Conditional and
// unknown statements, SERIAL and LOCAL_SERIAL consistency, and streams never
// speculate, whatever the profile says.
type SpeculationPolicy interface {
	// Delay returns how long to wait before starting another attempt of the
	// execution s describes, and false to start no more. The driver calls
	// it on the request goroutine, or the timer's, so it must not block.
	Delay(s Speculation) (time.Duration, bool)
}

// SpeculationPolicyFunc adapts a function to SpeculationPolicy.
type SpeculationPolicyFunc func(Speculation) (time.Duration, bool)

// Delay calls f.
func (f SpeculationPolicyFunc) Delay(s Speculation) (time.Duration, bool) { return f(s) }

// NoSpeculation starts no additional attempts. Set exec.Options.Speculation
// to NoSpeculation{} to disable speculation for an execution while nil
// inherits the profile's policy.
//
// It has no fields; its zero value is ready to use.
type NoSpeculation struct{}

// Delay always returns false.
func (NoSpeculation) Delay(Speculation) (time.Duration, bool) { return 0, false }

// Speculation describes an execution that may start another attempt. The
// driver fills it; policies only read it. Later releases may add fields.
//
// Its zero value describes no execution.
type Speculation struct {
	// Deadline is when the page's context ends, and zero when it has no
	// deadline. Another attempt started after it cannot help.
	Deadline time.Time
	// Context is the execution's context: the caller's, bounded by the
	// driver's timeouts. Values the application attached with
	// context.WithValue reach the policy through it, such as a request's
	// priority. It is nil only in the zero Speculation.
	Context context.Context
	// Keyspace and Table name the statement's table, when known.
	Keyspace, Table string
	// Started is the number of attempts started so far, speculative ones
	// included.
	Started int
	// Elapsed is the time since the page's first attempt started.
	Elapsed time.Duration
	// LatencyP50, LatencyP95 and LatencyP99 are the session's latency
	// percentiles, as metrics.Snapshot reports them, and zero until the
	// session has measured enough requests.
	LatencyP50, LatencyP95, LatencyP99 time.Duration
	// Consistency is the execution's consistency level.
	Consistency cql.Consistency
}
