// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"time"
)

// SpeculationPolicy schedules additional attempts of eligible executions.
//
// An execution is eligible when it is idempotent (cql.Statement.Idempotent,
// or exec.Options.Idempotent when set) and its statement is
// cql.OrdinaryStatement: for a batch, every member's. Conditional and
// unknown statements, SERIAL and LOCAL_SERIAL consistency, and streams never
// speculate, whatever the profile says.
type SpeculationPolicy interface {
	// Delay returns how long to wait before starting another attempt, given
	// the number of attempts started so far, and false to start no more.
	Delay(started int) (time.Duration, bool)
}

// SpeculationPolicyFunc adapts a function to SpeculationPolicy.
type SpeculationPolicyFunc func(started int) (time.Duration, bool)

// Delay calls f.
func (f SpeculationPolicyFunc) Delay(started int) (time.Duration, bool) { return f(started) }
