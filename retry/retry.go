// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package retry decides whether a failed request attempt is retried.
package retry

import (
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Action is what the driver does after a failed attempt. The zero Action is
// Stop. Later releases may add values, so a switch over them needs a default
// case.
type Action uint8

const (
	// Stop returns the error to the caller.
	Stop Action = iota
	// Same retries on the same host.
	Same
	// Next retries on the next host of the query plan.
	Next
	// IgnoreWrite treats the failed write as successful.
	IgnoreWrite
)

// String returns the retry action name, or Action(n) for a value it does not
// define.
func (a Action) String() string {
	switch a {
	case Stop:
		return "stop"
	case Same:
		return "same"
	case Next:
		return "next"
	case IgnoreWrite:
		return "ignore write"
	default:
		return fmt.Sprintf("Action(%d)", uint8(a))
	}
}

// Attempt describes a failed attempt. The driver fills it; policies only
// read it.
//
// Its zero value describes no failure.
type Attempt struct {
	Err error
	// Number is the 1-based number of the failed attempt.
	Number      int
	Idempotent  bool
	Consistency cql.Consistency
	// Conditional reports a lightweight transaction.
	Conditional bool
	// ExecutionStatus reports what this attempt may have done on the cluster.
	ExecutionStatus cqlerr.ExecutionStatus
}

// Decision is a policy's answer. The zero value stops retrying.
type Decision struct {
	Action Action
	// Consistency, when set, replaces the consistency of the next attempt.
	Consistency cql.Option[cql.Consistency]
	// Delay postpones the next attempt.
	Delay time.Duration
}

// Policy decides retries. The driver calls it on the request goroutine, so
// it must not block.
type Policy interface {
	Decide(Attempt) Decision
}

// Never stops after the first failed attempt. Set exec.Options.Retry to Never{}
// to disable retries for an execution while nil inherits the profile's policy.
//
// It has no fields; its zero value is ready to use.
type Never struct{}

// Decide always returns Stop.
func (Never) Decide(Attempt) Decision { return Decision{Action: Stop} }

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(Attempt) Decision

// Decide calls f.
func (f PolicyFunc) Decide(a Attempt) Decision { return f(a) }
