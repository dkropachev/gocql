// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package retry decides whether a failed request attempt is retried.
package retry

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Action is what the driver does after a failed attempt.
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

// Attempt describes a failed attempt. The driver fills it; policies only
// read it.
type Attempt struct {
	Err error
	// Number is the 1-based number of the failed attempt.
	Number      int
	Idempotent  bool
	Consistency cql.Consistency
	// Conditional reports a lightweight transaction.
	Conditional bool
	// BytesWritten reports that the attempt may have been applied, that is
	// ExecutionStatus != cqlerr.NotExecuted.
	//
	// Deprecated: use ExecutionStatus.
	BytesWritten bool
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

// Never stops after the first failed attempt. Set Options.Retry to Never{}
// to disable retries for an execution while nil inherits the profile's policy.
type Never struct{}

// Decide always returns Stop.
func (Never) Decide(Attempt) Decision { return Decision{Action: Stop} }

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(Attempt) Decision

// Decide calls f.
func (f PolicyFunc) Decide(a Attempt) Decision { return f(a) }
