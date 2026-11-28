// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package retry decides whether a failed request attempt is retried.
package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/metadata"
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
	default:
		return fmt.Sprintf("Action(%d)", uint8(a))
	}
}

// Attempt describes a failed attempt. The driver fills it; policies only
// read it.
//
// Its zero value describes no failure.
type Attempt struct {
	// Context is the execution's context: the caller's, bounded by the
	// driver's timeouts. Values the application attached with
	// context.WithValue reach the policy through it. It is nil only in the
	// zero Attempt.
	Context context.Context
	// Err is the attempt's error.
	Err error
	// Keyspace and Table name the statement's table, when known.
	Keyspace, Table string
	// Attempt is the 1-based number of the failed attempt.
	Attempt int
	// Idempotent reports that the request was marked idempotent.
	Idempotent bool
	// Consistency is the consistency level the attempt used.
	Consistency cql.Consistency
	// StatementKind classifies the request's statement, or a batch's statements
	// together: ConditionalStatement when one of them is conditional, else
	// OrdinaryStatement. A SERIAL or LOCAL_SERIAL read is OrdinaryStatement,
	// though the driver routes it as it routes a conditional statement. The
	// driver resolves an UnknownStatement before sending it (see
	// cql.UnknownStatement); a policy should still treat anything but
	// OrdinaryStatement as a possible lightweight transaction.
	StatementKind cql.StatementKind
	// Batch is the kind of batch the request sends; it is unset for any
	// other request.
	Batch cql.Option[cql.BatchKind]
	// ExecutionStatus reports what this attempt may have done on the cluster.
	ExecutionStatus cqlerr.ExecutionStatus
	// Target is the host, and shard, the failed attempt was sent to, as in
	// routing.AttemptEvent.Target. It is zero when the attempt failed
	// before a connection was chosen.
	Target metadata.Target
	// Elapsed is the time since the page's first attempt started, the
	// failed attempt included, as in Speculation.Elapsed. A policy that
	// bounds a page's retries by time reads it.
	Elapsed time.Duration
}

// Retryable reports whether sending the request again after this attempt can
// succeed and is safe, as cqlerr.Retryable does for the error a call
// returns: Err's Kind is Transient, and the attempt did not execute
// (ExecutionStatus is cqlerr.NotExecuted) or the request is Idempotent and
// not a possible lightweight transaction (StatementKind is
// cql.OrdinaryStatement). Err holds no cqlerr.ExecutionError, so
// cqlerr.Retryable(a.Err) cannot see the request's idempotence; a policy
// reads this instead.
//
// It is false for the zero Attempt.
func (a Attempt) Retryable() bool {
	if a.Err == nil || !cqlerr.KindOf(a.Err).Transient() {
		return false
	}
	switch a.ExecutionStatus {
	case cqlerr.NotExecuted:
		return true
	case cqlerr.MaybeExecuted, cqlerr.PartiallyExecuted:
		return a.Idempotent && a.StatementKind == cql.OrdinaryStatement
	default:
		return false
	}
}

// Decision is a policy's answer. The zero value stops retrying.
type Decision struct {
	// Action is what the driver does next.
	Action Action
	// Consistency, when set, replaces the consistency of the next attempt.
	Consistency cql.Option[cql.Consistency]
	// Delay postpones the next attempt.
	Delay time.Duration
}

// Policy decides retries. The driver calls it on the request goroutine, so
// it must not block.
type Policy interface {
	// Decide returns what to do after the failed attempt.
	Decide(Attempt) Decision
}

// NeverRetry stops after the first failed attempt. Set
// execopt.Options.Retry to NeverRetry{} to disable retries for an execution
// while nil inherits the profile's policy.
//
// It has no fields; its zero value is ready to use.
type NeverRetry struct{}

// Decide always returns Stop.
func (NeverRetry) Decide(Attempt) Decision { return Decision{Action: Stop} }

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(Attempt) Decision

// Decide calls f.
func (f PolicyFunc) Decide(a Attempt) Decision { return f(a) }
