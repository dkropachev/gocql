// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

// runUnavailable runs one read whose first attempt the node answers with
// Unavailable, under profile, and returns its error.
func runUnavailable(t *testing.T, profile settings.Profile, timeout time.Duration) error {
	t.Helper()
	s, conns, peers := runtimeTestSession(t, 1)
	profile.Timeout = timeout
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeUnavailable, unavailableBody()))
	}()
	bound := runtimeQuery(t)
	defer bound.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: new(profile)}))
	if page != nil {
		page.Close()
	}
	return err
}

// TestInvalidRetryDecisionWrapsAttempt checks that a decision the driver
// cannot carry out fails the request with a cql.ValidationError that still
// wraps the attempt's error.
func TestInvalidRetryDecisionWrapsAttempt(t *testing.T) {
	for _, general := range []bool{false, true} {
		for name, decision := range map[string]retry.Decision{
			"Action":      {Action: retry.Action(3)},
			"Consistency": {Action: retry.Same, Consistency: cql.Some(cql.Consistency(0xff))},
			"Delay":       {Action: retry.Same, Delay: -time.Second},
		} {
			t.Run(map[bool]string{false: "Direct/", true: "General/"}[general]+name, func(t *testing.T) {
				profile := settings.InternalDefaultProfile()
				profile.Retry = retry.PolicyFunc(func(retry.Attempt) retry.Decision { return decision })
				if general {
					profile.Speculation = neverSpeculate{}
				}
				err := runUnavailable(t, profile, 0)
				if _, ok := errors.AsType[*cql.ValidationError](err); !ok {
					t.Fatalf("err = %v; want a validation error", err)
				}
				if _, ok := errors.AsType[*cqlerr.UnavailableError](err); !ok {
					t.Fatalf("err = %v; lost the attempt's error", err)
				}
			})
		}
	}
}

// TestAttemptAsAfterTimeout checks that a request whose Timeout runs out
// while a retry waits after Unavailable reports ErrRequestTimeout, and that
// cqlerr.AttemptAs still finds the Unavailable, which errors.As does not.
func TestAttemptAsAfterTimeout(t *testing.T) {
	for _, general := range []bool{false, true} {
		t.Run(map[bool]string{false: "Direct", true: "General"}[general], func(t *testing.T) {
			profile := settings.InternalDefaultProfile()
			profile.Retry = retry.PolicyFunc(func(retry.Attempt) retry.Decision {
				return retry.Decision{Action: retry.Same, Delay: time.Hour}
			})
			if general {
				profile.Speculation = neverSpeculate{}
			}
			err := runUnavailable(t, profile, 50*time.Millisecond)
			if !errors.Is(err, cqlerr.ErrRequestTimeout) || cqlerr.KindOf(err) != cqlerr.KindTimeout {
				t.Fatalf("err = %v (%v); want the request timeout", err, cqlerr.KindOf(err))
			}
			if _, ok := errors.AsType[*cqlerr.UnavailableError](err); ok {
				t.Fatal("errors.As found an earlier attempt's error")
			}
			unavailable, ok := cqlerr.AttemptAs[*cqlerr.UnavailableError](err)
			if !ok || unavailable == nil {
				t.Fatalf("AttemptAs missed the Unavailable of %v", err)
			}
		})
	}
}

// TestRetryAttemptDescribesFailure checks the fields a retry policy reads
// besides the error: where the attempt went, the table and the time since
// the page's first attempt.
func TestRetryAttemptDescribesFailure(t *testing.T) {
	for _, general := range []bool{false, true} {
		t.Run(map[bool]string{false: "Direct", true: "General"}[general], func(t *testing.T) {
			var mu sync.Mutex
			var seen []retry.Attempt
			profile := settings.InternalDefaultProfile()
			profile.Retry = retry.PolicyFunc(func(a retry.Attempt) retry.Decision {
				mu.Lock()
				seen = append(seen, a)
				mu.Unlock()
				return retry.Decision{}
			})
			if general {
				profile.Speculation = neverSpeculate{}
			}
			err := runUnavailable(t, profile, 0)
			if _, ok := errors.AsType[*cqlerr.UnavailableError](err); !ok {
				t.Fatalf("err = %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 1 {
				t.Fatalf("Decide called %d times", len(seen))
			}
			a := seen[0]
			if a.Target.HostID.IsZero() || a.Attempt != 1 || a.Elapsed <= 0 || a.Context == nil {
				t.Fatalf("Attempt = %+v", a)
			}
			failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
			if !ok || failure.StatementKind != cql.OrdinaryStatement {
				t.Fatalf("err = %#v; want the statement kind recorded", err)
			}
			if general && failure.Result.SpeculationSkipped != cql.SpeculationSkippedNotIdempotent {
				t.Fatalf("SpeculationSkipped = %v, want not idempotent", failure.Result.SpeculationSkipped)
			}
			if !general && failure.Result.SpeculationSkipped != cql.SpeculationNotSkipped {
				t.Fatalf("SpeculationSkipped = %v without a policy", failure.Result.SpeculationSkipped)
			}
		})
	}
}

// TestSerialReadReportsOrdinaryKind checks that a SERIAL or LOCAL_SERIAL
// read, routed as a conditional statement, reports its own kind to the
// retry policy and in its ExecutionError, so ConditionalUnknown does not
// call a failed read an LWT that may have applied.
func TestSerialReadReportsOrdinaryKind(t *testing.T) {
	for _, consistency := range []cql.Consistency{cql.Serial, cql.LocalSerial} {
		t.Run(consistency.String(), func(t *testing.T) {
			var mu sync.Mutex
			var seen []retry.Attempt
			profile := settings.InternalDefaultProfile()
			profile.Consistency = consistency
			profile.Retry = retry.PolicyFunc(func(a retry.Attempt) retry.Decision {
				mu.Lock()
				seen = append(seen, a)
				mu.Unlock()
				return retry.Decision{}
			})
			err := runUnavailable(t, profile, 0)
			if _, ok := errors.AsType[*cqlerr.UnavailableError](err); !ok {
				t.Fatalf("err = %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 1 || seen[0].StatementKind != cql.OrdinaryStatement {
				t.Fatalf("attempts = %+v; want one of an ordinary statement", seen)
			}
			failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
			if !ok || failure.StatementKind != cql.OrdinaryStatement {
				t.Fatalf("err = %#v; want an ordinary statement", err)
			}
			if cqlerr.ConditionalUnknown(err) {
				t.Fatal("ConditionalUnknown reports a SERIAL read")
			}
		})
	}
}
