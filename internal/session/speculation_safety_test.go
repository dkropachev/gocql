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

// TestSpeculationSafety pins which executions speculate and what their
// results report.
func TestSpeculationSafety(t *testing.T) {
	profileFor := func(routes *recordingRoutes, policy retry.Policy) *settings.Profile {
		profile := settings.InternalDefaultProfile()
		profile.Routing = routes
		profile.Retry = policy
		profile.Speculation = runtimeSpeculation{}
		return new(profile)
	}

	// A request not marked idempotent never speculates, however long its
	// first attempt takes.
	t.Run("NonIdempotentNeverSpeculates", func(t *testing.T) {
		_, routes, run, conns, peers := twoHostSession(t)
		second := silent(t, peers[1])
		go func() {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			time.Sleep(50 * time.Millisecond) // well past the 1ms speculation delay
			_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
		}()
		if err := run(&Options{Profile: profileFor(routes, retry.Safe{})}); err != nil {
			t.Fatal(err)
		}
		peers[1].Close()
		if !<-second {
			t.Fatal("a speculative attempt reached the second host")
		}
		if n := len(routes.observed()); n != 1 {
			t.Fatalf("%d attempts, want 1", n)
		}
	})

	// The winner ends the execution and cancels the losing attempt; the
	// loser's event says it was cancelled and speculative attempts are
	// marked.
	t.Run("LoserCancelled", func(t *testing.T) {
		_, routes, run, conns, peers := twoHostSession(t)
		go func() {
			// The primary never answers.
			typedTestReadRequest(t, conns[0], peers[0])
		}()
		go func() {
			stream := typedTestReadRequest(t, conns[1], peers[1])
			_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
		}()
		if err := run(&Options{Profile: profileFor(routes, retry.Never{}), Idempotent: true}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for len(routes.observed()) < 2 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		events := routes.observed()
		if len(events) != 2 {
			t.Fatalf("%d attempt events, want 2", len(events))
		}
		for _, e := range events {
			switch e.Attempt {
			case 1:
				if e.Speculative || !e.Cancelled || e.Err == nil {
					t.Fatalf("loser event %+v, want a cancelled primary", e)
				}
			case 2:
				if !e.Speculative || e.Cancelled || e.Err != nil {
					t.Fatalf("winner event %+v, want a successful speculative attempt", e)
				}
			default:
				t.Fatalf("unexpected attempt %+v", e)
			}
		}
	})

	// When every attempt fails, the error lists each of them, speculative
	// ones included, with the host it ran on.
	t.Run("EveryFailedAttemptReported", func(t *testing.T) {
		_, routes, run, conns, peers := twoHostSession(t)
		release := make(chan struct{})
		go func() {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			<-release // fail after the speculative attempt
			_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeOverloaded, nil))
		}()
		go func() {
			stream := typedTestReadRequest(t, conns[1], peers[1])
			_, _ = peers[1].Write(serverError(t, stream, cqlerr.CodeOverloaded, nil))
			close(release)
		}()
		err := run(&Options{Profile: profileFor(routes, retry.Never{}), Idempotent: true})
		failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
		if !ok {
			t.Fatalf("not an ExecutionError: %v", err)
		}
		if failure.Result.Attempts != 2 || len(failure.Attempts) != 2 {
			t.Fatalf("result %d attempts, error lists %+v; want 2", failure.Result.Attempts, failure.Attempts)
		}
		hosts := map[cql.UUID]int{}
		for _, a := range failure.Attempts {
			hosts[a.HostID] = a.Number
			if a.Status != cqlerr.NotExecuted {
				t.Fatalf("attempt %+v", a)
			}
		}
		if hosts[routes.targets[0].HostID] != 1 || hosts[routes.targets[1].HostID] != 2 {
			t.Fatalf("attempts %+v, want #1 on the first host and #2 on the second", failure.Attempts)
		}
	})

	// An execution its context ends lists the attempts it stopped waiting
	// for, as possibly applied.
	t.Run("CancelledAttemptsReported", func(t *testing.T) {
		s, routes, _, conns, peers := twoHostSession(t)
		var wg sync.WaitGroup
		wg.Add(2)
		for i := range conns {
			go func() {
				defer wg.Done()
				typedTestReadRequest(t, conns[i], peers[i]) // never answered
			}()
		}
		bound := runtimeQuery(t)
		defer bound.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { wg.Wait(); cancel() }()
		page, err := RunPage(ctx, s, bound, cql.StartPage, 0, testOptions(Options{Profile: profileFor(routes, retry.Never{}), Idempotent: true}))
		if page != nil {
			page.Close()
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want the cancellation: %v", err)
		}
		failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
		if !ok {
			t.Fatalf("not an ExecutionError: %v", err)
		}
		if failure.ExecutionStatus() != cqlerr.MaybeExecuted || failure.Result.Attempts != 2 || len(failure.Attempts) != 2 {
			t.Fatalf("status %v, result %d attempts, error lists %+v; want 2 possibly applied", failure.ExecutionStatus(), failure.Result.Attempts, failure.Attempts)
		}
		for i, a := range failure.Attempts {
			if a.Number != i+1 || a.HostID != routes.targets[i].HostID || a.Status != cqlerr.MaybeExecuted || !errors.Is(a.Err, context.Canceled) {
				t.Fatalf("attempt %d: %+v", i+1, a)
			}
		}
	})
}
