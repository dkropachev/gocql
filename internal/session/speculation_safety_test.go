// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// TestSpeculationSafety pins which executions speculate and what their
// results report.
func TestSpeculationSafety(t *testing.T) {
	profileFor := func(routes *runtimePolicy, policy retry.Policy) *settings.Profile {
		profile := settings.InternalDefaultProfile()
		profile.Routing = routes
		profile.Retry = policy
		profile.Speculation = runtimeSpeculation{}
		return new(profile)
	}

	// A request not marked idempotent never speculates, however long its
	// first attempt takes.
	t.Run("NonIdempotentNeverSpeculates", func(t *testing.T) {
		_, routes, run, conns, peers := hostsSession(t, 2)
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
		_, routes, run, conns, peers := hostsSession(t, 2)
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
		_, routes, run, conns, peers := hostsSession(t, 2)
		release := make(chan struct{})
		go func() {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			select { // fail after the speculative attempt
			case <-release:
			case <-time.After(5 * time.Second):
			}
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
		s, routes, _, conns, peers := hostsSession(t, 2)
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
		bounded, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		ctx, cancel := context.WithCancel(bounded)
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
		// The message names both attempts, the last one with the cause.
		msg := err.Error()
		if !strings.HasPrefix(msg, "gocql: request: #2 "+routes.targets[1].HostID.String()+": context canceled") || !strings.Contains(msg, "earlier attempts: #1 "+routes.targets[0].HostID.String()+": context canceled") {
			t.Fatalf("message %q", msg)
		}
	})

	// A retry and a speculative attempt run side by side: the first host
	// fails after the speculative attempt started on the second, Safe moves
	// the retry to the third, and its success ends the execution and
	// cancels the speculative attempt. Each is counted once.
	t.Run("RetryAlongsideSpeculation", func(t *testing.T) {
		s, routes, run, conns, peers := hostsSession(t, 3)
		speculated := make(chan struct{})
		go func() {
			stream := typedTestReadRequest(t, conns[0], peers[0])
			select {
			case <-speculated:
			case <-time.After(5 * time.Second):
			}
			_, _ = peers[0].Write(serverError(t, stream, cqlerr.CodeOverloaded, nil))
		}()
		go func() {
			typedTestReadRequest(t, conns[1], peers[1]) // never answered
			close(speculated)
		}()
		go func() {
			stream := typedTestReadRequest(t, conns[2], peers[2])
			_, _ = peers[2].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "row")))
		}()
		if err := run(&Options{Profile: profileFor(routes, retry.Safe{}), Idempotent: true}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for len(routes.observed()) < 3 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		byAttempt := map[int]routing2.AttemptEvent{}
		for _, e := range routes.observed() {
			byAttempt[e.Attempt] = e
		}
		first, spec, retried := byAttempt[1], byAttempt[2], byAttempt[3]
		if len(byAttempt) != 3 || first.Target.HostID != routes.targets[0].HostID || first.Err == nil ||
			!spec.Speculative || spec.Target.HostID != routes.targets[1].HostID || !spec.Cancelled ||
			retried.Speculative || retried.Target.HostID != routes.targets[2].HostID || retried.Err != nil {
			t.Fatalf("attempt events %+v", byAttempt)
		}
		if got := s.Metrics(); got.Attempts != 3 || got.Retries != 1 || got.Speculations != 1 {
			t.Fatalf("metrics %+v, want 3 attempts, 1 retry, 1 speculation", got)
		}
	})
}
