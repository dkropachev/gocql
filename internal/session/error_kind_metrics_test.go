// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	implCql "github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// TestErrorKindMetrics checks that the metrics count a timeout apart from a
// cancellation, and every failed execution by kind.
func TestErrorKindMetrics(t *testing.T) {
	t.Run("Executions", func(t *testing.T) {
		s, _, peers := runtimeTestSession(t, 1)
		defer s.Close()
		// The node reads every request and never answers.
		go func() { _, _ = io.Copy(io.Discard, peers[0]) }()
		b := runtimeQuery(t)
		defer b.Close()

		// The driver's own timeout is a timeout.
		_, err := Exec(context.Background(), s, b, testOptions(Options{Timeout: implCql.Option[time.Duration]{Value: 20 * time.Millisecond, Set: true}}))
		if cqlerr.KindOf(err) != cqlerr.KindTimeout {
			t.Fatalf("driver timeout: %v (%v)", err, cqlerr.KindOf(err))
		}
		// The caller's deadline and cancellation are cancellations.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err = Exec(ctx, s, b, CompiledOptions{})
		cancel()
		if cqlerr.KindOf(err) != cqlerr.KindCanceled {
			t.Fatalf("deadline: %v (%v)", err, cqlerr.KindOf(err))
		}
		ctx, cancel = context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		_, err = Exec(ctx, s, b, CompiledOptions{})
		if cqlerr.KindOf(err) != cqlerr.KindCanceled {
			t.Fatalf("cancel: %v (%v)", err, cqlerr.KindOf(err))
		}
		m := s.Metrics()
		if m.Errors != 3 || m.Timeouts != 1 || m.Canceled != 2 {
			t.Fatalf("Errors %d, Timeouts %d, Canceled %d; want 3, 1, 2", m.Errors, m.Timeouts, m.Canceled)
		}
		if len(m.ErrorsByKind) != 2 || m.ErrorsByKind[cqlerr.KindTimeout] != 1 || m.ErrorsByKind[cqlerr.KindCanceled] != 2 {
			t.Fatalf("ErrorsByKind = %v", m.ErrorsByKind)
		}
	})

	t.Run("Attempts", func(t *testing.T) {
		unavailable := &cqlerr.UnavailableError{}
		for _, tc := range []struct {
			err                error
			partial            bool
			canceled, timedOut bool
			status             cqlerr.ExecutionStatus
		}{
			{nil, false, false, false, cqlerr.Executed},
			{context.Canceled, false, true, false, cqlerr.NotExecuted},
			{context.DeadlineExceeded, false, true, false, cqlerr.NotExecuted},
			{cqlerr.ErrRequestTimeout, false, false, true, cqlerr.NotExecuted},
			{errors.New("other"), false, false, false, cqlerr.MaybeExecuted},
			{unavailable, false, false, false, cqlerr.NotExecuted},
			// An unlogged batch the server rejected may have applied some
			// partitions, as AttemptFailure reports.
			{unavailable, true, false, false, cqlerr.MaybeExecuted},
		} {
			event := routing2.AttemptEvent{Err: tc.err}
			attemptEnded(&event, tc.partial)
			if event.Canceled != tc.canceled || event.TimedOut != tc.timedOut || event.ExecutionStatus != tc.status {
				t.Errorf("%v: Canceled %t, TimedOut %t, ExecutionStatus %v", tc.err, event.Canceled, event.TimedOut, event.ExecutionStatus)
			}
		}
	})

	t.Run("ZeroSnapshot", func(t *testing.T) {
		rt := new(InternalSessionRuntime)
		if m := rt.metricsSnapshot(); m.ErrorsByKind != nil || m.Canceled != 0 || m.Timeouts != 0 {
			t.Fatalf("a runtime without errors: %+v", m)
		}
	})
}

// TestConnectionsMetric checks that Metrics counts the connections of every
// host's pool.
func TestConnectionsMetric(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 3)
	if got := s.Metrics().Connections; got != 3 {
		t.Fatalf("Connections = %d, want 3", got)
	}
	var zero *implCluster.Core
	if got := zero.Metrics().Connections; got != 0 {
		t.Fatalf("zero Connections = %d", got)
	}
}
