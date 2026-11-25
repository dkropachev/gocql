// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cqlerr"
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

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := Exec(ctx, s, b, CompiledOptions{})
		cancel()
		if cqlerr.KindOf(err) != cqlerr.KindTimeout {
			t.Fatalf("deadline: %v (%v)", err, cqlerr.KindOf(err))
		}
		ctx, cancel = context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		_, err = Exec(ctx, s, b, CompiledOptions{})
		if cqlerr.KindOf(err) != cqlerr.KindCanceled {
			t.Fatalf("cancel: %v (%v)", err, cqlerr.KindOf(err))
		}
		m := s.Metrics()
		if m.Errors != 2 || m.Timeouts != 1 || m.Canceled != 1 {
			t.Fatalf("Errors %d, Timeouts %d, Canceled %d; want 2, 1, 1", m.Errors, m.Timeouts, m.Canceled)
		}
		if len(m.ErrorsByKind) != 2 || m.ErrorsByKind[cqlerr.KindTimeout] != 1 || m.ErrorsByKind[cqlerr.KindCanceled] != 1 {
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
			{context.DeadlineExceeded, false, false, true, cqlerr.NotExecuted},
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
