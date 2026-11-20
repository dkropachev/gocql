// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

// sameHostSafe is retry.Safe on a one-host session: it records each attempt
// and retries in place where Safe would move to the next host.
type sameHostSafe struct {
	mu       sync.Mutex
	attempts []retry.Attempt
}

func (p *sameHostSafe) Decide(a retry.Attempt) retry.Decision {
	p.mu.Lock()
	p.attempts = append(p.attempts, a)
	p.mu.Unlock()
	d := (retry.Safe{}).Decide(a)
	if d.Action == retry.Next {
		d.Action = retry.Same
	}
	return d
}

// TestBatchRejectedAfterPartialApply checks that an unlogged or counter
// batch rejected as Unavailable or Overloaded reports MaybeExecuted and that
// retry.Safe does not replay it, while a logged batch is replayed.
func TestBatchRejectedAfterPartialApply(t *testing.T) {
	for _, kind := range []cql.BatchKind{cql.LoggedBatch, cql.UnloggedBatch, cql.CounterBatch} {
		for _, code := range []cqlerr.ErrorCode{cqlerr.CodeUnavailable, cqlerr.CodeOverloaded} {
			for _, general := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/%v/general=%t", kind, code, general), func(t *testing.T) {
					testBatchRejected(t, kind, code, general)
				})
			}
		}
	}
}

func testBatchRejected(t *testing.T, kind cql.BatchKind, code cqlerr.ErrorCode, general bool) {
	s, conns, peers := runtimeTestSession(t, 1)
	defer s.Close()
	partial := kind != cql.LoggedBatch
	q := batchTestQuery(t, s)
	batch := Batch{Kind: kind}
	for range 2 {
		if err := q.AddToBatch(&batch, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		stream := typedTestReadRequest(t, conns[0], peers[0])
		var body []byte
		if code == cqlerr.CodeUnavailable {
			body = unavailableBody()
		}
		if _, err := peers[0].Write(serverError(t, stream, code, body)); err != nil || partial {
			return
		}
		stream = typedTestReadRequest(t, conns[0], peers[0])
		_, _ = peers[0].Write(typedTestResponse(t, 4, stream, binary.BigEndian.AppendUint32(nil, frm.ResultKindVoid)))
	}()
	policy := &sameHostSafe{}
	profile := settings.InternalDefaultProfile()
	profile.Retry = policy
	if general {
		profile.Speculation = neverSpeculate{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := InternalWrapSession(s).ExecBatch(ctx, &batch, testOptions(Options{Profile: new(profile)}))
	if len(policy.attempts) == 0 {
		t.Fatalf("the retry policy saw no attempt: %v", err)
	}
	a := policy.attempts[0]
	if a.Batch != cql.Some(kind) {
		t.Errorf("Attempt.Batch = %+v, want %v", a.Batch, kind)
	}
	if !partial {
		if err != nil || len(policy.attempts) != 1 || a.ExecutionStatus != cqlerr.NotExecuted {
			t.Fatalf("logged batch: %v after %d attempts, status %v; want a replay that succeeds", err, len(policy.attempts), a.ExecutionStatus)
		}
		return
	}
	if a.ExecutionStatus != cqlerr.MaybeExecuted || len(policy.attempts) != 1 {
		t.Errorf("attempts %d, status %v; want one attempt, MaybeExecuted", len(policy.attempts), a.ExecutionStatus)
	}
	failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
	if !ok || !errors.Is(err, code) || failure.ExecutionStatus() != cqlerr.MaybeExecuted || cqlerr.ExecutionStatusOf(err) != cqlerr.MaybeExecuted {
		t.Fatalf("ExecBatch = %v; want an ExecutionError of %v reporting MaybeExecuted", err, code)
	}
	if len(failure.Attempts) != 1 || failure.Attempts[0].Status != cqlerr.MaybeExecuted {
		t.Errorf("attempts %+v; want one, MaybeExecuted", failure.Attempts)
	}
}
