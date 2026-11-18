// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type observerCtxKey struct{}

// attemptHookOnly implements only observe.AttemptObserver.
type attemptHookOnly struct {
	attempts atomic.Int64
	sawCtx   atomic.Bool
}

func (o *attemptHookOnly) StartAttempt(ctx context.Context, _ routing2.AttemptStart) func(routing2.AttemptEvent) {
	o.attempts.Add(1)
	o.sawCtx.Store(ctx.Value(observerCtxKey{}) == "caller")
	return nil
}

// requestHookOnly implements only observe.RequestObserver.
type requestHookOnly struct{ requests, ended atomic.Int64 }

func (o *requestHookOnly) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	o.requests.Add(1)
	return ctx, func(observe.RequestEvent) { o.ended.Add(1) }
}

// TestObserverHooks checks that an observer gets only the hooks it
// implements, and that an attempt hook without a request hook receives the
// caller's context.
func TestObserverHooks(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	ctx := context.WithValue(context.Background(), observerCtxKey{}, "caller")

	attempts := new(attemptHookOnly)
	if _, err := Exec(ctx, s, b, testOptions(Options{Observer: attempts})); err != nil {
		t.Fatal(err)
	}
	if attempts.attempts.Load() != 1 || !attempts.sawCtx.Load() {
		t.Fatalf("attempt hook: %d calls, caller's context %t", attempts.attempts.Load(), attempts.sawCtx.Load())
	}

	requests := new(requestHookOnly)
	if _, err := Exec(ctx, s, b, testOptions(Options{Observer: requests})); err != nil {
		t.Fatal(err)
	}
	if requests.requests.Load() != 1 || requests.ended.Load() != 1 {
		t.Fatalf("request hook: %d starts, %d ends", requests.requests.Load(), requests.ended.Load())
	}

	if _, err := Exec(ctx, s, b, testOptions(Options{Observer: struct{}{}})); err == nil {
		t.Fatal("Exec accepted an observer without hooks")
	}
}
