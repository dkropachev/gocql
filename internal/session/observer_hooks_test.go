// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type observerCtxKey struct{}

// attemptHookOnly has only an attempt hook.
type attemptHookOnly struct {
	attempts atomic.Int64
	sawCtx   atomic.Bool
}

func (o *attemptHookOnly) StartAttempt(ctx context.Context, _ observe.AttemptStart) func(routing2.AttemptEvent) {
	o.attempts.Add(1)
	o.sawCtx.Store(ctx.Value(observerCtxKey{}) == "caller")
	return nil
}

// requestHookOnly has only a request hook.
type requestHookOnly struct{ requests, ended atomic.Int64 }

func (o *requestHookOnly) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	o.requests.Add(1)
	return ctx, func(observe.RequestEvent) { o.ended.Add(1) }
}

// TestObserverHooks checks that an observer gets only the hooks it
// implements, on the direct and the general path, that an attempt hook
// without a request hook receives the caller's context.
func TestObserverHooks(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	ctx := context.WithValue(context.Background(), observerCtxKey{}, "caller")

	attempts := new(attemptHookOnly)
	if _, err := Exec(ctx, s, b, testOptions(Options{Observer: hooksOf(attempts)})); err != nil {
		t.Fatal(err)
	}
	if attempts.attempts.Load() != 1 || !attempts.sawCtx.Load() {
		t.Fatalf("attempt hook: %d calls, caller's context %t", attempts.attempts.Load(), attempts.sawCtx.Load())
	}

	requests := new(requestHookOnly)
	if _, err := Exec(ctx, s, b, testOptions(Options{Observer: hooksOf(requests)})); err != nil {
		t.Fatal(err)
	}
	if requests.requests.Load() != 1 || requests.ended.Load() != 1 {
		t.Fatalf("request hook: %d starts, %d ends", requests.requests.Load(), requests.ended.Load())
	}

	// The general path, where attempts run on goroutines of their own.
	general := new(attemptHookOnly)
	if _, err := Exec(ctx, s, b, testOptions(Options{Observer: hooksOf(general), Profile: generalPathProfile()})); err != nil {
		t.Fatal(err)
	}
	if general.attempts.Load() != 1 || !general.sawCtx.Load() {
		t.Fatalf("general path attempt hook: %d calls, caller's context %t", general.attempts.Load(), general.sawCtx.Load())
	}

}

// TestAttemptFields checks that an attempt's start and end name its
// connection's address and its consistency on the direct and the general
// path, and that its end reports what it executed.
func TestAttemptFields(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	b := runtimeQuery(t)
	defer b.Close()
	for _, general := range []bool{false, true} {
		var start observe.AttemptStart
		var end routing2.AttemptEvent
		o := observe.Hooks{Attempt: func(_ context.Context, s observe.AttemptStart) func(routing2.AttemptEvent) {
			start = s
			return func(e routing2.AttemptEvent) { end = e }
		}}
		options := Options{Observer: o, Consistency: cql.Some(cql.LocalQuorum)}
		if general {
			options.Profile = generalPathProfile()
		}
		if _, err := Exec(context.Background(), s, b, testOptions(options)); err != nil {
			t.Fatal(err)
		}
		want := conns[0].Address()
		if start.Address != want || start.Consistency != cql.LocalQuorum {
			t.Errorf("general %t: start address %q, consistency %v", general, start.Address, start.Consistency)
		}
		if end.Address != want || end.Consistency != cql.LocalQuorum || end.ExecutionStatus != cqlerr.Executed {
			t.Errorf("general %t: end address %q, consistency %v, status %v", general, end.Address, end.Consistency, end.ExecutionStatus)
		}
	}
}
