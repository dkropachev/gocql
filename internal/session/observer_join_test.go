// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"slices"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

// orderLog records the hooks observers call, in order.
type orderLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *orderLog) add(call string) {
	l.mu.Lock()
	l.calls = append(l.calls, call)
	l.mu.Unlock()
}

func (l *orderLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	calls := l.calls
	l.calls = nil
	return calls
}

// orderObserver logs its request and attempt hooks under name, and checks
// that its StartRequest sees the context of the observer before it.
type orderObserver struct {
	name, after string
	log         *orderLog
}

type orderCtxKey struct{}

func (o *orderObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	if got, _ := ctx.Value(orderCtxKey{}).(string); got != o.after {
		o.log.add(o.name + " request after " + got)
	}
	o.log.add(o.name + " request")
	return context.WithValue(ctx, orderCtxKey{}, o.name), func(observe.RequestEvent) { o.log.add(o.name + " request end") }
}

func (o *orderObserver) StartAttempt(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) {
	o.log.add(o.name + " attempt")
	return func(routing2.AttemptEvent) { o.log.add(o.name + " attempt end") }
}

// TestObserverJoin checks that execopt.Options.Observer is joined after the
// session's observer, each hook calling the session's first and the
// per-call observer getting the session's request context, for options
// compiled for the session, for another session, and for a reusable query;
// that NoSessionObserver leaves the session's observer out, with or without
// a per-call one; and that Session.Observer returns the session's.
func TestObserverJoin(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	log := new(orderLog)
	sessionObserver := &orderObserver{name: "session", log: log}
	rt := InternalRuntimeFor(s)
	rt.config.Observer = hooksOf(sessionObserver)
	rt.observerHooks = hooksPtr(sessionObserver)
	if got := InternalWrapSession(s).Observer(); got.Request == nil || got.Attempt == nil {
		t.Fatalf("Session.Observer = %+v, want the configured hooks", got)
	}
	call := &orderObserver{name: "call", after: "session", log: log}
	alone := &orderObserver{name: "call", log: log}
	compile := func(v Options) CompiledOptions {
		t.Helper()
		o, err := NewCompiledOptions(s, v)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	joined := []string{"session request", "call request", "session attempt", "call attempt", "session attempt end", "call attempt end", "session request end", "call request end"}
	for _, c := range []struct {
		name    string
		options CompiledOptions
		want    []string
	}{
		{"none", CompiledOptions{}, []string{"session request", "session attempt", "session attempt end", "session request end"}},
		{"compiled", compile(Options{Observer: hooksOf(call)}), joined},
		{"unbound", testOptions(Options{Observer: hooksOf(call)}), joined},
		{"NoSessionObserver", compile(Options{Observer: hooksOf(alone), NoSessionObserver: true}), []string{"call request", "call attempt", "call attempt end", "call request end"}},
		{"NoSessionObserver unbound", testOptions(Options{Observer: hooksOf(alone), NoSessionObserver: true}), []string{"call request", "call attempt", "call attempt end", "call request end"}},
		{"NoSessionObserver alone", compile(Options{NoSessionObserver: true}), nil},
	} {
		b := runtimeQuery(t)
		if _, err := Exec(context.Background(), s, b, c.options); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		b.Close()
		if got := log.take(); !slices.Equal(got, c.want) {
			t.Errorf("%s: hooks %q, want %q", c.name, got, c.want)
		}
	}

	for name, options := range map[string]CompiledOptions{"compiled": compile(Options{Observer: hooksOf(call)}), "unbound": testOptions(Options{Observer: hooksOf(call)})} {
		statement := binding.Statement[cql.EmptyRow, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "UPDATE ks.tbl SET v = 1 WHERE k = 1"}
		q, err := newUnpreparedQuery(s, statement, options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.Exec(context.Background(), cql.EmptyRow{}); err != nil {
			t.Fatal(err)
		}
		q.Close()
		if got := log.take(); !slices.Equal(got, joined) {
			t.Errorf("reusable query, %s options: hooks %q, want %q", name, got, joined)
		}
	}
}

// TestInlineExecutionDropsContext checks that a reusable query's inline
// execution, kept for the query's next one, does not keep the caller's
// context or the Request hook's end function once it has finished.
func TestInlineExecutionDropsContext(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	void := binary.BigEndian.AppendUint32(nil, 1)
	go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
	hooks := observe.Hooks{Request: func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(observe.RequestEvent) {}
	}}
	options, err := NewCompiledOptions(s, Options{Observer: hooks})
	if err != nil {
		t.Fatal(err)
	}
	statement := binding.Statement[cql.EmptyRow, cql.EmptyRow]{Kind: cql.OrdinaryStatement, CQL: "UPDATE ks.tbl SET v = 1 WHERE k = 1"}
	q, err := newUnpreparedQuery(s, statement, options)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.WithValue(context.Background(), orderCtxKey{}, "caller")
	if _, err := q.Exec(ctx, cql.EmptyRow{}); err != nil {
		t.Fatal(err)
	}
	if l := &q.state.inlineLogical; l.observerCtx != nil || l.requestEnd != nil {
		t.Fatalf("finished inline execution keeps observerCtx %v, requestEnd set %t", l.observerCtx, l.requestEnd != nil)
	}
}
