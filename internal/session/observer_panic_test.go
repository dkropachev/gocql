// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"encoding/binary"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type panicCtxKey struct{}

// panicLog records, for each "observer panicked" record, the panic value
// and the panicCtxKey value of the context it was logged with.
type panicLog struct {
	mu      sync.Mutex
	records []string
}

func (h *panicLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *panicLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *panicLog) WithGroup(string) slog.Handler            { return h }
func (h *panicLog) Handle(ctx context.Context, r slog.Record) error {
	if r.Message != "observer panicked" {
		return nil
	}
	var value string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "panic" {
			value = a.Value.String()
		}
		return true
	})
	label, _ := ctx.Value(panicCtxKey{}).(string)
	h.mu.Lock()
	h.records = append(h.records, value+" in "+label)
	h.mu.Unlock()
	return nil
}

func (h *panicLog) take() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	records := h.records
	h.records = nil
	return records
}

// TestObserverPanicsLogged checks that a panic in any hook, joined or not,
// is logged once with the request's context: the one the Request hooks
// returned, or the caller's without them. A panic Join recovers in a start
// hook is logged when the driver ends what that hook started, which it does
// for every attempt, a speculative one that loses after its request ended
// included.
func TestObserverPanicsLogged(t *testing.T) {
	label := func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return context.WithValue(ctx, panicCtxKey{}, "request"), func(observe.RequestEvent) {}
	}
	okRequest := func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(observe.RequestEvent) {}
	}
	panicRequest := func(context.Context, observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		panic("request start")
	}
	panicRequestEnd := func(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(observe.RequestEvent) { panic("request end") }
	}
	okAttempt := func(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) {
		return func(routing2.AttemptEvent) {}
	}
	panicAttempt := func(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) { panic("attempt start") }
	panicAttemptEnd := func(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) {
		return func(routing2.AttemptEvent) { panic("attempt end") }
	}

	for _, c := range []struct {
		name          string
		session, call observe.Hooks
		want          string
	}{
		{"attempt start alone", observe.Hooks{Attempt: panicAttempt}, observe.Hooks{}, "attempt start in caller"},
		{"joined attempt start without Request", observe.Hooks{Attempt: panicAttempt}, observe.Hooks{Attempt: okAttempt}, "attempt start in caller"},
		{"joined attempt end without Request", observe.Hooks{Attempt: panicAttemptEnd}, observe.Hooks{Attempt: okAttempt}, "attempt end in caller"},
		{"joined attempt start", observe.Hooks{Request: label, Attempt: panicAttempt}, observe.Hooks{Attempt: okAttempt}, "attempt start in request"},
		{"request start alone", observe.Hooks{Request: panicRequest}, observe.Hooks{}, "request start in caller"},
		{"joined request start", observe.Hooks{Request: label}, observe.Hooks{Request: panicRequest, Attempt: okAttempt}, "request start in request"},
		{"joined first request start", observe.Hooks{Request: panicRequest}, observe.Hooks{Request: okRequest}, "request start in caller"},
		{"joined request end", observe.Hooks{Request: label}, observe.Hooks{Request: panicRequestEnd}, "request end in request"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _, peers := runtimeTestSession(t, 1)
			logs := new(panicLog)
			implCluster.SessionData(s).InternalLogger = slog.New(logs)
			void := binary.BigEndian.AppendUint32(nil, 1)
			go serveLoopback(peers[0], func([]byte) []byte { return void }, nil)
			rt := InternalRuntimeFor(s)
			rt.config.Observer = c.session
			if !c.session.IsZero() {
				rt.observerHooks = &c.session
			}
			options, err := NewCompiledOptions(s, Options{Observer: c.call})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), panicCtxKey{}, "caller")
			b := runtimeQuery(t)
			if _, err := Exec(ctx, s, b, options); err != nil {
				t.Fatal(err)
			}
			b.Close()
			if got := logs.take(); !slices.Equal(got, []string{c.want}) {
				t.Errorf("logged %q, want %q", got, c.want)
			}
		})
	}

	t.Run("speculative loser", func(t *testing.T) {
		s, conns, peers := runtimeTestSession(t, 2)
		logs := new(panicLog)
		implCluster.SessionData(s).InternalLogger = slog.New(logs)
		policy := &runtimePolicy{targets: []metadata.Target{{HostID: implEndpoint.InternalHostInfoHostUUID(conns[0].InternalHost)}, {HostID: implEndpoint.InternalHostInfoHostUUID(conns[1].InternalHost)}}}
		profile := settings.InternalDefaultProfile()
		profile.Routing = policy
		profile.Speculation = runtimeSpeculation{}
		// The first attempt's start panics; it loses to the speculative one
		// and ends only after its request has.
		panicFirst := func(_ context.Context, start routing2.AttemptStart) func(routing2.AttemptEvent) {
			if start.Attempt == 1 {
				panic("attempt start")
			}
			return func(routing2.AttemptEvent) {}
		}
		hooks := observe.Join(observe.Hooks{Request: label, Attempt: panicFirst}, observe.Hooks{Attempt: okAttempt})
		go func() {
			stream := typedTestReadRequest(t, conns[1], peers[1])
			_, _ = peers[1].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "winner")))
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bound := runtimeQuery(t)
		page, err := RunPage(ctx, s, bound, cql.StartPage(), 0, testOptions(Options{Profile: new(profile), Idempotent: cql.Some(true), Observer: hooks}))
		if err != nil {
			t.Fatal(err)
		}
		bound.Close()
		page.Close()
		// The winner may cancel the first attempt before its writer sends
		// it, so answer it only if it arrives.
		go func() {
			if _, stream, _, err := readRequest(peers[0]); err == nil {
				_, _ = peers[0].Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "loser")))
			}
		}()
		if err := bound.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		if got, want := logs.take(), []string{"attempt start in request"}; !slices.Equal(got, want) {
			t.Errorf("logged %q, want %q", got, want)
		}
	})
}
