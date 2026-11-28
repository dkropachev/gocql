// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

type ctxKey string

// requestHook and attemptHook append what the hooks named name see to *log.
func requestHook(name string, log *[]string) observe.Hooks {
	return observe.Hooks{Request: func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		*log = append(*log, fmt.Sprintf("%s start %d parent=%v", name, start.ID, ctx.Value(ctxKey("span"))))
		id := start.ID
		return context.WithValue(ctx, ctxKey("span"), name), func(observe.RequestEvent) {
			*log = append(*log, fmt.Sprintf("%s end %d", name, id))
		}
	}}
}

func attemptHook(name string, log *[]string) observe.Hooks {
	return observe.Hooks{Attempt: func(ctx context.Context, start routing.AttemptStart) func(routing.AttemptEvent) {
		*log = append(*log, fmt.Sprintf("%s attempt %d parent=%v", name, start.Attempt, ctx.Value(ctxKey("span"))))
		return func(e routing.AttemptEvent) { *log = append(*log, fmt.Sprintf("%s attempt end %d", name, e.Attempt)) }
	}}
}

func both(name string, log *[]string) observe.Hooks {
	return observe.Hooks{Request: requestHook(name, log).Request, Attempt: attemptHook(name, log).Attempt}
}

func TestJoin(t *testing.T) {
	t.Run("Union", func(t *testing.T) {
		var log []string
		o := observe.Join(requestHook("a", &log), observe.Hooks{}, attemptHook("b", &log), both("c", &log))
		if o.Request == nil || o.Attempt == nil {
			t.Fatal("joined hooks lack a hook")
		}
		ctx, endRequest := o.Request(context.Background(), observe.RequestStart{ID: 7})
		endAttempt := o.Attempt(ctx, routing.AttemptStart{Attempt: 1})
		endAttempt(routing.AttemptEvent{Attempt: 1})
		endRequest(observe.RequestEvent{})
		want := []string{
			"a start 7 parent=<nil>",
			"c start 7 parent=a",
			"b attempt 1 parent=c",
			"c attempt 1 parent=c",
			"b attempt end 1",
			"c attempt end 1",
			"a end 7",
			"c end 7",
		}
		if !slices.Equal(log, want) {
			t.Fatalf("calls\n%q\nwant\n%q", log, want)
		}
	})

	t.Run("OnlyRequest", func(t *testing.T) {
		var log []string
		o := observe.Join(requestHook("a", &log), requestHook("b", &log))
		if o.Attempt != nil || o.Request == nil {
			t.Fatal("a join of request hooks has an attempt hook or no request hook")
		}
		_, end := o.Request(context.Background(), observe.RequestStart{ID: 3})
		end(observe.RequestEvent{})
		want := []string{"a start 3 parent=<nil>", "b start 3 parent=a", "a end 3", "b end 3"}
		if !slices.Equal(log, want) {
			t.Fatalf("calls\n%q\nwant\n%q", log, want)
		}
	})

	t.Run("OnlyAttempt", func(t *testing.T) {
		var log []string
		o := observe.Join(attemptHook("a", &log), attemptHook("b", &log))
		if o.Request != nil || o.Attempt == nil {
			t.Fatal("a join of attempt hooks has a request hook or no attempt hook")
		}
		end := o.Attempt(context.Background(), routing.AttemptStart{Attempt: 2})
		end(routing.AttemptEvent{Attempt: 2})
		want := []string{"a attempt 2 parent=<nil>", "b attempt 2 parent=<nil>", "a attempt end 2", "b attempt end 2"}
		if !slices.Equal(log, want) {
			t.Fatalf("calls\n%q\nwant\n%q", log, want)
		}
	})

	t.Run("Degenerate", func(t *testing.T) {
		if !observe.Join().IsZero() || !observe.Join(observe.Hooks{}, observe.Hooks{}).IsZero() {
			t.Fatal("a join of no hooks has a hook")
		}
		// One hook is kept as it is, without a wrapper.
		var log []string
		calls := 0
		single := observe.Hooks{Request: func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
			calls++
			return requestHook("a", &log).Request(ctx, start)
		}}
		o := observe.Join(observe.Hooks{}, single, attemptHook("b", &log))
		if o.Request == nil || o.Attempt == nil {
			t.Fatal("Join lost a hook")
		}
		o.Request(context.Background(), observe.RequestStart{ID: 1})
		if calls != 1 || len(log) != 1 {
			t.Fatalf("single request hook: %d calls, log %q", calls, log)
		}
	})

	t.Run("Panic", func(t *testing.T) {
		var log []string
		bad := observe.Hooks{Request: func(context.Context, observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
			panic("start")
		}}
		badEnd := observe.Hooks{Attempt: func(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
			return func(routing.AttemptEvent) { panic("end") }
		}}
		o := observe.Join(bad, requestHook("a", &log), badEnd, attemptHook("b", &log))
		// A panic in a start hook still starts the others and is raised
		// from the end function, after every end ran.
		_, end := o.Request(context.Background(), observe.RequestStart{ID: 1})
		if end == nil {
			t.Fatal("no end function after a panicking start")
		}
		if v := recovered(func() { end(observe.RequestEvent{}) }); v != "start" {
			t.Fatalf("end raised %v, want the start panic", v)
		}
		// A panic in an end function still ends the others.
		endAttempt := o.Attempt(context.Background(), routing.AttemptStart{Attempt: 2})
		if v := recovered(func() { endAttempt(routing.AttemptEvent{Attempt: 2}) }); v != "end" {
			t.Fatalf("attempt end raised %v, want the end panic", v)
		}
		want := []string{"a start 1 parent=<nil>", "a end 1", "b attempt 2 parent=<nil>", "b attempt end 2"}
		if !slices.Equal(log, want) {
			t.Fatalf("calls\n%q\nwant\n%q", log, want)
		}
	})
}

func recovered(fn func()) (v any) {
	defer func() { v = recover() }()
	fn()
	return nil
}
