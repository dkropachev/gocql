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

// requestHook and attemptHook append what the hooks of observer name see
// to *log.
func requestHook(name string, log *[]string) observe.RequestObserverFunc {
	return func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		*log = append(*log, fmt.Sprintf("%s start %d parent=%v", name, start.ID, ctx.Value(ctxKey("span"))))
		return context.WithValue(ctx, ctxKey("span"), name), func(e observe.RequestEvent) {
			*log = append(*log, fmt.Sprintf("%s end %d", name, e.ID))
		}
	}
}

func attemptHook(name string, log *[]string) observe.AttemptObserverFunc {
	return func(ctx context.Context, start observe.AttemptStart) func(routing.AttemptEvent) {
		*log = append(*log, fmt.Sprintf("%s attempt %d parent=%v", name, start.Attempt, ctx.Value(ctxKey("span"))))
		return func(e routing.AttemptEvent) { *log = append(*log, fmt.Sprintf("%s attempt end %d", name, e.Attempt)) }
	}
}

type both struct {
	observe.RequestObserverFunc
	observe.AttemptObserverFunc
}

func TestJoin(t *testing.T) {
	t.Run("Union", func(t *testing.T) {
		var log []string
		o := observe.Join(requestHook("a", &log), nil, attemptHook("b", &log), both{requestHook("c", &log), attemptHook("c", &log)})
		r, ok := o.(observe.RequestObserver)
		if !ok {
			t.Fatal("joined observer is no RequestObserver")
		}
		a, ok := o.(observe.AttemptObserver)
		if !ok {
			t.Fatal("joined observer is no AttemptObserver")
		}
		ctx, endRequest := r.StartRequest(context.Background(), observe.RequestStart{ID: 7})
		endAttempt := a.StartAttempt(ctx, observe.AttemptStart{Attempt: 1})
		endAttempt(routing.AttemptEvent{Attempt: 1})
		endRequest(observe.RequestEvent{ID: 7})
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
		if _, ok := o.(observe.AttemptObserver); ok {
			t.Fatal("a join of request observers is an AttemptObserver")
		}
		if _, ok := o.(observe.RequestObserver); !ok {
			t.Fatal("a join of request observers is no RequestObserver")
		}
	})

	t.Run("Degenerate", func(t *testing.T) {
		if o := observe.Join(); o != nil {
			t.Fatalf("Join() = %v, want nil", o)
		}
		if o := observe.Join(nil, nil); o != nil {
			t.Fatalf("Join(nil, nil) = %v, want nil", o)
		}
		var log []string
		single := requestHook("a", &log)
		if o, ok := observe.Join(nil, single).(observe.RequestObserverFunc); !ok || o == nil {
			t.Fatalf("Join of one observer = %T, want the observer", o)
		}
		// An observer with no hook is returned as is, for the driver to
		// reject by its own type.
		if o := observe.Join(single, 42); o != 42 {
			t.Fatalf("Join with a hookless observer = %v, want it", o)
		}
	})

	t.Run("Panic", func(t *testing.T) {
		var log []string
		bad := observe.RequestObserverFunc(func(context.Context, observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
			panic("start")
		})
		badEnd := observe.AttemptObserverFunc(func(context.Context, observe.AttemptStart) func(routing.AttemptEvent) {
			return func(routing.AttemptEvent) { panic("end") }
		})
		o := observe.Join(bad, requestHook("a", &log), badEnd, attemptHook("b", &log))
		// A panic in a start hook still starts the others and is raised
		// from the end function, after every end ran.
		_, end := o.(observe.RequestObserver).StartRequest(context.Background(), observe.RequestStart{ID: 1})
		if end == nil {
			t.Fatal("no end function after a panicking start")
		}
		if v := recovered(func() { end(observe.RequestEvent{ID: 1}) }); v != "start" {
			t.Fatalf("end raised %v, want the start panic", v)
		}
		// A panic in an end function still ends the others.
		endAttempt := o.(observe.AttemptObserver).StartAttempt(context.Background(), observe.AttemptStart{Attempt: 2})
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
