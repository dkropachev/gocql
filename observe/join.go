// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"context"

	routing2 "github.com/scylladb/gocql/v2/routing"
)

// RequestObserverFunc is a function used as a RequestObserver: its
// StartRequest calls f. A nil RequestObserverFunc panics when called.
type RequestObserverFunc func(context.Context, RequestStart) (context.Context, func(RequestEvent))

// StartRequest calls f(ctx, start).
func (f RequestObserverFunc) StartRequest(ctx context.Context, start RequestStart) (context.Context, func(RequestEvent)) {
	return f(ctx, start)
}

// AttemptObserverFunc is a function used as an AttemptObserver: its
// StartAttempt calls f. A nil AttemptObserverFunc panics when called.
type AttemptObserverFunc func(context.Context, AttemptStart) func(routing2.AttemptEvent)

// StartAttempt calls f(ctx, start).
func (f AttemptObserverFunc) StartAttempt(ctx context.Context, start AttemptStart) func(routing2.AttemptEvent) {
	return f(ctx, start)
}

// Join returns an Observer that calls every observer's hooks, in the order
// given. It implements RequestObserver when any of them does, and
// AttemptObserver when any of them does; each hook calls only the observers
// that implement it.
//
// Nil observers are skipped. Join returns nil when none is left, and the
// observer itself when one is. When an observer implements no hook, Join
// returns that observer, so config.ConfigBuilder.Build and
// gocql.Session.CompileOptions reject it by its own type.
//
// The joined StartRequest passes each observer the context the one before it
// returned, and StartAttempt gets the context the last one returned. A panic
// in one observer's hook does not keep the others from being called: Join
// raises the first panic again once they have been, for the driver to log.
// A panic in StartRequest or StartAttempt is raised from the end function
// they return, so every observer still sees its request or attempt end.
func Join(observers ...Observer) Observer {
	var j joined
	var last Observer
	n := 0
	for _, o := range observers {
		if o == nil {
			continue
		}
		r, isRequest := o.(RequestObserver)
		a, isAttempt := o.(AttemptObserver)
		if !isRequest && !isAttempt {
			return o
		}
		if isRequest {
			j.requests = append(j.requests, r)
		}
		if isAttempt {
			j.attempts = append(j.attempts, a)
		}
		last = o
		n++
	}
	switch {
	case n <= 1:
		return last
	case j.requests != nil && j.attempts != nil:
		return &joinedBoth{j}
	case j.requests != nil:
		return &joinedRequest{j}
	default:
		return &joinedAttempt{j}
	}
}

// joined holds the hooks of the observers Join combined, in order.
type joined struct {
	requests []RequestObserver
	attempts []AttemptObserver
}

type joinedRequest struct{ j joined }

func (o *joinedRequest) StartRequest(ctx context.Context, start RequestStart) (context.Context, func(RequestEvent)) {
	return o.j.startRequest(ctx, start)
}

type joinedAttempt struct{ j joined }

func (o *joinedAttempt) StartAttempt(ctx context.Context, start AttemptStart) func(routing2.AttemptEvent) {
	return o.j.startAttempt(ctx, start)
}

type joinedBoth struct{ j joined }

func (o *joinedBoth) StartRequest(ctx context.Context, start RequestStart) (context.Context, func(RequestEvent)) {
	return o.j.startRequest(ctx, start)
}

func (o *joinedBoth) StartAttempt(ctx context.Context, start AttemptStart) func(routing2.AttemptEvent) {
	return o.j.startAttempt(ctx, start)
}

// hookPanic records the first panic of the hooks it guards.
type hookPanic struct {
	value any
	set   bool
}

// call calls fn, recording its panic, if it is the first.
func (p *hookPanic) call(fn func()) {
	defer func() {
		if v := recover(); v != nil && !p.set {
			p.value, p.set = v, true
		}
	}()
	fn()
}

// raise panics with the recorded panic, if any.
func (p *hookPanic) raise() {
	if p.set {
		panic(p.value)
	}
}

func (j *joined) startRequest(ctx context.Context, start RequestStart) (context.Context, func(RequestEvent)) {
	var started hookPanic
	ends := make([]func(RequestEvent), 0, len(j.requests))
	for _, r := range j.requests {
		started.call(func() {
			next, end := r.StartRequest(ctx, start)
			if next != nil {
				ctx = next
			}
			if end != nil {
				ends = append(ends, end)
			}
		})
	}
	if len(ends) == 0 && !started.set {
		return ctx, nil
	}
	return ctx, func(e RequestEvent) {
		ended := started
		for _, end := range ends {
			ended.call(func() { end(e) })
		}
		ended.raise()
	}
}

func (j *joined) startAttempt(ctx context.Context, start AttemptStart) func(routing2.AttemptEvent) {
	var started hookPanic
	ends := make([]func(routing2.AttemptEvent), 0, len(j.attempts))
	for _, a := range j.attempts {
		started.call(func() {
			if end := a.StartAttempt(ctx, start); end != nil {
				ends = append(ends, end)
			}
		})
	}
	if len(ends) == 0 && !started.set {
		return nil
	}
	return func(e routing2.AttemptEvent) {
		ended := started
		for _, end := range ends {
			ended.call(func() { end(e) })
		}
		ended.raise()
	}
}
