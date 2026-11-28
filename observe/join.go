// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"context"

	"github.com/scylladb/gocql/v2/routing"
)

// Join returns Hooks that call every hooks' functions, in the order given.
// Each field of the result calls the same field of every argument that sets
// it, is that function when one argument sets it, and is nil when none does.
//
// The joined Request passes each Request the context the one before it
// returned, and Attempt gets the context the last one returned. A panic in
// one hook does not keep the others from being called: Join raises the
// first panic again once they have been, for the driver to log. A panic in
// Request or Attempt is raised from the end function they return, so every
// hook still sees its request or attempt end, and the driver logs it as that
// request or attempt ends. That end function is returned even when no hook
// returned one, for the panic to be raised.
func Join(hooks ...Hooks) Hooks {
	var requests []func(context.Context, RequestStart) (context.Context, func(RequestEvent))
	var attempts []func(context.Context, routing.AttemptStart) func(routing.AttemptEvent)
	for _, h := range hooks {
		if h.Request != nil {
			requests = append(requests, h.Request)
		}
		if h.Attempt != nil {
			attempts = append(attempts, h.Attempt)
		}
	}
	var joined Hooks
	switch len(requests) {
	case 0:
	case 1:
		joined.Request = requests[0]
	default:
		joined.Request = func(ctx context.Context, start RequestStart) (context.Context, func(RequestEvent)) {
			return startRequest(requests, ctx, start)
		}
	}
	switch len(attempts) {
	case 0:
	case 1:
		joined.Attempt = attempts[0]
	default:
		joined.Attempt = func(ctx context.Context, start routing.AttemptStart) func(routing.AttemptEvent) {
			return startAttempt(attempts, ctx, start)
		}
	}
	return joined
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

func startRequest(requests []func(context.Context, RequestStart) (context.Context, func(RequestEvent)), ctx context.Context, start RequestStart) (context.Context, func(RequestEvent)) {
	var started hookPanic
	ends := make([]func(RequestEvent), 0, len(requests))
	for _, r := range requests {
		started.call(func() {
			next, end := r(ctx, start)
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

func startAttempt(attempts []func(context.Context, routing.AttemptStart) func(routing.AttemptEvent), ctx context.Context, start routing.AttemptStart) func(routing.AttemptEvent) {
	var started hookPanic
	ends := make([]func(routing.AttemptEvent), 0, len(attempts))
	for _, a := range attempts {
		started.call(func() {
			if end := a(ctx, start); end != nil {
				ends = append(ends, end)
			}
		})
	}
	if len(ends) == 0 && !started.set {
		return nil
	}
	return func(e routing.AttemptEvent) {
		ended := started
		for _, end := range ends {
			ended.call(func() { end(e) })
		}
		ended.raise()
	}
}
