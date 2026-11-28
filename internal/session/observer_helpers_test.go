// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/routing"
)

// hooksOf returns the observe.Hooks of a test observer: its StartRequest
// and StartAttempt methods, each when it has one.
func hooksOf(o any) observe.Hooks {
	var h observe.Hooks
	if r, ok := o.(interface {
		StartRequest(context.Context, observe.RequestStart) (context.Context, func(observe.RequestEvent))
	}); ok {
		h.Request = r.StartRequest
	}
	if a, ok := o.(interface {
		StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent)
	}); ok {
		h.Attempt = a.StartAttempt
	}
	return h
}

// hooksPtr returns the hooks of o as an execution or session holds them,
// nil when o has none.
func hooksPtr(o any) *observe.Hooks {
	h := hooksOf(o)
	if h.IsZero() {
		return nil
	}
	return &h
}
