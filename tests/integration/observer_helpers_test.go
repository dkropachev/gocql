//go:build integration

package integration

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
