package session

import (
	"context"
	"sync"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
)

type execVoidObserver struct {
	mu        sync.Mutex
	requests  []settings.RequestEvent
	attempts  []routing.AttemptEvent
	onAttempt func(routing.AttemptEvent)
}

func (o *execVoidObserver) ObserveRequest(event settings.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, event)
}
func (o *execVoidObserver) ObserveAttempt(event routing.AttemptEvent) {
	o.mu.Lock()
	o.attempts = append(o.attempts, event)
	o.mu.Unlock()
	if o.onAttempt != nil {
		o.onAttempt(event)
	}
}

func (o *execVoidObserver) StartRequest(ctx context.Context, _ settings.RequestStart) (context.Context, func(settings.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o *execVoidObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return o.ObserveAttempt
}
