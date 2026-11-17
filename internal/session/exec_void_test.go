package session

import (
	"context"
	"sync"

	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type execVoidObserver struct {
	mu        sync.Mutex
	requests  []observe.RequestEvent
	attempts  []routing2.AttemptEvent
	onAttempt func(routing2.AttemptEvent)
}

func (o *execVoidObserver) ObserveRequest(event observe.RequestEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, event)
}
func (o *execVoidObserver) ObserveAttempt(event routing2.AttemptEvent) {
	o.mu.Lock()
	o.attempts = append(o.attempts, event)
	o.mu.Unlock()
	if o.onAttempt != nil {
		o.onAttempt(event)
	}
}

func (o *execVoidObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o *execVoidObserver) StartAttempt(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) {
	return o.ObserveAttempt
}
