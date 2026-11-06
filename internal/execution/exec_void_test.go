package execution

import (
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
