//go:build integration

package integration

import (
	"context"
	"sync"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

type v2TabletAttemptObserver struct {
	mu      sync.Mutex
	targets []metadata.Target
}

func (*v2TabletAttemptObserver) ObserveRequest(config.RequestEvent) {}
func (o *v2TabletAttemptObserver) ObserveAttempt(event routing.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if event.Err == nil {
		o.targets = append(o.targets, event.Target)
	}
}

func (o *v2TabletAttemptObserver) StartRequest(ctx context.Context, _ config.RequestStart) (context.Context, func(config.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o *v2TabletAttemptObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return o.ObserveAttempt
}
func (o *v2TabletAttemptObserver) snapshot() []metadata.Target {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]metadata.Target(nil), o.targets...)
}
