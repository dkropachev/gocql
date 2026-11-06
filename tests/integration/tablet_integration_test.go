//go:build integration

package integration

import (
	"sync"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/routing"
)

type v2TabletAttemptObserver struct {
	mu      sync.Mutex
	targets []routing.Target
}

func (*v2TabletAttemptObserver) ObserveRequest(gocql.RequestEvent) {}
func (o *v2TabletAttemptObserver) ObserveAttempt(event gocql.AttemptEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if event.Err == nil {
		o.targets = append(o.targets, event.Target)
	}
}
func (o *v2TabletAttemptObserver) snapshot() []routing.Target {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]routing.Target(nil), o.targets...)
}
