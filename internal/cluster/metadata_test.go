package cluster

import (
	"github.com/scylladb/gocql/v2/internal/settings"
)

type panicLifecycle struct{}

func (panicLifecycle) ObserveLifecycle(settings.LifecycleEvent) { panic("observer failure") }
