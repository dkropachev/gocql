package cluster

import (
	"github.com/scylladb/gocql/v2/observe"
)

type panicLifecycle struct{}

func (panicLifecycle) ObserveLifecycle(observe.LifecycleEvent) { panic("observer failure") }
