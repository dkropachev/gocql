// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

// LifecycleObserver receives the session's and its connections' lifecycle
// events: connecting, startup, authentication, closing and their failures.
// ObserveLifecycle must not block.
type LifecycleObserver interface {
	// ObserveLifecycle receives one event. It must not block.
	ObserveLifecycle(LifecycleEvent)
}

// LifecycleEvent is one step of a session's or connection's life: Kind names
// what happened, Stage how far it got, and Err why it failed, if it did.
//
// The driver fills it; its zero value is empty.
type LifecycleEvent struct {
	At          time.Time
	Err         error
	Kind, Stage string
	Stream      int16
	HostID      cql.UUID
}
