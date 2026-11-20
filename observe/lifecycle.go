// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

// LifecycleObserver receives the session's and its connections' lifecycle
// events: connecting, startup, authentication, closing and their failures.
type LifecycleObserver interface {
	// ObserveLifecycle receives one event. It must not block.
	ObserveLifecycle(LifecycleEvent)
}

// LifecycleEvent is one step of a session's or connection's life: Kind names
// what happened, Stage how far it got, and Err why it failed, if it did.
//
// The driver fills it; its zero value is empty.
type LifecycleEvent struct {
	// At is when the step happened.
	At time.Time
	// Err is why the step failed, or nil.
	Err error
	// Kind names what the event concerns, such as "session" or "host", and
	// Stage the step, such as "ready", "connected", "down" or "closed".
	Kind, Stage string
	// Stream is the stream ID of the request the event concerns, or zero.
	Stream int16
	// HostID is the host the event concerns, or the zero UUID for a session
	// event.
	HostID cql.UUID
}
