// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package observe

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

// LifecycleObserver receives the session's lifecycle events: the session
// becoming ready and closing, and its hosts connecting and going down.
type LifecycleObserver interface {
	// ObserveLifecycle receives one event. It must not block.
	ObserveLifecycle(LifecycleEvent)
}

// LifecycleKind is what a LifecycleEvent concerns. The zero LifecycleKind is
// empty and names nothing. Later releases may add values, so a switch over
// them needs a default case.
type LifecycleKind string

const (
	// LifecycleSession is an event of the session itself.
	LifecycleSession LifecycleKind = "session"
	// LifecycleHost is an event of one host, which LifecycleEvent.HostID
	// names.
	LifecycleHost LifecycleKind = "host"
)

// LifecycleStage is the step a LifecycleEvent reports. The zero
// LifecycleStage is empty and names no step. Later releases may add values,
// so a switch over them needs a default case.
type LifecycleStage string

const (
	// StageReady: the session finished connecting and serves requests.
	StageReady LifecycleStage = "ready"
	// StageConnected: the session's pool connected to a host.
	StageConnected LifecycleStage = "connected"
	// StageDown: the session marked a host down.
	StageDown LifecycleStage = "down"
	// StageClosed: the session closed.
	StageClosed LifecycleStage = "closed"
)

// LifecycleEvent is one step of a session's life: Kind names what it
// concerns and Stage the step.
//
// The driver fills it; its zero value is empty.
type LifecycleEvent struct {
	// At is when the step happened.
	At time.Time
	// Kind names what the event concerns.
	Kind LifecycleKind
	// Stage names the step.
	Stage LifecycleStage
	// HostID is the host of a LifecycleHost event, and the zero UUID for a
	// LifecycleSession event.
	HostID cql.UUID
}
