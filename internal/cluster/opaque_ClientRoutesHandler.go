// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/events"

	"github.com/scylladb/gocql/v2/internal/eventbus"
)

type ClientRoutesHandlerStorage struct {
	InternalLog           StdLogger
	InternalC             InternalControlConnection
	InternalResolver      DNSResolver
	InternalSession       *Session
	connectionEpoch       map[string]uint64
	pairEpoch             map[InternalPair]uint64
	InternalSub           *eventbus.Subscriber[events.Event]
	InternalAddrOverrides map[string]string // connectionID → user-supplied ConnectionAddr
	InternalUpdateTasks   chan InternalUpdateTask
	InternalCloseChan     chan struct{}
	InternalRouteCache    InternalClientRouteCache
	InternalCfg           ClientRoutesConfig
	InternalWorkers       sync.WaitGroup
	stopOnce              sync.Once
	queueMu               sync.Mutex
	needsResync           atomic.Bool
	InternalInitialized   atomic.Bool
	InternalPickTLSPorts  bool
}

func ClientRoutesHandlerData(v *ClientRoutesHandler) *ClientRoutesHandlerStorage {
	return &v.privateStorage
}
