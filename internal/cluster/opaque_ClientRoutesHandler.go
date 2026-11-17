// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"log/slog"
	"sync"
	"sync/atomic"

	endpoint2 "github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	"github.com/scylladb/gocql/v2/internal/events"
)

type ClientRoutesHandlerStorage struct {
	log                 *slog.Logger
	c                   InternalControlConnection
	resolver            endpoint2.Resolver
	InternalSession     *Core
	connectionEpoch     map[string]uint64
	pairEpoch           map[InternalPair]uint64
	sub                 *eventbus.Subscriber[events.Event]
	addrOverrides       map[string]string // connectionID → user-supplied ConnectionAddr
	InternalUpdateTasks chan InternalUpdateTask
	InternalCloseChan   chan struct{}
	InternalRouteCache  clientRouteCache
	cfg                 ClientRoutesConfig
	InternalWorkers     sync.WaitGroup
	stopOnce            sync.Once
	queueMu             sync.Mutex
	needsResync         atomic.Bool
	initialized         atomic.Bool
	pickTLSPorts        bool
}

func ClientRoutesHandlerData(v *ClientRoutesHandler) *ClientRoutesHandlerStorage {
	return &v.privateStorage
}
