// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
	"sync/atomic"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/events"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
)

type ClientRoutesHandlerStorage struct {
	log                 *slog.Logger
	c                   InternalControlConnection
	resolver            endpoint.DNSResolver
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
