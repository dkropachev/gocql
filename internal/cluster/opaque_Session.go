/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"context"

	"sync"
	"sync/atomic"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/debounce"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/events"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/transport"

	"github.com/scylladb/gocql/v2/internal/eventbus"
)

type SessionStorage struct {
	InternalTypedState          executionLifecycle
	InternalTypedReady          atomic.Value // the *session.InternalSessionRuntime in InternalTypedState, once fully initialized; never replaced
	InternalControl             InternalControlConnection
	InternalCtx                 context.Context
	InternalLogger              *slog.Logger
	InternalPolicy              HostSelectionPolicy
	connectObserver             ConnectObserver
	frameObserver               transport.FrameHeaderObserver
	streamObserver              transport.StreamObserver
	initErr                     error
	InternalAddressTranslator   endpoint.AddressTranslator
	schema                      *schemafence.InternalPreparedSchemaTracker
	v2Peripheral                atomic.Pointer[InternalV2PeripheralState]
	InternalNodeEvents          *InternalEventDebouncer
	InternalHostSource          *InternalRingDescriber
	InternalPool                *pool.InternalPolicyConnPool
	InternalRingRefresher       *debounce.RefreshDebouncer
	readyCh                     chan struct{}
	InternalCancel              context.CancelFunc
	InternalSchemaEvents        *InternalEventDebouncer
	InternalMetadataDescriber   *InternalMetadataDescriber
	eventBus                    *eventbus.EventBus[events.Event]
	InternalConnCfg             *transport.ConnConfig
	InternalClientRoutesHandler *ClientRoutesHandler
	driverConfigReporter        *driverConfigReporter
	closeDone                   chan struct{}
	// id is a globally unique identifier for this session, reported to
	// the cluster via the SESSION_ID STARTUP option so that all connections
	// belonging to the same session can be correlated in system.clients.
	id                     string
	InternalCfg            InternalClusterSettings
	nextTypedHost          atomic.Uint64
	mu                     sync.RWMutex
	InternalSessionStateMu sync.RWMutex
	InternalTypedMu        sync.Mutex
	schemaMu               sync.Mutex
	// closedOrClosing mirrors InternalIsClosing||InternalIsClosed for lock-free
	// Closed; it is set under InternalSessionStateMu with either flag.
	closedOrClosing                   atomic.Bool
	InternalIsClosing                 bool
	InternalHasAggregatesAndFunctions bool
	InternalUseSystemSchema           bool
	tabletsRoutingV1                  bool
	isInitialized                     bool
	InternalIsClosed                  bool
}

func SessionData(v *Core) *SessionStorage { return &v.privateStorage }
