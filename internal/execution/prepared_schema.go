// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"sync/atomic"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/schemafence"
)

func (rt *InternalSessionRuntime) InternalSchemaTracker() *schemafence.InternalPreparedSchemaTracker {
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	if rt.schema == nil {
		rt.schema = &schemafence.InternalPreparedSchemaTracker{InternalEpochs: make(map[schemafence.InternalPreparedDependencyKey]*atomic.Uint64)}
	}
	return rt.schema
}

func (s *InternalPreparedState) cacheCurrent() bool {
	s.InternalMu.RLock()
	defer s.InternalMu.RUnlock()
	return s.InternalInitialEpoch.InternalCurrent()
}
func (rt *InternalSessionRuntime) beginPreparedWorker(s *implCluster.Core) bool {
	rt.prepareWorkersMu.Lock()
	defer rt.prepareWorkersMu.Unlock()
	if rt.prepareWorkersClosed || s.Closed() {
		return false
	}
	rt.prepareWorkers.Add(1)
	return true
}

// Remote schema observations have no total order. Coalesce the need for an
// authoritative typed refresh instead of picking a winner by opaque server ID.
func (s *InternalPreparedState) noteMetadataConflict() error {
	if s.InternalSession == nil {
		return nil
	}
	if s.InternalSession.Closed() {
		return runtimeerrors.ErrClosed
	}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	return s.queueMetadataConflict()
}
func (s *InternalPreparedState) queueMetadataConflict() error {
	p := s.InternalSession.Peripheral()
	s.conflictDirty.Store(true)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaReconciled.Load() + 1
	_, pending := p.InternalPreparedRefreshScopes[s.InternalInfo.Keyspace]
	p.InternalMu.Unlock()
	if s.conflictEpoch.Load() == epoch && pending {
		return nil
	}
	if err := s.InternalSession.ScheduleV2PreparedRefresh(s.InternalInfo.Keyspace); err != nil {
		s.conflictEpoch.Store(0)
		return err
	}
	s.conflictEpoch.Store(epoch)
	s.conflictRefresh.Store(true)
	target := "TABLE"
	if s.InternalInfo.Table == "" {
		target = "KEYSPACE"
	}
	s.InternalSession.InvalidatePreparedSchema(implCluster.Event{Keyspace: s.InternalInfo.Keyspace, Object: s.InternalInfo.Table, Target: target})
	s.InternalSession.MarkV2SchemaStale(s.InternalInfo.Keyspace, false)
	return nil
}
func (s *InternalPreparedState) reconcilePendingConflict() error {
	if !s.conflictDirty.Load() || s.InternalSession == nil {
		return nil
	}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	p := s.InternalSession.Peripheral()
	p.InternalMu.Lock()
	_, pending := p.InternalPreparedRefreshScopes[s.InternalInfo.Keyspace]
	p.InternalMu.Unlock()
	if pending {
		return nil
	}
	if s.conflictEpoch.Load() != 0 {
		s.conflictDirty.Store(false)
		return nil
	}
	return s.queueMetadataConflict()
}
