// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metrics"
)

type InternalExecutionWaiter interface{ Wait() }
type executionLifecycle interface {
	Trace(context.Context, cqltype.UUID) (TraceInfo, error)
	StopPrepared() InternalExecutionWaiter
	StopActive() InternalExecutionWaiter
	MetricsSnapshot() metrics.Snapshot
	Profile() *settings.Profile
	ConfigSnapshot() settings.Config
}

func (s *Core) CurrentExecution() executionLifecycle {
	s.privateStorage.InternalTypedMu.Lock()
	defer s.privateStorage.InternalTypedMu.Unlock()
	return s.privateStorage.InternalTypedState
}
func (s *Core) SchemaTracker() *schemafence.InternalPreparedSchemaTracker {
	s.privateStorage.schemaMu.Lock()
	defer s.privateStorage.schemaMu.Unlock()
	if s.privateStorage.schema == nil {
		s.privateStorage.schema = &schemafence.InternalPreparedSchemaTracker{InternalEpochs: make(map[schemafence.InternalPreparedDependencyKey]*atomic.Uint64)}
	}
	return s.privateStorage.schema
}
