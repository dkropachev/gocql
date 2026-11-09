// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"sync/atomic"
)

type InternalExecutionWaiter interface{ Wait() }
type InternalExecutionLifecycle interface {
	Trace(context.Context, UUID) (TraceInfo, error)
	StopPrepared() InternalExecutionWaiter
	StopActive() InternalExecutionWaiter
	MetricsSnapshot() MetricsSnapshot
	Profile() *Profile
	ConfigSnapshot() Config
}

func InternalCurrentExecution(s *Core) InternalExecutionLifecycle {
	s.privateStorage.InternalTypedMu.Lock()
	defer s.privateStorage.InternalTypedMu.Unlock()
	return s.privateStorage.InternalTypedState
}
func InternalSessionSchemaTracker(s *Core) *preparedSchemaTracker {
	s.privateStorage.schemaMu.Lock()
	defer s.privateStorage.schemaMu.Unlock()
	if s.privateStorage.schema == nil {
		s.privateStorage.schema = &preparedSchemaTracker{InternalEpochs: make(map[preparedDependencyKey]*atomic.Uint64)}
	}
	return s.privateStorage.schema
}
