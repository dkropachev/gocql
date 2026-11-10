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
	"context"
	"sync"
	"sync/atomic"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// Each logical operation has a closed input owner of its own. Its final drain
// includes accepted writers and speculative/prefetch workers, independently of
// output page leases and other operations using the same binding.
type executionLifetime struct {
	stop          executionStopper
	cancel        context.CancelFunc
	done          chan struct{}
	mu            sync.Mutex
	inputReleased atomic.Bool
	stopping      bool
}

func (l *logicalExecution) releaseInput() {
	if l.lifetime.inputReleased.CompareAndSwap(false, true) {
		l.request.InternalOwner.InternalRelease()
	}
}

// executionStopper ends an execution on query or session shutdown. Result
// handles implement it directly, so registering one allocates nothing.
type executionStopper interface {
	stopExecution(*logicalExecution)
}

// stopFunc adapts a closure; func values are stored in interfaces directly.
type stopFunc func()

func (f stopFunc) stopExecution(*logicalExecution) { f() }

func (l *logicalExecution) installStop(stop executionStopper) {
	l.lifetime.mu.Lock()
	stopping := l.lifetime.stopping
	if !stopping {
		l.lifetime.stop = stop
	}
	l.lifetime.mu.Unlock()
	if stopping {
		stop.stopExecution(l)
	}
}
func (l *logicalExecution) shutdown() {
	l.lifetime.mu.Lock()
	l.lifetime.stopping = true
	stop := l.lifetime.stop
	cancel := l.lifetime.cancel
	l.lifetime.stop = nil
	l.lifetime.mu.Unlock()
	cancel()
	if stop != nil {
		stop.stopExecution(l)
	}
}
func (l *logicalExecution) cancelExecution() {
	l.lifetime.mu.Lock()
	cancel := l.lifetime.cancel
	l.lifetime.mu.Unlock()
	cancel()
}
func (s *InternalSessionRuntime) registerExecution(l *logicalExecution, parent *inputOwner) error {
	if query := l.request.query; query != nil {
		// Session.Close marks the session closing before StopActive, which
		// finds the execution through query.logical under query.mu. Checking
		// the mark under query.mu orders this registration either before
		// StopActive's stop of the query, which then cancels it, or after the
		// mark, which refuses it; no session lock is needed.
		query.mu.Lock()
		if query.closed || s.Core.Closed() {
			query.mu.Unlock()
			return runtimeerrors.ErrClosed
		}
		query.logical = l
		query.mu.Unlock()
		// The previous generation's owner drained before the query became idle.
		// Parent release can admit the next generation, so nothing after it
		// may dereference the preceding execution's reusable request.
		query.execOwner.resetExecution(query.releaseOwner)
		l.request.InternalOwner = &query.execOwner
		return nil
	}
	implCluster.SessionData(s.Core).InternalSessionStateMu.RLock()
	defer implCluster.SessionData(s.Core).InternalSessionStateMu.RUnlock()
	if implCluster.SessionData(s.Core).InternalIsClosing || implCluster.SessionData(s.Core).InternalIsClosed {
		return runtimeerrors.ErrClosed
	}
	rt := s
	rt.InternalMu.Lock()
	if rt.executions == nil {
		rt.executions = make(map[*logicalExecution]struct{})
	}
	rt.executions[l] = struct{}{}
	rt.InternalMu.Unlock()
	owner := newClosedInputOwner()
	done := l.lifetime.done
	owner.onDrain = func() {
		rt.InternalMu.Lock()
		delete(rt.executions, l)
		rt.InternalMu.Unlock()
		close(done)
		parent.InternalRelease()
	}
	l.request.InternalOwner = owner
	return nil
}
func (s *InternalSessionRuntime) stopExecutions() []*logicalExecution {
	rt := s
	rt.InternalMu.Lock()
	active := make([]*logicalExecution, 0, len(rt.executions))
	for l := range rt.executions {
		active = append(active, l)
	}
	rt.InternalMu.Unlock()
	// Cancel every operation before joining one of them.
	for _, l := range active {
		l.cancelExecution()
	}
	for _, l := range active {
		l.shutdown()
	}
	return active
}

// A zero duration disables only this local deadline. WithTimeout itself avoids
// allocating a timer when the caller already has an earlier deadline.
func InternalExecutionContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return implCluster.InternalExecutionContext(parent, timeout)
}
