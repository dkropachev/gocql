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

package session

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
	// Close marks the session closing (Core.Closed) before it lists the
	// registry, shard by shard under each shard's lock. Checking the mark under
	// the shard lock orders this registration either before Close's listing of
	// the shard, which then finds it, or after the mark, which refuses it; a
	// refused execution is never listed.
	e := &sessionExecution{l: l, parent: parent}
	e.owner.state.Store(ownerClosed | 1)
	e.owner.onDrain = e.drained
	shard := s.registry.shard(l.id)
	e.shard = shard
	shard.mu.Lock()
	if s.Core.Closed() {
		shard.mu.Unlock()
		return runtimeerrors.ErrClosed
	}
	// Close may stop the execution as soon as the lock is released, and the
	// stop releases this owner.
	l.request.InternalOwner = &e.owner
	shard.push(e)
	shard.mu.Unlock()
	return nil
}

// executionShards is the number of registry shards, a power of two. A shard is
// picked by request id, not by P, and drained often runs on another goroutine
// than the registration, so the lines are not core-local. Each shard has its
// own lock and cache lines, so concurrent registrations, which take
// consecutive request ids, rarely share either.
const executionShards = 32

// executionRegistry holds the session's in-flight executions that no reusable
// query owns, for Close to stop. It replaced a map under the session's
// InternalMu: registration and drain each took that lock, so every
// Bind+Exec serialized on it twice.
type executionRegistry struct {
	shards [executionShards]executionShard
	_      cacheLinePad
}

// A shard spans two cache lines: the registry is only 8-byte aligned, so with
// one line per shard its lock could share a line with a neighbour's.
type executionShard struct {
	head *sessionExecution
	mu   sync.Mutex
	_    [2*cacheLineSize - 16]byte
}

func (r *executionRegistry) shard(id uint64) *executionShard {
	return &r.shards[id&(executionShards-1)]
}

// sessionExecution is a registered execution: its input owner and its links
// in a shard's list. The owner drains when the execution's own reference and
// every writer's are released.
type sessionExecution struct {
	l          *logicalExecution
	parent     *inputOwner
	shard      *executionShard
	prev, next *sessionExecution
	owner      inputOwner
}

// push and unlink run under the shard lock.
func (sh *executionShard) push(e *sessionExecution) {
	e.next = sh.head
	if sh.head != nil {
		sh.head.prev = e
	}
	sh.head = e
}

func (sh *executionShard) unlink(e *sessionExecution) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		sh.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	}
	e.prev, e.next = nil, nil
}

func (e *sessionExecution) drained() {
	e.shard.mu.Lock()
	e.shard.unlink(e)
	e.shard.mu.Unlock()
	close(e.l.lifetime.done)
	e.parent.InternalRelease()
}

// stopExecutions cancels and stops every registered execution, returning them
// for Close to join. Close marked the session closing first, so every
// registration either is listed here or was refused.
func (s *InternalSessionRuntime) stopExecutions() []*logicalExecution {
	var active []*logicalExecution
	for i := range s.registry.shards {
		shard := &s.registry.shards[i]
		shard.mu.Lock()
		for e := shard.head; e != nil; e = e.next {
			active = append(active, e.l)
		}
		shard.mu.Unlock()
	}
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
