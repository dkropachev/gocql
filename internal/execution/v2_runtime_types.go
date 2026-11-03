// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "context"
import "sync"
import "github.com/scylladb/gocql/v2/internal/wire"

// inputOwner serializes close against admission. Each admitted operation and
// transport writer has an independent reference; result leases never use it.
type InternalInputOwner struct {
	mu              sync.Mutex
	InternalClosed  bool
	InternalRefs    int
	done            chan struct{} // nil for execution owners, which are never waited on
	InternalOnDrain func()
	onIdle          func()
	release         func() // bound InternalRelease, reused by writer leases
}

func InternalNewInputOwner() *InternalInputOwner {
	return &InternalInputOwner{done: make(chan struct{})}
}

// resetExecution prepares a reusable execution owner for its next generation.
// The previous generation has drained: its last action was the drain callback,
// which runs after the owner is unlocked and never touches it again.
func (o *InternalInputOwner) resetExecution(onDrain func()) {
	release := o.release
	*o = InternalInputOwner{InternalRefs: 1, InternalClosed: true, InternalOnDrain: onDrain, release: release}
	if o.release == nil {
		o.release = o.InternalRelease
	}
}
func (o *InternalInputOwner) InternalAdmit() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.InternalClosed {
		return ErrClosed
	}
	o.InternalRefs++
	return nil
}
func (o *InternalInputOwner) InternalRetain() { o.mu.Lock(); o.InternalRefs++; o.mu.Unlock() }

// drainLocked retires a closed owner whose last reference is gone. The drain
// callback runs after unlocking: it may admit the next generation of a reusable
// owner, so the caller must not touch the owner after invoking it.
func (o *InternalInputOwner) drainLocked() func() {
	drain := o.InternalOnDrain
	o.InternalOnDrain = nil
	if o.done != nil {
		close(o.done)
	}
	return drain
}
func (o *InternalInputOwner) InternalRelease() {
	o.mu.Lock()
	if o.InternalRefs <= 0 {
		o.mu.Unlock()
		panic("gocql: unbalanced input owner release")
	}
	o.InternalRefs--
	if o.InternalRefs == 0 && o.onIdle != nil {
		o.onIdle()
	}
	var drain func()
	if o.InternalClosed && o.InternalRefs == 0 {
		drain = o.drainLocked()
	}
	o.mu.Unlock()
	if drain != nil {
		drain()
	}
}
func (o *InternalInputOwner) InternalClose() {
	o.mu.Lock()
	if o.InternalClosed {
		o.mu.Unlock()
		return
	}
	o.InternalClosed = true
	var drain func()
	if o.InternalRefs == 0 {
		drain = o.drainLocked()
	}
	o.mu.Unlock()
	if drain != nil {
		drain()
	}
}
func (o *InternalInputOwner) InternalWait(ctx context.Context) error {
	o.mu.Lock()
	closed := o.InternalClosed
	done := o.done
	o.mu.Unlock()
	if !closed {
		return typeValidation("binding", "Wait requires Close first")
	}
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (o *InternalInputOwner) InternalWriterLease(chain wire.Chain) *wire.Lease {
	o.InternalRetain()
	if o.release != nil {
		return wire.NewLease(chain, o.release)
	}
	return wire.NewLease(chain, o.InternalRelease)
}

type InternalExecutionRequest struct {
	query              *queryState
	tabletFeedback     tabletFeedbackStamp
	tabletVersionBlock byte
	connection         *Conn
	InternalStatement  string
	keyspace           string
	InternalPrepared   *InternalPreparedState
	InternalValues     encodedValues
	InternalOwner      *inputOwner
	InternalRouting    RoutingInfo
	InternalOptions    InternalResolvedOptions
	batch              *InternalBatchState
	InternalLogical    *InternalLogicalExecution
}

type ExecutionError struct {
	Err    error
	Result Result
}

func (e *ExecutionError) Error() string { return "gocql: request: " + e.Err.Error() }
func (e *ExecutionError) Unwrap() error { return e.Err }
