// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// inputOwner serializes close against admission. Each admitted operation and
// transport writer has an independent reference; result leases never use it.
//
// The reference count and the closed flag share one atomic word, so admission,
// retention and release take no lock. The transition to zero references runs
// onIdle, and, once closed, the drain callback, as the releasing call's final
// actions.
type InternalInputOwner struct {
	done            chan struct{} // nil for execution owners, which are never waited on
	InternalOnDrain func()
	onIdle          func()
	release         func() // bound InternalRelease, reused by writer leases
	writer          ownerWriterLease
	state           atomic.Int64 // ownerClosed | reference count
	drained         atomic.Bool
}

// ownerWriterLease is an input owner's reusable writer lease. A writer lease
// is never shared: its holders drop it at the final release, so once that
// release finished it can be initialised again. inUse covers the whole
// lease lifetime, including the final release's reads of the lease.
type ownerWriterLease struct {
	owner *InternalInputOwner
	wire.Lease
	inUse atomic.Bool
}

func (w *ownerWriterLease) ReleaseLease() {
	owner := w.owner
	w.inUse.Store(false)
	owner.InternalRelease()
}

const (
	ownerClosed   = int64(1) << 62
	ownerRefsMask = ownerClosed - 1
)

func InternalNewInputOwner() *InternalInputOwner {
	return &InternalInputOwner{done: make(chan struct{})}
}

// newClosedInputOwner returns an execution owner holding the execution's own
// reference: it drains when that reference and every writer's are released.
func newClosedInputOwner() *InternalInputOwner {
	o := new(InternalInputOwner)
	o.state.Store(ownerClosed | 1)
	return o
}

// resetExecution prepares a reusable execution owner for its next generation.
// The previous generation has drained: its last action was the drain callback,
// which runs after the final release and never touches the owner again.
func (o *InternalInputOwner) resetExecution(onDrain func()) {
	o.state.Store(ownerClosed | 1)
	o.drained.Store(false)
	o.done, o.InternalOnDrain, o.onIdle = nil, onDrain, nil
	if o.release == nil {
		o.release = o.InternalRelease
	}
}

// InternalRefs reports the current reference count.
func (o *InternalInputOwner) InternalRefs() int { return int(o.state.Load() & ownerRefsMask) }

// InternalClosed reports whether the owner stopped admitting operations.
func (o *InternalInputOwner) InternalClosed() bool { return o.state.Load()&ownerClosed != 0 }

func (o *InternalInputOwner) InternalAdmit() error {
	for {
		v := o.state.Load()
		if v&ownerClosed != 0 {
			return ErrClosed
		}
		if o.state.CompareAndSwap(v, v+1) {
			return nil
		}
	}
}
func (o *InternalInputOwner) InternalRetain() { o.state.Add(1) }

// drain retires a closed owner whose last reference is gone, once. The drain
// callback may admit the next generation of a reusable owner, so the caller
// must not touch the owner after invoking it.
func (o *InternalInputOwner) drain() {
	if !o.drained.CompareAndSwap(false, true) {
		return
	}
	drain := o.InternalOnDrain
	o.InternalOnDrain = nil
	if o.done != nil {
		close(o.done)
	}
	if drain != nil {
		drain()
	}
}
func (o *InternalInputOwner) InternalRelease() {
	var v int64
	for {
		v = o.state.Load()
		if v&ownerRefsMask == 0 {
			panic("gocql: unbalanced input owner release")
		}
		if o.state.CompareAndSwap(v, v-1) {
			break
		}
	}
	if v&ownerRefsMask != 1 {
		return
	}
	if o.onIdle != nil {
		o.onIdle()
	}
	if v&ownerClosed != 0 {
		o.drain()
	}
}
func (o *InternalInputOwner) InternalClose() {
	for {
		v := o.state.Load()
		if v&ownerClosed != 0 {
			return
		}
		if o.state.CompareAndSwap(v, v|ownerClosed) {
			if v&ownerRefsMask == 0 {
				o.drain()
			}
			return
		}
	}
}
func (o *InternalInputOwner) InternalWait(ctx context.Context) error {
	done := o.done
	if !o.InternalClosed() {
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
	// One attempt at a time normally holds the writer lease; a retry whose
	// previous writer still holds it gets a lease of its own.
	if w := &o.writer; w.inUse.CompareAndSwap(false, true) {
		w.owner = o
		wire.InitLease(&w.Lease, chain, w)
		return &w.Lease
	}
	if o.release != nil {
		return wire.NewLease(chain, o.release)
	}
	return wire.NewLease(chain, o.InternalRelease)
}

type InternalExecutionRequest struct {
	InternalSource statementSource // the statement; nil sends InternalStatement as CQL text
	query          *queryState
	// responseDest is query-owned storage for the first page's response on
	// the direct path. It is cleared once that page succeeded, so that
	// continuation and prefetched pages get storage of their own; Each, which
	// releases every page before requesting the next, lends it again.
	responseDest *responsePage
	// served is the connection whose attempt produced the last direct page;
	// pinned, when set, is where the next page's first attempt goes.
	served, pinned     *Conn
	connection         *Conn
	InternalOwner      *inputOwner
	batch              *InternalBatchState
	InternalLogical    *InternalLogicalExecution
	InternalStatement  string
	keyspace           string
	InternalValues     encodedValues
	tabletFeedback     tabletFeedbackStamp
	InternalRouting    RoutingInfo
	InternalOptions    InternalResolvedOptions
	tabletVersionBlock byte
}

type ExecutionError = runtimeerrors.ExecutionError

func newExecutionError(err error, result Result, status ExecutionStatus, attempts []AttemptFailure) *ExecutionError {
	return runtimeerrors.NewExecutionError(err, result, status, attempts)
}
