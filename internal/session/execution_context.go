// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/internal/transport"
)

// executionContext is a reusable query's per-execution deadline and
// cancellation scope, replacing context.WithTimeout on the direct path. The
// struct, its timer, its Done channel and its bound CancelFuncs belong to the
// query; a generation allocates only the parent registration of a cancellable
// parent.
//
// The CancelFunc returned by begin finishes the generation. Done channels are
// handed out two ways: Done, for watchers that may outlive an attempt, and
// InternalTransientDone, for transport waits that end before the attempt
// returns. When no Done watcher or AfterFunc exists at finish, nobody can be
// waiting on the channel, so it stays open for the next generation; Done and
// Err still report cancellation for the rest of this generation. A real
// cancellation (cancelFunc, deadline, parent) always closes the channel.
//
// A generation begins only after the previous one drained (the query is idle),
// so no in-flight work still holds the context. Callbacks that fire late from
// a previous generation are discarded by generation or deadline checks.
type executionContext struct {
	deadline time.Time // effective deadline, including the parent's
	timerAt  time.Time // own deadline armed on timer; zero when disarmed
	// timerFire is when timer is set to fire, zero when it is stopped or has
	// fired. A finished generation leaves the timer running: deadlines grow
	// from one generation to the next, so a later generation keeps the
	// earlier firing and expire re-arms it for what remains.
	timerFire time.Time
	parent    context.Context
	err       error
	timer     *time.Timer
	// done is the generation's Done channel. It is replaced only in begin,
	// when a cancellation closed it, so InternalTransientDone reads it without
	// the lock: begin runs only after the previous generation drained.
	done chan struct{}
	// wake receives a wake when a generation is cancelled, so that a typed
	// wait receiving its response from it needs no select (see
	// transport.InternalWake). begin drains a wake left from before.
	wake       chan transport.InternalCallResp
	stopParent func() bool
	// linked is the execution context this generation is a child of, which
	// cancels it through child without an AfterFunc registration.
	linked *executionContext
	// child and childGen name the generation of a linked child context.
	child       *executionContext
	cancelFunc  context.CancelFunc // bound once: cancel(context.Canceled)
	finishFunc  context.CancelFunc // bound once: finish
	parentFunc  func()             // bound once: parentCancelled
	afters      []*executionAfterFunc
	gen         uint64
	childGen    uint64
	mu          sync.Mutex
	cancelled   atomic.Bool
	hasDeadline bool
	doneClosed  bool // done was closed by a cancellation
	shared      bool // Done was handed out this generation
	// lazy marks a cancellable parent that is watched without a
	// registration: Err checks it, transport waits select on its Done
	// channel (InternalParentDone), and Done or AfterFunc register a
	// watcher only once something may wait on this context's channel.
	lazy     bool
	watching bool // the lazy parent's watcher is registered
}

type executionAfterFunc struct {
	ctx *executionContext
	f   func()
	gen uint64
}

var closedExecutionDone = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

func newExecutionContext() *executionContext {
	c := &executionContext{wake: transport.InternalNewWakeChannel()}
	c.cancelFunc = c.cancelCanceled
	c.finishFunc = c.finish
	c.parentFunc = c.parentCancelled
	return c
}

// begin starts a generation with parent and timeout and returns the context
// with the CancelFunc that finishes it. timeout <= 0 leaves only the parent's
// deadline. Cancellation from another goroutine must use cancelFunc.
func (c *executionContext) begin(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return c.beginAt(parent, timeout, time.Time{})
}

// beginAt is begin with the time the execution started, read by the caller
// for its own use; a zero now reads the clock when a timeout needs it.
func (c *executionContext) beginAt(parent context.Context, timeout time.Duration, now time.Time) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	c.gen++
	gen := c.gen
	c.parent = parent
	c.err = nil
	c.cancelled.Store(false)
	c.shared = false
	c.afters = c.afters[:0]
	c.timerAt = time.Time{}
	c.lazy, c.watching = false, false
	transport.InternalDrainWake(c.wake)
	if c.done == nil || c.doneClosed {
		c.done, c.doneClosed = make(chan struct{}), false
	}
	c.deadline, c.hasDeadline = parent.Deadline()
	if timeout > 0 {
		if now.IsZero() {
			now = time.Now()
		}
		own := now.Add(timeout)
		if !c.hasDeadline || own.Before(c.deadline) {
			c.deadline, c.hasDeadline = own, true
			c.timerAt = own
		}
	}
	if !c.timerAt.IsZero() {
		if c.timer == nil {
			c.timer = time.AfterFunc(timeout, c.expire)
			c.timerFire = c.timerAt
		} else if c.timerFire.IsZero() || c.timerAt.Before(c.timerFire) {
			c.timer.Reset(timeout)
			c.timerFire = c.timerAt
		}
	}
	p, linked := parent.(*executionContext)
	linked = linked && p != c
	if linked || parent.Done() != nil {
		c.lazy = true
	}
	c.mu.Unlock()
	if linked {
		// A child of another execution context (a page within an
		// execution) links to it directly; disarm unlinks it.
		if err := p.linkChild(c, gen); err != nil {
			c.cancel(err)
		}
	} else if err := parent.Err(); err != nil {
		c.cancel(err)
	}
	return c, c.finishFunc
}

// watchParentLocked registers the lazy parent's watcher once something may
// wait on this generation's Done channel. A linked child relies on its
// parent's link for the parent's own cancellation, and asks the parent to
// watch the parent's parent. Lock order is child, then parent.
func (c *executionContext) watchParentLocked() {
	if !c.lazy || c.watching || c.err != nil {
		return
	}
	c.watching = true
	if p, ok := c.parent.(*executionContext); ok {
		p.mu.Lock()
		p.watchParentLocked()
		p.mu.Unlock()
		return
	}
	c.stopParent = context.AfterFunc(c.parent, c.parentFunc)
}

// InternalParentDone is the Done channel of the cancellable context this
// generation watches lazily, or nil. A transport wait selects on it next to
// InternalTransientDone; Err then reports the parent's cancellation.
func (c *executionContext) InternalParentDone() <-chan struct{} {
	if !c.lazy {
		return nil
	}
	if p, ok := c.parent.(*executionContext); ok {
		return p.InternalParentDone()
	}
	return c.parent.Done()
}

// finish ends the generation after its attempts returned. With no Done watcher
// and no AfterFunc registered, the open channel is kept for the next
// generation instead of being closed and reallocated.
func (c *executionContext) finish() {
	c.mu.Lock()
	if c.err != nil || c.shared || len(c.afters) > 0 || c.child != nil {
		c.cancelLocked(context.Canceled)
		return
	}
	c.err = context.Canceled
	c.cancelled.Store(true)
	// The timer keeps running for a later generation; when it fires idle,
	// expire lets it stop.
	c.timerAt = time.Time{}
	if c.stopParent != nil || c.linked != nil {
		c.disarmLocked()
	}
	c.mu.Unlock()
}

// disarmLocked stops the timer and parent registration. Stop under the lock: a
// deferred stop could otherwise disarm the next generation. Neither stop waits
// for a callback that needs this lock.
func (c *executionContext) disarmLocked() {
	c.timerAt = time.Time{}
	if c.timer != nil {
		c.timer.Stop()
		c.timerFire = time.Time{}
	}
	if c.stopParent != nil {
		c.stopParent()
		c.stopParent = nil
	}
	if p := c.linked; p != nil {
		c.linked = nil
		p.mu.Lock()
		if p.child == c {
			p.child = nil
		}
		p.mu.Unlock()
	}
}

// linkChild makes child's generation gen follow c's cancellation, or reports
// c's error when it is already cancelled. Lock order is child, then parent:
// a cancelling parent releases its lock before it cancels the child.
func (c *executionContext) linkChild(child *executionContext, gen uint64) error {
	child.mu.Lock()
	c.mu.Lock()
	err := c.err
	if err == nil && child.gen == gen && child.err == nil {
		child.linked = c
		c.child, c.childGen = child, gen
	}
	c.mu.Unlock()
	child.mu.Unlock()
	return err
}

// cancelGen cancels generation gen only, so that a parent's late
// cancellation cannot reach a later generation of the child.
func (c *executionContext) cancelGen(gen uint64, err error) {
	c.mu.Lock()
	if c.gen != gen {
		c.mu.Unlock()
		return
	}
	c.cancelLocked(err)
}

func (c *executionContext) expire() {
	c.mu.Lock()
	c.timerFire = time.Time{}
	// A firing kept from a previous generation, or a late callback, sees a
	// later or no deadline; a later deadline re-arms the timer.
	if c.timerAt.IsZero() {
		c.mu.Unlock()
		return
	}
	if now := time.Now(); now.Before(c.timerAt) {
		c.timer.Reset(c.timerAt.Sub(now))
		c.timerFire = c.timerAt
		c.mu.Unlock()
		return
	}
	c.cancelLocked(context.DeadlineExceeded)
}

// parentCancelled follows the current generation's parent. A late callback
// from an earlier, different parent finds the current parent alive and does
// nothing; a reused parent that is cancelled must cancel this generation too.
func (c *executionContext) parentCancelled() {
	c.mu.Lock()
	err := c.parent.Err()
	if err == nil {
		c.mu.Unlock()
		return
	}
	c.cancelLocked(err)
}

func (c *executionContext) cancelCanceled() { c.cancel(context.Canceled) }

func (c *executionContext) cancel(err error) {
	c.mu.Lock()
	c.cancelLocked(err)
}

// cancelLocked cancels the current generation, stops its timer and parent
// registration, unlocks, and starts AfterFunc callbacks.
func (c *executionContext) cancelLocked(err error) {
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	c.cancelled.Store(true)
	close(c.done)
	c.doneClosed = true
	transport.InternalWake(c.wake)
	c.disarmLocked()
	afters := c.afters
	c.afters = nil
	child, childGen := c.child, c.childGen
	c.child = nil
	c.mu.Unlock()
	if child != nil {
		child.cancelGen(childGen, err)
	}
	for _, a := range afters {
		go a.f()
	}
}

func (c *executionContext) Deadline() (time.Time, bool) { return c.deadline, c.hasDeadline }

func (c *executionContext) Done() <-chan struct{} {
	c.mu.Lock()
	c.watchParentLocked()
	d := c.doneLocked()
	c.shared = true
	c.mu.Unlock()
	return d
}

// InternalWakeChannel is the channel cancellation wakes; see wake.
func (c *executionContext) InternalWakeChannel() chan transport.InternalCallResp { return c.wake }

// InternalTransientDone is Done for a wait that ends before the current
// attempt returns, so that it does not prevent channel reuse at finish.
func (c *executionContext) InternalTransientDone() <-chan struct{} {
	if c.cancelled.Load() {
		return closedExecutionDone
	}
	return c.done
}

func (c *executionContext) doneLocked() chan struct{} {
	if c.err != nil {
		return closedExecutionDone
	}
	return c.done
}

func (c *executionContext) Err() error {
	if !c.cancelled.Load() {
		if !c.lazy {
			return nil
		}
		// Done must be closed once Err reports, so a lazily watched
		// parent's cancellation cancels this generation first.
		err := c.parent.Err()
		if err == nil {
			return nil
		}
		c.cancel(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *executionContext) Value(key any) any { return c.parent.Value(key) }

// AfterFunc implements the context package's afterFuncer, so contexts derived
// from an execution context, and context.AfterFunc, need no watcher goroutine.
func (c *executionContext) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		go f()
		return func() bool { return false }
	}
	c.watchParentLocked()
	a := &executionAfterFunc{ctx: c, gen: c.gen, f: f}
	c.afters = append(c.afters, a)
	c.mu.Unlock()
	return a.stop
}

// stop reports whether it prevented f from running.
func (a *executionAfterFunc) stop() bool {
	c := a.ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != a.gen {
		return false
	}
	for i, registered := range c.afters {
		if registered == a {
			c.afters = append(c.afters[:i], c.afters[i+1:]...)
			return true
		}
	}
	return false
}

func (c *executionContext) String() string { return "gocql.executionContext" }
