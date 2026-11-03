// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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
	mu          sync.Mutex
	parent      context.Context
	deadline    time.Time // effective deadline, including the parent's
	hasDeadline bool
	timerAt     time.Time // own deadline armed on timer; zero when disarmed
	timer       *time.Timer
	// done is the generation's Done channel. It is replaced only in begin,
	// when a cancellation closed it, so InternalTransientDone reads it without
	// the lock: begin runs only after the previous generation drained.
	done       chan struct{}
	doneClosed bool // done was closed by a cancellation
	shared     bool // Done was handed out this generation
	cancelled  atomic.Bool
	err        error
	gen        uint64
	stopParent func() bool
	afters     []*executionAfterFunc
	cancelFunc context.CancelFunc // bound once: cancel(context.Canceled)
	finishFunc context.CancelFunc // bound once: finish
	parentFunc func()             // bound once: parentCancelled
}

type executionAfterFunc struct {
	ctx *executionContext
	gen uint64
	f   func()
}

var closedExecutionDone = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

func newExecutionContext() *executionContext {
	c := &executionContext{}
	c.cancelFunc = c.cancelCanceled
	c.finishFunc = c.finish
	c.parentFunc = c.parentCancelled
	return c
}

// begin starts a generation with parent and timeout and returns the context
// with the CancelFunc that finishes it. timeout <= 0 leaves only the parent's
// deadline. Cancellation from another goroutine must use cancelFunc.
func (c *executionContext) begin(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	c.gen++
	gen := c.gen
	c.parent = parent
	c.err = nil
	c.cancelled.Store(false)
	c.shared = false
	c.afters = c.afters[:0]
	c.timerAt = time.Time{}
	if c.done == nil || c.doneClosed {
		c.done, c.doneClosed = make(chan struct{}), false
	}
	c.deadline, c.hasDeadline = parent.Deadline()
	if timeout > 0 {
		own := time.Now().Add(timeout)
		if !c.hasDeadline || own.Before(c.deadline) {
			c.deadline, c.hasDeadline = own, true
			c.timerAt = own
		}
	}
	if !c.timerAt.IsZero() {
		if c.timer == nil {
			c.timer = time.AfterFunc(timeout, c.expire)
		} else {
			c.timer.Reset(timeout)
		}
	}
	c.mu.Unlock()
	if err := parent.Err(); err != nil {
		c.cancel(err)
	} else if parent.Done() != nil {
		stop := context.AfterFunc(parent, c.parentFunc)
		c.mu.Lock()
		if c.gen == gen && c.err == nil {
			c.stopParent = stop
			stop = nil
		}
		c.mu.Unlock()
		if stop != nil {
			stop()
		}
	}
	return c, c.finishFunc
}

// finish ends the generation after its attempts returned. With no Done watcher
// and no AfterFunc registered, the open channel is kept for the next
// generation instead of being closed and reallocated.
func (c *executionContext) finish() {
	c.mu.Lock()
	if c.err != nil || c.shared || len(c.afters) > 0 {
		c.cancelLocked(context.Canceled)
		return
	}
	c.err = context.Canceled
	c.cancelled.Store(true)
	c.disarmLocked()
	c.mu.Unlock()
}

// disarmLocked stops the timer and parent registration. Stop under the lock: a
// deferred stop could otherwise disarm the next generation. Neither stop waits
// for a callback that needs this lock.
func (c *executionContext) disarmLocked() {
	c.timerAt = time.Time{}
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.stopParent != nil {
		c.stopParent()
		c.stopParent = nil
	}
}

func (c *executionContext) expire() {
	c.mu.Lock()
	// A late callback from a previous generation sees a later or no deadline.
	if c.timerAt.IsZero() || time.Now().Before(c.timerAt) {
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
	c.disarmLocked()
	afters := c.afters
	c.afters = nil
	c.mu.Unlock()
	for _, a := range afters {
		go a.f()
	}
}

func (c *executionContext) Deadline() (time.Time, bool) { return c.deadline, c.hasDeadline }

func (c *executionContext) Done() <-chan struct{} {
	c.mu.Lock()
	d := c.doneLocked()
	c.shared = true
	c.mu.Unlock()
	return d
}

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
		return nil
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
