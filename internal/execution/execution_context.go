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
// struct, its timer and its bound CancelFunc belong to the query; a generation
// allocates only its Done channel and, for a cancellable parent, the parent
// registration.
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
	done        chan struct{}
	cancelled   atomic.Bool
	err         error
	gen         uint64
	stopParent  func() bool
	afters      []*executionAfterFunc
	cancelFunc  context.CancelFunc // bound once: cancel(context.Canceled)
	parentFunc  func()             // bound once: parentCancelled
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
	c.parentFunc = c.parentCancelled
	return c
}

// begin starts a generation with parent and timeout and returns the context
// with its CancelFunc. timeout <= 0 leaves only the parent's deadline.
func (c *executionContext) begin(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	c.gen++
	gen := c.gen
	c.parent = parent
	c.err = nil
	c.cancelled.Store(false)
	c.done = nil
	c.afters = c.afters[:0]
	c.timerAt = time.Time{}
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
	return c, c.cancelFunc
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
	if c.done == nil {
		c.done = closedExecutionDone
	} else {
		close(c.done)
	}
	c.timerAt = time.Time{}
	// Stop under the lock: a deferred stop could otherwise disarm the next
	// generation. Neither stop waits for a callback that needs this lock.
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.stopParent != nil {
		c.stopParent()
		c.stopParent = nil
	}
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
	if c.done == nil {
		c.done = make(chan struct{})
	}
	d := c.done
	c.mu.Unlock()
	return d
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
