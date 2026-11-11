// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func waitDone(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("context was not cancelled")
	}
}

type transientDoneContext interface {
	InternalTransientDone() <-chan struct{}
}

func TestExecutionContext(t *testing.T) {
	t.Run("AfterFunc", func(t *testing.T) {
		c := newExecutionContext()
		ctx, cancel := c.begin(context.Background(), time.Hour)
		ran := make(chan struct{})
		context.AfterFunc(ctx, func() { close(ran) })
		stopped := context.AfterFunc(ctx, func() { t.Error("stopped callback ran") })
		if !stopped() {
			t.Fatal("stop did not prevent the callback")
		}
		child, childCancel := context.WithCancel(ctx)
		defer childCancel()
		cancel()
		waitDone(t, child)
		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Fatal("AfterFunc did not run")
		}
		late := make(chan struct{})
		if context.AfterFunc(ctx, func() { close(late) })() {
			t.Fatal("stop after cancellation reported success")
		}
		<-late

		ctx, cancel = c.begin(context.Background(), time.Hour)
		defer cancel()
		if stopped() {
			t.Fatal("stale stop affected the next generation")
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	})

	t.Run("Cancel", func(t *testing.T) {
		c := newExecutionContext()
		ctx, cancel := c.begin(context.Background(), time.Hour)
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		cancel()
		waitDone(t, ctx)
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal(ctx.Err())
		}

		parent, parentCancel := context.WithCancel(context.WithValue(context.Background(), "key", "value"))
		ctx, cancel = c.begin(parent, time.Hour)
		defer cancel()
		if ctx.Value("key") != "value" {
			t.Fatal("value not forwarded")
		}
		parentCancel()
		waitDone(t, ctx)
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal(ctx.Err())
		}

		early, earlyCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer earlyCancel()
		ctx, cancel = c.begin(early, time.Hour)
		defer cancel()
		if d, _ := ctx.Deadline(); time.Until(d) > 10*time.Millisecond {
			t.Fatal("parent deadline not inherited", d)
		}
		waitDone(t, ctx)
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal(ctx.Err())
		}

		done, doneCancel := context.WithCancel(context.Background())
		doneCancel()
		ctx, cancel = c.begin(done, time.Hour)
		defer cancel()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("cancelled parent admitted", ctx.Err())
		}
	})

	t.Run("Concurrency", func(t *testing.T) {
		c := newExecutionContext()
		for range 200 {
			ctx, cancel := c.begin(context.Background(), time.Millisecond)
			var wg sync.WaitGroup
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-ctx.Done()
					_ = ctx.Err()
				}()
			}
			go cancel()
			wg.Wait()
			cancel()
		}
	})

	t.Run("Deadline", func(t *testing.T) {
		c := newExecutionContext()
		ctx, cancel := c.begin(context.Background(), 10*time.Millisecond)
		defer cancel()
		if d, ok := ctx.Deadline(); !ok || time.Until(d) > 10*time.Millisecond {
			t.Fatal("deadline", d, ok)
		}
		waitDone(t, ctx)
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal(ctx.Err())
		}
	})

	t.Run("Finish", func(t *testing.T) {
		c := newExecutionContext()
		ctx, finish := c.begin(context.Background(), time.Hour)
		transient := ctx.(transientDoneContext).InternalTransientDone()
		finish()
		// The finished generation still reports cancellation to later observers.
		waitDone(t, ctx)
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal(ctx.Err())
		}
		select {
		case <-transient:
			t.Fatal("unshared channel was closed instead of reused")
		default:
		}
		ctx, finish = c.begin(context.Background(), time.Hour)
		if got := ctx.(transientDoneContext).InternalTransientDone(); got != transient {
			t.Fatal("open channel was not reused")
		}
		// A real cancellation wakes transient waiters and is never reused.
		c.cancelFunc()
		select {
		case <-transient:
		default:
			t.Fatal("cancellation did not close the transient channel")
		}
		finish()
		ctx, finish = c.begin(context.Background(), time.Hour)
		if ctx.(transientDoneContext).InternalTransientDone() == transient {
			t.Fatal("closed channel was reused")
		}
		// A Done watcher may outlive the attempt, so finish must close its channel.
		shared := ctx.Done()
		finish()
		select {
		case <-shared:
		default:
			t.Fatal("finish left a Done watcher waiting")
		}
		// So must a registered AfterFunc.
		ctx, finish = c.begin(context.Background(), time.Hour)
		ran := make(chan struct{})
		context.AfterFunc(ctx, func() { close(ran) })
		finish()
		<-ran
		ctx, finish = c.begin(context.Background(), time.Hour)
		defer finish()
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	})

	t.Run("Generation", func(t *testing.T) {
		t.Run("KeptTimerFollowsLaterDeadline", func(t *testing.T) {
			c := newExecutionContext()
			_, cancel := c.begin(context.Background(), 20*time.Millisecond)
			cancel()
			// The finished generation's firing is kept; it must re-arm for this
			// generation's later deadline instead of cancelling it early.
			start := time.Now()
			ctx, cancel := c.begin(context.Background(), 80*time.Millisecond)
			defer cancel()
			time.Sleep(40 * time.Millisecond)
			if ctx.Err() != nil {
				t.Fatal("a kept firing cancelled a later deadline", ctx.Err())
			}
			waitDone(t, ctx)
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatal(ctx.Err())
			}
			if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
				t.Fatal("cancelled before its deadline", elapsed)
			}
			// An earlier deadline than the kept firing re-arms the timer.
			_, cancel = c.begin(context.Background(), time.Hour)
			cancel()
			ctx, cancel = c.begin(context.Background(), 10*time.Millisecond)
			defer cancel()
			waitDone(t, ctx)
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatal(ctx.Err())
			}
		})

		t.Run("LateCallbacksSpareNext", func(t *testing.T) {
			c := newExecutionContext()
			_, cancel := c.begin(context.Background(), time.Millisecond)
			cancel()
			parent, parentCancel := context.WithCancel(context.Background())
			_, cancel = c.begin(parent, time.Hour)
			cancel()
			ctx, cancel := c.begin(context.Background(), time.Hour)
			defer cancel()
			parentCancel()
			time.Sleep(20 * time.Millisecond)
			if ctx.Err() != nil {
				t.Fatal("a previous generation cancelled the current one", ctx.Err())
			}
			// A parent callback already running for a previous generation is discarded.
			c.parentCancelled()
			if ctx.Err() != nil {
				t.Fatal("stale parent callback cancelled the current generation")
			}
			// A late timer callback sees the current, later deadline and is discarded.
			c.expire()
			if ctx.Err() != nil {
				t.Fatal("early timer callback cancelled the current generation")
			}
			// A timer that fired late for a cancelled generation must leave the next
			// generation's timer armed.
			ctx, cancel = c.begin(context.Background(), 10*time.Millisecond)
			defer cancel()
			waitDone(t, ctx)
		})
	})

	// A cancellable parent is watched lazily: no registration until something
	// may wait on Done, Err reports the parent's cancellation and closes Done,
	// InternalParentDone exposes the parent's channel to transport waits, and a
	// handed-out Done channel or a derived context still follows the parent.
	t.Run("LazyParent", func(t *testing.T) {
		c := newExecutionContext()
		parent, parentCancel := context.WithCancel(context.Background())
		ctx, cancel := c.begin(parent, time.Hour)
		if c.stopParent != nil || c.InternalParentDone() != parent.Done() {
			t.Fatal("parent registered eagerly, or its channel not exposed")
		}
		parentCancel()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("Err missed the parent's cancellation", ctx.Err())
		}
		select {
		case <-c.InternalTransientDone():
		default:
			t.Fatal("Err reported cancellation with Done still open")
		}
		cancel()

		parent, parentCancel = context.WithCancel(context.Background())
		ctx, cancel = c.begin(parent, time.Hour)
		done := ctx.Done() // a watcher may now wait on the channel alone
		parentCancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("handed-out Done did not follow the parent")
		}
		cancel()

		parent, parentCancel = context.WithCancel(context.Background())
		ctx, cancel = c.begin(parent, time.Hour)
		derived, derivedCancel := context.WithCancel(ctx)
		defer derivedCancel()
		parentCancel()
		waitDone(t, derived)
		cancel()

		// A linked page context exposes the execution's parent channel and
		// follows it once a watcher waits on the page's own channel.
		page := newExecutionContext()
		parent, parentCancel = context.WithCancel(context.Background())
		ctx, cancel = c.begin(parent, time.Hour)
		pctx, pcancel := page.begin(ctx, time.Hour)
		if page.InternalParentDone() != parent.Done() || c.stopParent != nil {
			t.Fatal("page context does not expose the execution's parent")
		}
		pdone := pctx.Done()
		parentCancel()
		select {
		case <-pdone:
		case <-time.After(5 * time.Second):
			t.Fatal("page Done did not follow the execution's parent")
		}
		pcancel()
		cancel()

		// A parent without Done is not watched at all.
		ctx, cancel = c.begin(context.Background(), time.Hour)
		if c.lazy || c.InternalParentDone() != nil || ctx.Err() != nil {
			t.Fatal("background parent watched")
		}
		cancel()
	})

	t.Run("LinkedChild", func(t *testing.T) {
		// A context begun under another execution context links to it directly:
		// the parent's cancellation, deadline and finish reach it, a linked child
		// registers no AfterFunc on the parent, and a cancellation of an earlier
		// child generation spares the next one.
		t.Run("Propagation", func(t *testing.T) {
			parent, child := newExecutionContext(), newExecutionContext()

			pctx, pcancel := parent.begin(context.Background(), time.Hour)
			cctx, ccancel := child.begin(pctx, time.Minute)
			if len(parent.afters) != 0 || parent.shared || parent.child != child {
				t.Fatal("child not linked directly", len(parent.afters), parent.shared)
			}
			if d, ok := cctx.Deadline(); !ok || time.Until(d) > time.Minute {
				t.Fatal("child deadline", d, ok)
			}
			ccancel()
			if parent.child != nil || child.linked != nil {
				t.Fatal("finished child still linked")
			}
			cctx, ccancel = child.begin(pctx, time.Minute)
			pcancel() // finishing the parent cancels a linked child
			waitDone(t, cctx)
			if !errors.Is(cctx.Err(), context.Canceled) {
				t.Fatal(cctx.Err())
			}
			ccancel()

			pctx, pcancel = parent.begin(context.Background(), 20*time.Millisecond)
			cctx, ccancel = child.begin(pctx, time.Hour)
			waitDone(t, cctx)
			if !errors.Is(cctx.Err(), context.DeadlineExceeded) {
				t.Fatal("parent deadline did not reach the child", cctx.Err())
			}
			ccancel()
			pcancel()

			// Beginning under a cancelled parent is cancelled at once.
			pctx, pcancel = parent.begin(context.Background(), time.Hour)
			pcancel()
			cctx, ccancel = child.begin(pctx, time.Hour)
			if !errors.Is(cctx.Err(), context.Canceled) {
				t.Fatal(cctx.Err())
			}
			ccancel()

			// A late cancellation for an earlier child generation spares the next.
			pctx, pcancel = parent.begin(context.Background(), time.Hour)
			defer pcancel()
			_, ccancel = child.begin(pctx, time.Hour)
			stale := child.gen
			ccancel()
			cctx, ccancel = child.begin(pctx, time.Hour)
			defer ccancel()
			child.cancelGen(stale, context.Canceled)
			if cctx.Err() != nil {
				t.Fatal("stale parent cancellation reached the next generation")
			}
		})

		// Parent cancellation, child finish and child expiry race without deadlock
		// or a missed cancellation.
		t.Run("Races", func(t *testing.T) {
			parent, child := newExecutionContext(), newExecutionContext()
			for i := 0; i < 2000; i++ {
				pctx, pcancel := parent.begin(context.Background(), time.Hour)
				cctx, ccancel := child.begin(pctx, time.Duration(i%3)*time.Microsecond+time.Microsecond)
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); parent.cancel(context.Canceled) }()
				go func() { defer wg.Done(); _ = cctx.Err() }()
				wg.Wait()
				waitDone(t, cctx)
				ccancel()
				pcancel()
			}
		})
	})

	t.Run("WakeChannel", func(t *testing.T) {
		c := newExecutionContext()
		_, cancel := c.begin(context.Background(), time.Hour)
		wake := c.InternalWakeChannel()
		cancel()
		select {
		case <-wake:
			t.Fatal("finishing a generation woke a wait")
		default:
		}
		_, _ = c.begin(context.Background(), time.Hour)
		c.cancelFunc()
		select {
		case <-wake:
		default:
			t.Fatal("cancellation did not wake a wait")
		}
		// A wake nobody received does not reach the next generation.
		_, _ = c.begin(context.Background(), time.Hour)
		c.cancelFunc()
		_, cancel = c.begin(context.Background(), time.Hour)
		defer cancel()
		select {
		case <-wake:
			t.Fatal("a previous generation's wake reached the next one")
		default:
		}
	})
}
