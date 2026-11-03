// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

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

func TestExecutionContextDeadline(t *testing.T) {
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
}

func TestExecutionContextCancelAndParent(t *testing.T) {
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
}

func TestExecutionContextLateCallbacksSpareNextGeneration(t *testing.T) {
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
}

func TestExecutionContextAfterFuncAndDerived(t *testing.T) {
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
}

func TestExecutionContextConcurrentUse(t *testing.T) {
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
}
