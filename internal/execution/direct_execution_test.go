// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"testing"
	"time"

	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type directTestObserver struct{}

func (directTestObserver) ObserveRequest(RequestEvent) {}
func (directTestObserver) ObserveAttempt(AttemptEvent) {}

type directTestStreamObserver struct{}

func (directTestStreamObserver) StreamContext(context.Context) implTransport.StreamObserverContext {
	return nil
}

// TestCanExecuteDirectOneShot checks that executions without a reusable
// query take the direct path unless an observer, speculation, write
// coalescing or a stream-observed connection needs the general one.
func TestCanExecuteDirectOneShot(t *testing.T) {
	rt := &InternalSessionRuntime{}
	request := func() *InternalExecutionRequest {
		return &InternalExecutionRequest{InternalLogical: &InternalLogicalExecution{}, InternalOptions: InternalResolvedOptions{InternalProfile: defaultProfile()}}
	}
	if !canExecuteDirect(rt, request()) {
		t.Fatal("one-shot execution should run direct")
	}
	r := request()
	r.query = &queryState{}
	if !canExecuteDirect(rt, r) {
		t.Fatal("query execution should run direct")
	}
	r = request()
	r.InternalLogical.observer = directTestObserver{}
	if canExecuteDirect(rt, r) {
		t.Fatal("an observer needs the general path")
	}
	r = request()
	r.connection = &Conn{InternalStreamObserver: directTestStreamObserver{}}
	if canExecuteDirect(rt, r) {
		t.Fatal("a stream-observed connection needs the general path")
	}
	r = request()
	r.InternalLogical = nil
	if canExecuteDirect(rt, r) {
		t.Fatal("an unadmitted request needs the general path")
	}
	coalescing := &InternalSessionRuntime{}
	coalescing.InternalConfig.CoalesceWait = time.Millisecond
	if canExecuteDirect(coalescing, request()) {
		t.Fatal("write coalescing needs the general path")
	}
}
