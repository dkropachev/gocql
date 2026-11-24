// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type directTestObserver struct{}

func (directTestObserver) ObserveRequest(observe.RequestEvent)  {}
func (directTestObserver) ObserveAttempt(routing2.AttemptEvent) {}

func (o directTestObserver) StartRequest(ctx context.Context, _ observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o directTestObserver) StartAttempt(context.Context, observe.AttemptStart) func(routing2.AttemptEvent) {
	return o.ObserveAttempt
}

type directTestStreamObserver struct{}

func (directTestStreamObserver) StreamContext(context.Context) implTransport.StreamObserverContext {
	return nil
}
