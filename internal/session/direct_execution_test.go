// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type directTestObserver struct{}

func (directTestObserver) ObserveRequest(settings.RequestEvent) {}
func (directTestObserver) ObserveAttempt(routing.AttemptEvent)  {}

func (o directTestObserver) StartRequest(ctx context.Context, _ settings.RequestStart) (context.Context, func(settings.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o directTestObserver) StartAttempt(context.Context, routing.AttemptStart) func(routing.AttemptEvent) {
	return o.ObserveAttempt
}

type directTestStreamObserver struct{}

func (directTestStreamObserver) StreamContext(context.Context) implTransport.StreamObserverContext {
	return nil
}
