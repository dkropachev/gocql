// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	routing2 "github.com/scylladb/gocql/v2/routing"
)

type directTestObserver struct{}

func (directTestObserver) ObserveRequest(settings.RequestEvent) {}
func (directTestObserver) ObserveAttempt(routing2.AttemptEvent) {}

func (o directTestObserver) StartRequest(ctx context.Context, _ settings.RequestStart) (context.Context, func(settings.RequestEvent)) {
	return ctx, o.ObserveRequest
}
func (o directTestObserver) StartAttempt(context.Context, routing2.AttemptStart) func(routing2.AttemptEvent) {
	return o.ObserveAttempt
}

type directTestStreamObserver struct{}

func (directTestStreamObserver) StreamContext(context.Context) implTransport.StreamObserverContext {
	return nil
}
