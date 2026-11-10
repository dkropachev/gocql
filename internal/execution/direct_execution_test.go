// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type directTestObserver struct{}

func (directTestObserver) ObserveRequest(settings.RequestEvent) {}
func (directTestObserver) ObserveAttempt(routing.AttemptEvent)  {}

type directTestStreamObserver struct{}

func (directTestStreamObserver) StreamContext(context.Context) implTransport.StreamObserverContext {
	return nil
}
