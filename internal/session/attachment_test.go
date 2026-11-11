// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/cluster"
)

func attachmentSession() (*cluster.Core, *InternalSessionRuntime) {
	s := new(cluster.Core)
	data := cluster.SessionData(s)
	data.InternalCtx, data.InternalCancel = context.WithCancel(context.Background())
	return s, InternalRuntimeFor(s)
}
func attachmentLogical(s *cluster.Core, parent *inputOwner) (*logicalExecution, context.CancelFunc) {
	_, cancel := context.WithCancel(context.Background())
	return &logicalExecution{rt: InternalRuntimeFor(s), request: &InternalExecutionRequest{InternalOwner: parent}, lifetime: executionLifetime{cancel: cancel, done: make(chan struct{})}}, cancel
}
