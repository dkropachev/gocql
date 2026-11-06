// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/cluster"
)

func attachmentSession() (*cluster.Session, *InternalSessionRuntime) {
	s := new(cluster.Session)
	data := cluster.SessionData(s)
	data.InternalCtx, data.InternalCancel = context.WithCancel(context.Background())
	return s, InternalRuntimeFor(s)
}
func attachmentLogical(s *cluster.Session, parent *inputOwner) (*InternalLogicalExecution, context.CancelFunc) {
	_, cancel := context.WithCancel(context.Background())
	return &InternalLogicalExecution{session: s, request: &InternalExecutionRequest{InternalOwner: parent}, lifetime: InternalExecutionLifetime{cancel: cancel, done: make(chan struct{})}}, cancel
}
