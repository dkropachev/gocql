// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cqlerr"
)

// InternalExecutionContext returns parent bounded by the driver's timeout.
// When the timeout runs out, context.Cause reports cqlerr.ErrRequestTimeout,
// which tells the driver's budget apart from parent's own deadline.
func InternalExecutionContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeoutCause(parent, timeout, cqlerr.ErrRequestTimeout)
	}
	return context.WithCancel(parent)
}
