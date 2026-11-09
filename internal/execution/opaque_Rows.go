// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	"sync"
)

type RowsStorage[R any] struct {
	nextErr         error
	terminalErr     error
	InternalCtx     context.Context
	decoder         RowDecoder[R]
	err             error
	prefetchJobs    chan InternalPrefetchJob
	pendingRelease  func()
	plans           *InternalResultPlanCache[R]
	future          *InternalPageFuture
	cancel          context.CancelFunc
	session         *implCluster.Core
	InternalRequest *InternalExecutionRequest
	InternalPage    *Page[R]
	InternalRelease func()
	// execution is finished and released when the rows close; pendingExecution
	// carries that release past the unlock, like pendingRelease.
	execution, pendingExecution *InternalLogicalExecution
	cursor                      queryCursor
	InternalHistory             pagingHistory
	info                        Result
	mu                          sync.Mutex
	rejectColumns               bool
	closed                      bool
	valid                       bool
}

func RowsData[R any](v Rows[R]) *RowsStorage[R] { return &v.impl.privateStorage }

// InternalNewRows returns a handle over new, empty iterator storage, for tests
// that assemble an iterator directly.
func InternalNewRows[R any]() Rows[R] { return newRowsHandle(new(rowsImpl[R])) }
