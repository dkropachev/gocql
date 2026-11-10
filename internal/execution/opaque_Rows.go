// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"

	"sync"
)

type RowsStorage[R any] struct {
	nextErr        error
	terminalErr    error
	ctx            context.Context
	decoder        codec.RowDecoder[R]
	err            error
	prefetchJobs   chan prefetchJob
	pendingRelease func()
	plans          *InternalResultPlanCache[R]
	future         *InternalPageFuture
	cancel         context.CancelFunc
	session        *implCluster.Core
	request        *InternalExecutionRequest
	page           *Page[R]
	release        func()
	// execution is finished and released when the rows close; pendingExecution
	// carries that release past the unlock, like pendingRelease.
	execution, pendingExecution *logicalExecution
	cursor                      queryCursor
	history                     paging.InternalPagingHistory
	info                        model.Result
	mu                          sync.Mutex
	rejectColumns               bool
	closed                      bool
	valid                       bool
}

func RowsData[R any](v Rows[R]) *RowsStorage[R] { return &v.impl.privateStorage }

// newRows returns a handle over new, empty iterator storage, for tests
// that assemble an iterator directly.
func newRows[R any]() Rows[R] { return newRowsHandle(new(rowsImpl[R])) }
