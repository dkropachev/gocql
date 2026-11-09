// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageStorage[R any] struct {
	InternalCtx       context.Context
	InternalPlan      RowPlan[R]
	err               error
	pendingRelease    func()
	destination       *R
	cancel            context.CancelFunc
	session           *implCluster.Core
	InternalResponse  *responsePage
	InternalRelease   func()
	cursor            queryCursor
	reader            RowReader
	InternalCursor    wire.Cursor
	InternalIndex     int32
	valid             bool
	closed            bool
	InternalExhausted bool
}

func PageData[R any](v *Page[R]) *PageStorage[R] { return &v.privateStorage }
