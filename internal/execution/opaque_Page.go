// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageStorage[R any] struct {
	InternalCtx       context.Context
	InternalPlan      codec.RowPlan[R]
	err               error
	pendingRelease    func()
	destination       *R
	cancel            context.CancelFunc
	session           *implCluster.Core
	InternalResponse  *transport.InternalResponsePage
	InternalRelease   func()
	cursor            queryCursor
	reader            codec.RowReader
	InternalCursor    wire.Cursor
	InternalIndex     int32
	valid             bool
	closed            bool
	InternalExhausted bool
}

func PageData[R any](v *Page[R]) *PageStorage[R] { return &v.privateStorage }
