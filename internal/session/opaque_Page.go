// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageStorage[R any] struct {
	ctx            context.Context
	plan           codec.RowPlan[R]
	err            error
	pendingRelease func()
	destination    *R
	cancel         context.CancelFunc
	session        *implCluster.Core
	response       *transport.InternalResponsePage
	release        func()
	cursor         queryCursor
	reader         codec.RowReader
	InternalCursor wire.Cursor
	index          int32
	valid          bool
	closed         bool
	exhausted      bool
}
