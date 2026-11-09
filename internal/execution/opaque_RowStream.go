// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package execution

import (
	"context"

	"sync"
	"sync/atomic"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type RowStreamStorage[R any] struct {
	InternalCtx         context.Context
	InternalDecoder     StreamRowDecoder[R]
	plan                StreamRowPlan[R]
	err                 error
	bufferedPage        *responsePage
	bufferedBody        *wire.Stream
	bufferedReader      *codec.StreamRowReader
	bufferedPlans       *streamPlanCache[R]
	attemptMetadata     *streamResponse
	closeDone           chan struct{}
	retryTarget         *Target
	InternalCancel      context.CancelFunc
	stop                func() bool
	InternalSession     *implCluster.Core
	InternalRequest     *InternalExecutionRequest
	response            *streamResponse
	InternalReservation *exclusiveConn
	prepared            *InternalHostPrepared
	pageCancel          context.CancelFunc
	cursor              queryCursor
	columns             Columns
	InternalHistory     pagingHistory
	info                StreamInfo
	limits              StreamLimits
	InternalActive      sync.Mutex
	mu                  sync.Mutex
	bufferedIndex       int32
	advancing           atomic.Bool
	InternalClosing     atomic.Bool
	pageBuffered        bool
	terminal            bool
	cleaned             bool
}

func RowStreamData[R any](v *RowStream[R]) *RowStreamStorage[R] { return &v.privateStorage }
