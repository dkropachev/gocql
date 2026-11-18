/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/pool"
)

func newTestSessionForTableMetadata(ctrl *schemaDataMock) *Core {
	s := newSchemaEventTestSessionWithMock(ctrl)
	SessionData(s).isInitialized = true
	return s
}

// shrinkingRetryPolicy allows one attempt for its first `allowed`
// GetMaxRetries calls, then none.
type shrinkingRetryPolicy struct {
	pool.ConstantReconnectionPolicy
	allowed int32
	calls   atomic.Int32
}

func (p *shrinkingRetryPolicy) GetMaxRetries() int {
	if p.calls.Add(1) <= p.allowed {
		return 1
	}
	return 0
}
