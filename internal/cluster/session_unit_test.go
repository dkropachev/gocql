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

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func newTestSessionForTableMetadata(ctrl *schemaDataMock) *Core {
	s := newSchemaEventTestSessionWithMock(ctrl)
	SessionData(s).isInitialized = true
	return s
}

// testWarningFramer is a mock framerInterface that returns configurable warnings.
type testWarningFramer struct {
	warnings      []string
	customPayload map[string][]byte
	released      bool
}

func (f *testWarningFramer) ReadBytesInternal() ([]byte, error) { return nil, nil }
func (f *testWarningFramer) GetCustomPayload() map[string][]byte {
	return f.customPayload
}
func (f *testWarningFramer) GetHeaderWarnings() []string { return f.warnings }
func (f *testWarningFramer) Release()                    { f.released = true }

type staticConnPicker struct {
	conn *transport.Conn
}

func (p staticConnPicker) Put(*transport.Conn) error { return nil }
func (p staticConnPicker) Remove(*transport.Conn)    {}
func (p staticConnPicker) InFlight() int             { return 0 }
func (p staticConnPicker) Size() (int, int)          { return 1, 0 }
func (p staticConnPicker) Close()                    {}
func (p staticConnPicker) NextShard() (shardID, nrShards int) {
	return 0, 0
}
func (p staticConnPicker) GetConnectionCount() int       { return 1 }
func (p staticConnPicker) GetExcessConnectionCount() int { return 0 }
func (p staticConnPicker) GetShardCount() int            { return 0 }

type staticSelectedHost struct {
	host *endpoint.HostInfo
}

func (h staticSelectedHost) Info() *endpoint.HostInfo { return h.host }
func (h staticSelectedHost) Token() endpoint.Token    { return nil }
func (h staticSelectedHost) Mark(error)               {}

type consistencyRetryPolicy struct {
	consistency cqltype.Consistency
}

type synchronizedRetryPolicy struct {
	maxRetries int
	entered    atomic.Int32
	allEntered chan struct{}
}

type keyspaceCapturingQueryObserver struct{ keyspace string }

type keyspaceCapturingBatchObserver struct{ keyspace string }

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
