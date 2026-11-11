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

package gocql

import (
	"sync/atomic"
	"testing"

	implSession "github.com/scylladb/gocql/v2/internal/session"

	"github.com/scylladb/gocql/v2/cql"
)

type countingDecoder struct {
	calls *atomic.Int32
	hook  func()
}

func (d countingDecoder) Compile(c cql.Columns) (cql.RowPlan[cql.Bytes], error) {
	d.calls.Add(1)
	if d.hook != nil {
		d.hook()
	}
	return cql.OneColumn(cql.BlobCodec).Compile(c)
}
func (d countingDecoder) Clone(v cql.Bytes) cql.Bytes { return v.Clone() }

func testPreparedState(t *testing.T, s *testSession, columns cql.Columns) (*preparedState, *hostPrepared) {
	t.Helper()
	tracker := runtimeFor(s).InternalSchemaTracker()
	dependencies := tracker.InternalDependencies("ks", cql.NewColumns(), columns)
	epoch := dependencies.InternalSnapshot()
	result := &typedResultMetadata{InternalColumns: columns, InternalEpoch: epoch, InternalTracker: tracker}
	hp := &hostPrepared{InternalId: []byte("id"), InternalMetadataID: []byte("meta"), InternalColumns: columns, InternalResult: result, InternalEpoch: epoch, InternalRevision: 1}
	state := &preparedState{InternalSession: (*implSession.Session)(s).Core(), InternalProtocol: 4, InternalStatement: "SELECT value FROM ks.tbl", InternalKeyspace: "ks", InternalParameters: cql.NewColumns(), InternalColumns: columns, InternalResult: result, InternalDependencies: dependencies, InternalInitialEpoch: epoch, InternalResultHost: cql.UUID{15: 1}, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}, InternalHosts: map[cql.UUID]*hostPrepared{{15: 1}: hp}}
	return state, hp
}
