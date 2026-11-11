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

package session

import (
	"github.com/scylladb/gocql/v2/retry"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implModel "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func testPage(t *testing.T, values ...string) *Page[implCqltype.Bytes] {
	t.Helper()
	typ := codecTestMust(implCqltype.ScalarType(implCqltype.TypeBlob))
	columns := implCqltype.NewColumns(implCqltype.ColumnSpec{Name: "value", Type: typ})
	var body []byte
	for _, v := range values {
		body = binary.BigEndian.AppendUint32(body, uint32(len(v)))
		body = append(body, v...)
	}
	chain := typedTestChain(t, body)
	return codecTestMust(InternalNewPage(&transport.InternalResponsePage{ResponseData: implModel.ResponseData{Columns: columns, Rows: chain, RowCount: int32(len(values)), Owner: wire.NewLease(chain, nil), State: implModel.CompletePageState()}}, codec.OneColumn(codec.BlobCodec), nil))
}

type countingDecoder struct {
	calls *atomic.Int32
	hook  func()
}

func (d countingDecoder) Compile(c implCqltype.Columns) (codec.RowPlan[implCqltype.Bytes], error) {
	d.calls.Add(1)
	if d.hook != nil {
		d.hook()
	}
	return codec.OneColumn(codec.BlobCodec).Compile(c)
}
func (d countingDecoder) Clone(v implCqltype.Bytes) implCqltype.Bytes { return v.Clone() }
func preparedState(t *testing.T, s *cluster.Core, columns implCqltype.Columns) (*InternalPreparedState, *InternalHostPrepared) {
	t.Helper()
	tracker := InternalRuntimeFor(s).InternalSchemaTracker()
	dependencies := tracker.InternalDependencies("ks", implCqltype.NewColumns(), columns)
	epoch := dependencies.InternalSnapshot()
	result := &implModel.ResultSchemaIdentity{InternalColumns: columns, InternalEpoch: epoch, InternalTracker: tracker}
	hp := &InternalHostPrepared{InternalId: []byte("id"), InternalMetadataID: []byte("meta"), InternalColumns: columns, InternalResult: result, InternalEpoch: epoch, InternalRevision: 1}
	state := &InternalPreparedState{InternalSession: s, InternalProtocol: 4, InternalStatement: "SELECT value FROM ks.tbl", InternalKeyspace: "ks", InternalParameters: implCqltype.NewColumns(), InternalColumns: columns, InternalResult: result, InternalDependencies: dependencies, InternalInitialEpoch: epoch, InternalResultHost: implCqltype.UUID{15: 1}, InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl"}, InternalHosts: map[implCqltype.UUID]*InternalHostPrepared{{15: 1}: hp}}
	return state, hp
}
func preparedBody(id []byte, parameters, columns implCqltype.Columns, pk ...int) []byte {
	b := binary.BigEndian.AppendUint32(nil, 4)
	b = binary.BigEndian.AppendUint16(b, uint16(len(id)))
	b = append(b, id...)
	for part, c := range []implCqltype.Columns{parameters, columns} {
		b = binary.BigEndian.AppendUint32(b, 0)
		b = binary.BigEndian.AppendUint32(b, uint32(c.Len()))
		if part == 0 {
			b = binary.BigEndian.AppendUint32(b, uint32(len(pk)))
			for _, i := range pk {
				b = binary.BigEndian.AppendUint16(b, uint16(i))
			}
		}
		for _, col := range cqltype.ColumnSlice(c) {
			for _, text := range []string{col.Keyspace, col.Table, col.Name} {
				b = binary.BigEndian.AppendUint16(b, uint16(len(text)))
				b = append(b, text...)
			}
			b = binary.BigEndian.AppendUint16(b, uint16(col.Type.Kind()))
		}
	}
	return b
}
func readRequest(server net.Conn) (byte, int, []byte, error) {
	var h [9]byte
	if _, e := io.ReadFull(server, h[:]); e != nil {
		return 0, 0, nil, e
	}
	b := make([]byte, binary.BigEndian.Uint32(h[5:]))
	_, e := io.ReadFull(server, b)
	return h[4], int(binary.BigEndian.Uint16(h[2:4])), b, e
}
func errorResponse(t *testing.T, stream, code int, id []byte) []byte {
	body := binary.BigEndian.AppendUint32(nil, uint32(code))
	body = binary.BigEndian.AppendUint16(body, 4)
	body = append(body, "test"...)
	if code == protocol.ErrCodeUnprepared {
		body = binary.BigEndian.AppendUint16(body, uint16(len(id)))
		body = append(body, id...)
	}
	response := typedTestResponse(t, 4, stream, body)
	response[4] = 0
	return response
}

type retryOnce struct{}

func (retryOnce) Decide(e retry.Attempt) retry.Decision {
	if e.Number == 1 {
		return retry.Decision{Action: retry.Same}
	}
	return retry.Decision{}
}
