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
	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/paging"

	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

func FuzzStreamExecutionBoundary(f *testing.F) {
	f.Add(runtimeRowsBody(nil, "value"), uint16(16))
	f.Add(runtimeRowsBody([]byte{}, ""), uint16(0))
	f.Add(runtimeRowsBody(nil), uint16(10))
	f.Fuzz(func(t *testing.T, body []byte, capacity uint16) {
		if len(body) > 64<<10 {
			return
		}
		frame := []byte{0x84, 0, 0, 0, 8}
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
		frame = append(frame, body...)
		memory := &memoryConn{source: bytes.NewReader(frame)}
		reservation := memoryReservation(4, nil, memory)
		s := func() *cluster.Core {
			opaqueStorageValue := new(cluster.Core)
			return opaqueStorageValue
		}()

		reservation.InternalConn.InternalHost = func() *endpoint.HostInfo {
			opaqueStorageValuex := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = cqltype.UUID{15: 1}
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalState.Store(endpoint.NodeUp)
			return opaqueStorageValuex
		}()
		reservation.InternalConn.InternalHooks = s.NewConnectionHooks()
		bound := runtimeQuery(t)
		defer bound.Close()
		ctx, cancel, request, err := InternalAdmitExecution(context.Background(), s, bound, CompiledOptions{})
		if err != nil {
			t.Fatal(err)
		}
		request.InternalOptions.InternalProfile.Speculation = nil
		cursor := func() *RowStream[[]byte] {
			opaqueStorageValuexx := new(RowStream[[]byte])
			RowStreamData(opaqueStorageValuexx).ctx = ctx
			RowStreamData(opaqueStorageValuexx).cancel = cancel
			RowStreamData(opaqueStorageValuexx).session = s
			RowStreamData(opaqueStorageValuexx).request = request
			RowStreamData(opaqueStorageValuexx).decoder = codec.StreamOneColumn(codec.StreamBlob)
			RowStreamData(opaqueStorageValuexx).reservation = reservation
			RowStreamData(opaqueStorageValuexx).history = paging.InternalPagingHistory{InternalMaxCount: 4, InternalMaxBytes: 64}
			return opaqueStorageValuexx
		}()
		RowStreamData(cursor).active.Lock()
		err = cursor.loadPage()
		RowStreamData(cursor).active.Unlock()
		if err != nil {
			rowStreamFail(cursor, err)
			RowStreamData(cursor).active.Lock()
			rowStreamCleanup(cursor, false)
			RowStreamData(cursor).active.Unlock()
		} else {
			dst := make([]byte, 0, int(capacity))
			for i := 0; i < 4 && cursor.NextInto(&dst); i++ {
			}
			cursor.Close()
			before := append([]byte(nil), dst...)
			if cursor.NextInto(&dst) || !bytes.Equal(before, dst) {
				t.Fatal("closed cursor touched destination")
			}
		}
		bound.Close()
		if err := bound.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if cursor.Info().DeliveredRows > 4 {
			t.Fatal("unbounded delivery")
		}
	})
}
