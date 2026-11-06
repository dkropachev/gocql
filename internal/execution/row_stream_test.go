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
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func rowStreamSession(t *testing.T, proto byte, compressor implProtocol.Compressor) (*cluster.Session, *transport.Conn, net.Conn) {
	t.Helper()
	s, cs, ps := runtimeTestSession(t, 0)
	c, peer := typedTestConn(t, proto, compressor)
	c.InternalHost = func() *endpoint.HostInfo {
		opaqueStorageValue := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = cqltype.UUID{15: 1}
		implEndpoint.HostInfoData(opaqueStorageValue).InternalState.Store(endpoint.NodeUp)
		return opaqueStorageValue
	}()
	c.InternalHooks = cluster.InternalNewConnectionHooks(s)
	implCluster.SessionData(s).InternalHostSource.InternalHosts[c.InternalHost.HostID()] = c.InternalHost
	picker := implPool.InternalNewDefaultConnPicker(1)
	picker.Put(c)
	pool := &implPool.InternalHostConnPool{InternalHost: c.InternalHost, InternalConnPicker: picker, InternalRepairCtx: c.InternalCtx}
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)] = pool
	_ = cs
	_ = ps
	reader := c.InternalR.(*transport.InternalConnReader)
	reader.InternalConn = &testReadTargets{Conn: reader.InternalConn}
	go c.InternalServe(c.InternalCtx)
	return s, c, peer
}
func testResponse(t *testing.T, c *transport.Conn, stream int, body []byte) []byte {
	t.Helper()
	data := typedTestResponse(t, c.InternalVersion, stream, body)
	if c.InternalVersion != 5 {
		return data
	}
	var out bytes.Buffer
	for off := 0; off < len(data); {
		end := min(off+segment.MaxPayloadSize, len(data))
		chain, err := wire.NewChain(wire.Borrow(data[off:end]))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := segment.EncodeSpans(chain, len(data) <= segment.MaxPayloadSize, c.InternalSegCompressor != nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = encoded.Each(func(span wire.Span) error { out.Write(span.Bytes()); return nil })
		off = end
	}
	return out.Bytes()
}

// The custom plan blocks after Reset. Close/cancel must join that invocation;
// concurrent NextInto must never enter Reset or access its destination.
type gatedDecoder struct {
	entered chan struct{}
	leave   chan struct{}
	resets  atomic.Int32
}

func (d *gatedDecoder) Compile(cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	return d, nil
}
func (d *gatedDecoder) Reset(dst *[]byte) {
	d.resets.Add(1)
	close(d.entered)
	<-d.leave
	*dst = (*dst)[:0]
}
func (d *gatedDecoder) DecodeInto(r *codec.StreamRowReader, dst *[]byte) error {
	plan, err := codec.StreamBlob.Compile(cqltype.BlobType)
	if err != nil {
		return err
	}
	return codec.ReadStreamColumn(r, plan, dst)
}

// Raw bulk reads must target caller storage after at most a bounded prefix.
type testReadTargets struct {
	net.Conn
	mu      sync.Mutex
	targets [][]byte
}

func (c *testReadTargets) Read(p []byte) (int, error) {
	c.mu.Lock()
	c.targets = append(c.targets, p)
	c.mu.Unlock()
	return c.Conn.Read(p)
}

type countStreamDecoder struct{ compiles atomic.Int32 }

func (d *countStreamDecoder) Compile(columns cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	d.compiles.Add(1)
	return codec.StreamOneColumn(codec.StreamBlob).Compile(columns)
}
func errorExtensions(frame []byte) []byte {
	extension := make([]byte, 16)
	extension[15] = 7
	extension = binary.BigEndian.AppendUint16(extension, 1)
	extension = binary.BigEndian.AppendUint16(extension, 8)
	extension = append(extension, "terminal"...)
	extension = binary.BigEndian.AppendUint16(extension, 1)
	extension = binary.BigEndian.AppendUint16(extension, 6)
	extension = append(extension, "latest"...)
	extension = binary.BigEndian.AppendUint32(extension, 3)
	extension = append(extension, "new"...)
	result := append([]byte(nil), frame[:9]...)
	result[1] = 2 | 4 | 8
	binary.BigEndian.PutUint32(result[5:], uint32(len(extension)+len(frame)-9))
	result = append(result, extension...)
	result = append(result, frame[9:]...)
	return result
}
