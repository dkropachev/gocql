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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package transport

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type recordingFrameHeaderObserver struct {
	t      *testing.T
	mu     sync.Mutex
	frames []ObservedFrameHeader
}

func (r *recordingFrameHeaderObserver) ObserveFrameHeader(ctx context.Context, frm ObservedFrameHeader) {
	r.mu.Lock()
	r.frames = append(r.frames, frm)
	r.mu.Unlock()
}

func (r *recordingFrameHeaderObserver) getFrames() []ObservedFrameHeader {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.frames
}

func newCallTable() *InternalCallTable { return &InternalCallTable{} }

func TestConn(t *testing.T) {
	t.Run("FramerFlags", func(t *testing.T) {
		comp := testMockedCompressor{}

		for _, version := range []byte{protocol.ProtoVersion4, protocol.ProtoVersion5, protocol.ProtoVersion5 | protocol.ProtoDirectionMask} {
			for _, compressor := range []protocol.Compressor{nil, comp} {
				if got := protocol.DefaultFramerFlags(compressor, version); got&frm.FlagBetaProtocol != 0 {
					t.Errorf("defaultFramerFlags(%v, 0x%02x) set FlagBetaProtocol", compressor != nil, version)
				}
				if got := protocol.NewFramer(compressor, version).Flags; got&frm.FlagBetaProtocol != 0 {
					t.Errorf("newFramer(%v, 0x%02x) set FlagBetaProtocol", compressor != nil, version)
				}
			}
		}

		// initCache is what the connection actually uses once the handshake completed.
		c := &Conn{InternalVersion: protocol.ProtoVersion5, InternalCompressor: comp, InternalLogger: slog.Default()}
		c.InternalInitFramerCache()
		if c.InternalFramers.InternalDefaults.InternalFlags&frm.FlagBetaProtocol != 0 {
			t.Error("initFramerCache(v5) seeded FlagBetaProtocol")
		}
	})
	// Checks that an empty host ID is not cached,
	// so the ID filled in later is observed, and that the cached ID is returned
	// afterwards without reading the host.
	t.Run("HostID", func(t *testing.T) {
		host := new(implEndpoint.HostInfo)
		c := &Conn{InternalHost: host}
		if id := c.InternalHostID(); !id.IsEmpty() {
			t.Fatalf("got %v for a host without an ID", id)
		}
		want := cqltype.UUID{15: 7}
		data := implEndpoint.HostInfoData(host)
		data.InternalMu.Lock()
		data.InternalHostId = want
		data.InternalMu.Unlock()
		if id := c.InternalHostID(); id != want {
			t.Fatalf("got %v, want %v", id, want)
		}
		data.InternalMu.Lock()
		data.InternalHostId = cqltype.UUID{}
		data.InternalMu.Unlock()
		if id := c.InternalHostID(); id != want {
			t.Fatalf("cached ID not used: got %v", id)
		}
	})
	// A validated limit is an explicit setting, including values above the default.
	t.Run("ProtocolLimits", func(t *testing.T) {
		for _, limit := range []int64{1024, 256 << 20, (256 << 20) + 1, 512 << 20, 1<<31 - 1} {
			c := &Conn{InternalCfg: &ConnConfig{TypedLimits: connectivity.ProtocolLimits{MaxFrameBytes: limit, MaxDecompressedBytes: limit}}}
			actual := c.typedProtocolLimits()
			if actual.MaxFrameBytes != limit || actual.MaxDecompressedBytes != limit {
				t.Errorf("configured limit %d became frame=%d decompressed=%d", limit, actual.MaxFrameBytes, actual.MaxDecompressedBytes)
			}
		}
	})
	t.Run("BuildTypedRequest", func(t *testing.T) {
		for _, limit := range []int64{(256 << 20) + 1, math.MaxInt32} {
			// One repeated MiB backing gives a large logical frame without a large
			// allocation. Values and headers retain the same immutable ranges.
			c := &Conn{InternalVersion: 4, InternalCfg: &ConnConfig{TypedLimits: connectivity.ProtocolLimits{MaxFrameBytes: limit, MaxDecompressedBytes: limit}}}
			// QUERY body: four-byte CQL length, one-byte CQL, consistency, flags,
			// value count and the value's four-byte length.
			payload := limit - 14
			var length [4]byte
			binary.BigEndian.PutUint32(length[:], uint32(payload))
			spans := []wire.Span{wire.Borrow(length[:])}
			block := make([]byte, 1<<20)
			for remaining := payload; remaining > 0; {
				n := min(remaining, int64(len(block)))
				spans = append(spans, wire.Borrow(block[:int(n)]))
				remaining -= n
			}
			values, err := wire.NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			request := &model.AttemptRequest{Statement: "Q", Values: values, ValueCount: 1}
			packet, err := c.InternalBuildTypedRequest(request, 0)
			if err != nil {
				t.Fatal(limit, err)
			}
			if packet.Len() != limit+frm.HeaderSize {
				t.Fatalf("logical packet length %d, want %d", packet.Len(), limit+frm.HeaderSize)
			}
			header := make([]byte, frm.HeaderSize)
			prefix, _ := packet.Slice(0, frm.HeaderSize)
			prefix.CopyTo(header)
			if got := int64(binary.BigEndian.Uint32(header[5:])); got != limit {
				t.Fatal(got, limit)
			}
			c.InternalCfg.TypedLimits.MaxFrameBytes--
			if _, err := c.InternalBuildTypedRequest(request, 0); err == nil {
				t.Fatal("request above configured bound accepted")
			}
		}
	})
	t.Run("HandshakeFramers", func(t *testing.T) {
		c := &Conn{InternalVersion: 4, InternalCfg: &ConnConfig{TypedLimits: connectivity.ProtocolLimits{MaxFrameBytes: 512 << 20, MaxDecompressedBytes: 511 << 20}}}
		for _, f := range []*protocol.Framer{c.InternalGetReadFramer(), c.InternalGetWriteFramer()} {
			if f.MaxFrameBytes != 512<<20 || f.MaxDecompressedBytes != 511<<20 {
				t.Fatal("handshake framer lost limits", f.MaxFrameBytes, f.MaxDecompressedBytes)
			}
		}
	})
}
