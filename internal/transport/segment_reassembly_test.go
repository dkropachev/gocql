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

package transport

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// segmentReader is a minimal connReadSource backed by an in-memory byte stream,
// used to drive the v5 segment reassembly path without a real socket.
type segmentReader struct {
	r *bytes.Reader
	// disarmed records the current disarm state. There is no deadline to disarm on
	// an in-memory stream, but implementing it is what keeps this reader usable as
	// driverConn.r — a reader that could not be disarmed would silently exercise the
	// receive path with the idle-wait handling switched off.
	disarmed bool
}

var _ InternalConnReadSource = (*segmentReader)(nil)

func newSegmentReader(b []byte) *segmentReader {
	return &segmentReader{r: bytes.NewReader(b)}
}

func (s *segmentReader) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *segmentReader) Close() error               { return nil }
func (s *segmentReader) RemoteAddr() net.Addr       { return nil }
func (s *segmentReader) SetTimeout(_ time.Duration) {}
func (s *segmentReader) GetTimeout() time.Duration  { return 0 }
func (s *segmentReader) InternalSetDisarm(v bool)   { s.disarmed = v }

// mustUncompressedSegment builds a single uncompressed transport segment
// carrying payload, failing the test on error.
func mustUncompressedSegment(t *testing.T, payload []byte, selfContained bool) []byte {
	t.Helper()
	seg, err := newUncompressedSegment(payload, selfContained)
	if err != nil {
		t.Fatalf("newUncompressedSegment: %v", err)
	}
	return seg
}

type typedSegmentAllocationReader struct {
	*segmentReader
	payloads [][]byte
}

func (r *typedSegmentAllocationReader) Read(p []byte) (int, error) {
	if len(p) > protocol.HeadSize {
		r.payloads = append(r.payloads, p)
	}
	return r.segmentReader.Read(p)
}

// v5ReadyFrame builds a v5 READY response frame on the event stream, declaring
// bodyLen body bytes but carrying only the bytes in body. With no session
// attached, processFrameSource parses such a frame and drops it, which is all
// these tests need from a well-formed one.
func v5ReadyFrame(bodyLen int, body []byte) []byte {
	frame := make([]byte, protocol.HeadSize)
	frame[0] = protocol.ProtoVersion5 | protocol.ProtoDirectionMask
	binary.BigEndian.PutUint16(frame[2:4], uint16(0xFFFF))
	frame[4] = byte(frm.OpReady)
	binary.BigEndian.PutUint32(frame[5:protocol.HeadSize], uint32(bodyLen))
	return append(frame, body...)
}

// slowSegmentReader stalls once, on the first read that reaches delayFrom, so a
// test can put a known lower bound on how long a chosen part of the receive path
// spent waiting on the network.
type slowSegmentReader struct {
	*segmentReader
	delayFrom int64
	delay     time.Duration
	stalled   bool
}

func (s *slowSegmentReader) Read(p []byte) (int, error) {
	if !s.stalled && s.r.Size()-int64(s.r.Len()) >= s.delayFrom {
		s.stalled = true
		time.Sleep(s.delay)
	}
	return s.segmentReader.Read(p)
}

// invertingCompressor is a segment compressor that actually changes the bytes it is
// given, unlike testSegmentCompressor's passthrough.
//
// That difference is the whole point of using it below: with a passthrough, a payload
// read through the uncompressed path comes back byte-identical to one read through the
// compressed path, so the test cannot tell which layout was used. Inverting makes the
// two paths distinguishable, which is what lets the assertion catch a connection whose
// header and payload readers disagree about the layout.
type invertingCompressor struct{}

func (invertingCompressor) Name() string { return "inverting" }

func invertBytes(dst, src []byte) []byte {
	for _, b := range src {
		dst = append(dst, b^0xFF)
	}
	return dst
}

func (invertingCompressor) Encode(data []byte) ([]byte, error) { return invertBytes(nil, data), nil }
func (invertingCompressor) Decode(data []byte) ([]byte, error) { return invertBytes(nil, data), nil }

func (invertingCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return invertBytes(dst, src), nil
}

func (invertingCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return invertBytes(dst, src), nil
}
