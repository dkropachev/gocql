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
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

// Segment builders for the tests in this package, which construct transport
// segments to feed the connection's receive path (segment_reassembly_test.go,
// conn_test.go) or to check what the send path produced (frame_test.go).
//
// The codec and its own tests live in internal/segment. What is left here is the
// bridge: these take this package's Compressor, so a test can pass the same
// compressor it configured a driverConn or framer with, and narrow it the way the
// production callers do.
//
// Each builds or reads one whole segment in a single call, with buffers of its own.
// No production caller works that way — the receive loop splits header and payload
// across two phases to re-arm its read deadline, and the send path encodes straight
// into the framer's wire buffer — so these model an allocation contract the driver
// deliberately does not follow.

// readUncompressedSegment reads a full uncompressed segment (header + payload) in
// one call, into a payload buffer of its own.
func readUncompressedSegment(r io.Reader) ([]byte, bool, error) {
	var scratch segment.Scratch

	h, err := segment.ReadUncompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := segment.ReadUncompressedPayload(r, h, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// readCompressedSegment reads a full compressed segment (header + payload) in one
// call, into payload buffers of its own.
func readCompressedSegment(r io.Reader, compressor protocol.Compressor) ([]byte, bool, error) {
	segComp, err := protocol.AsSegmentCompressor(compressor)
	if err != nil {
		return nil, false, err
	}

	var scratch segment.Scratch

	h, err := segment.ReadCompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := segment.ReadCompressedPayload(r, h, segComp, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// newUncompressedSegment returns payload as a standalone uncompressed segment.
func newUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	return segment.AppendUncompressed(nil, payload, isSelfContained)
}

// newCompressedSegment returns payload as a standalone compressed segment.
func newCompressedSegment(payload []byte, isSelfContained bool, compressor protocol.Compressor) ([]byte, error) {
	segComp, err := protocol.AsSegmentCompressor(compressor)
	if err != nil {
		return nil, err
	}
	return segment.AppendCompressed(nil, payload, isSelfContained, segComp)
}

// testSegmentCompressor is a Compressor that also supports v5 segment compression,
// passing bytes through unchanged.
//
// Declared here rather than reusing frame_test.go's testMockedCompressor because this
// file carries no build tag and that one is behind //go:build unit -- a helper taken
// from a tagged file compiles under `go test -tags unit` and nowhere else.
type testSegmentCompressor struct{}

func (testSegmentCompressor) Name() string                       { return "test-segment" }
func (testSegmentCompressor) Encode(data []byte) ([]byte, error) { return data, nil }
func (testSegmentCompressor) Decode(data []byte) ([]byte, error) { return data, nil }
func (testSegmentCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (testSegmentCompressor) AppendDecompressed(dst, src []byte, decompressedLength uint32) ([]byte, error) {
	return append(dst, src...), nil
}

func TestSegment(t *testing.T) {
	t.Run("ReadContinuationSegment", func(t *testing.T) {
		t.Run("ReturnsPayload", func(t *testing.T) {
			seg := mustUncompressedSegment(t, []byte("hello"), false)
			c := &Conn{InternalR: newSegmentReader(seg)}

			payload, err := c.readContinuationSegment()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := string(payload); got != "hello" {
				t.Fatalf("got %q, want %q", got, "hello")
			}
		})
		t.Run("RejectsSelfContained", func(t *testing.T) {
			seg := mustUncompressedSegment(t, []byte("hello"), true)
			c := &Conn{InternalR: newSegmentReader(seg)}

			_, err := c.readContinuationSegment()
			if err == nil || !strings.Contains(err.Error(), "expected a continuation") {
				t.Fatalf("expected self-contained rejection, got %v", err)
			}
		})
		t.Run("RejectsEmptyPayload", func(t *testing.T) {
			seg := mustUncompressedSegment(t, nil, false)
			c := &Conn{InternalR: newSegmentReader(seg)}

			_, err := c.readContinuationSegment()
			if err == nil || !strings.Contains(err.Error(), "no progress") {
				t.Fatalf("expected no-progress rejection, got %v", err)
			}
		})
	})

	t.Run("RecvSplitFrame", func(t *testing.T) {
		// Pins that continuation segments
		// carrying more bytes than the CQL frame header declared are rejected, rather
		// than appended past the size the reassembly buffer was allocated for.
		t.Run("RejectsOverlongStream", func(t *testing.T) {
			// A header declaring a 4-byte body, so the whole frame is headSize+4 bytes,
			// followed by a continuation segment carrying 8 body bytes.
			header := make([]byte, protocol.HeadSize)
			header[0] = protocol.ProtoVersion5 | protocol.ProtoDirectionMask
			header[8] = 4

			seg := mustUncompressedSegment(t, []byte("01234567"), false)
			c := &Conn{InternalR: newSegmentReader(seg)}

			err := c.recvSplitFrame(context.Background(), header, time.Time{}, time.Time{})
			if err == nil || !strings.Contains(err.Error(), "exceeds its declared length") {
				t.Fatalf("expected over-long rejection, got %v", err)
			}
		})
		// This test retains its original identity
		// and memory bound, translated to the typed transport: one final receive buffer
		// per segment, with no additional frame-sized reassembly or legacy-framer copy.
		t.Run("AllocatesOneFrameBuffer", func(t *testing.T) {
			const bodyLen = 3 * segment.MaxPayloadSize
			const runs = 4
			body := typedTestRowsBody(bytes.Repeat([]byte{0x5A}, bodyLen))
			frame := typedTestResponse(t, protocol.ProtoVersion5, 1, body)
			var stream []byte
			for src := frame; len(src) > 0; {
				n := min(len(src), segment.MaxPayloadSize)
				stream = append(stream, mustUncompressedSegment(t, src[:n], false)...)
				src = src[n:]
			}
			reader := &typedSegmentAllocationReader{segmentReader: newSegmentReader(bytes.Repeat(stream, runs))}
			c := &Conn{InternalVersion: protocol.ProtoVersion5, InternalR: reader, InternalStreams: streams.New(), InternalCalls: newCallTable(), InternalLogger: &logging.InternalDefaultLogger{}}
			c.InternalInitFramerCache()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for i := 0; i < runs; i++ {
				// A buffered receiver replaces the execution goroutine so process-wide
				// allocation accounting measures the receive path without scheduling noise.
				call := &InternalCallReq{typed: true, InternalStreamID: 1, InternalResp: make(chan InternalCallResp, 1)}
				c.InternalCalls.Set(1, call)
				reader.payloads = nil
				if err := c.InternalRecvSegment(context.Background()); err != nil {
					t.Fatalf("run %d: %v", i, err)
				}
				response := <-call.InternalResp
				if response.InternalErr != nil {
					t.Fatal(response.InternalErr)
				}
				page := response.page
				if page == nil || page.RowCount != 1 {
					t.Fatal("typed response was not delivered")
				}
				count := 0
				err := page.Rows.Each(func(span wire.Span) error {
					data := span.Bytes()
					matched := false
					// Match the actual socket Read destination, not a buffer constructed by
					// the parser after receiving: every row span must borrow a final segment.
					for _, received := range reader.payloads {
						for offset := 0; offset+len(data) <= len(received); offset++ {
							if &data[0] == &received[offset] {
								matched = true
								break
							}
						}
						if matched {
							break
						}
					}
					if !matched {
						t.Error("row payload was copied out of its receive segment")
					}
					count++
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if count < 3 {
					t.Fatal("split response was flattened into a frame buffer")
				}
				page.Owner.Release()
			}
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc
			if limit := uint64(runs) * uint64(len(frame)) * 3 / 2; allocated > limit {
				t.Errorf("receiving %d frames of %d bytes allocated %d bytes, want at most %d", runs, len(frame), allocated, limit)
			}
		})
		// Drives recvSplitFrame with a CQL
		// frame header declaring a body length beyond frm.MaxFrameSize. The declared
		// length is rejected before any large allocation and before processFrame.
		t.Run("RejectsOversizedLength", func(t *testing.T) {
			// Build a 9-byte CQL frame header (v5 response) whose length field is
			// frm.MaxFrameSize+1, then wrap it in a single non-self-contained segment.
			header := make([]byte, 9)
			header[0] = protocol.ProtoVersion5 | protocol.ProtoDirectionMask // version (response)
			header[1] = 0                                                    // flags
			// header[2:4] stream, header[4] opcode left zero
			oversized := uint32(frm.MaxFrameSize + 1)
			header[5] = byte(oversized >> 24)
			header[6] = byte(oversized >> 16)
			header[7] = byte(oversized >> 8)
			header[8] = byte(oversized)

			seg := mustUncompressedSegment(t, header, false)
			c := &Conn{InternalR: newSegmentReader(seg)}

			err := c.recvSplitFrame(context.Background(), nil, time.Time{}, time.Time{})
			if err == nil || !strings.Contains(err.Error(), "invalid frame body length") {
				t.Fatalf("expected oversized-length rejection, got %v", err)
			}
		})
		// Ensures the header-accumulation
		// loop terminates (with an error) when the peer stops sending before even the
		// 9-byte CQL header is complete, rather than looping forever.
		t.Run("RejectsTruncatedHeader", func(t *testing.T) {
			// A single 4-byte continuation segment, then EOF: fewer than the 9 header
			// bytes recvSplitFrame needs.
			seg := mustUncompressedSegment(t, []byte("abcd"), false)
			c := &Conn{InternalR: newSegmentReader(seg)}

			err := c.recvSplitFrame(context.Background(), nil, time.Time{}, time.Time{})
			if err == nil {
				t.Fatalf("expected error on truncated header stream, got nil")
			}
			if !bytes.Contains([]byte(err.Error()), []byte("continuation segment header")) &&
				err != io.EOF {
				t.Fatalf("expected read failure after truncation, got %v", err)
			}
		})
	})
	// Pins that a frame header inside
	// a self-contained segment cannot declare a body the segment does not carry.
	//
	// A self-contained segment holds only whole frames, so such a header is a
	// framing violation — but readFrame acts on the declared length before it
	// discovers the short read, and the io.ErrUnexpectedEOF that follows is not a
	// net.Error, so processFrameSource would keep it per-request and leave the
	// connection up for the peer to do it again. A ~20-byte segment would buy a
	// frm.MaxFrameSize allocation, repeatable.
	t.Run("ProcessAllFramesInSegment", func(t *testing.T) {
		newConn := func(stream []byte) *Conn {
			c := &Conn{
				InternalR:       newSegmentReader(stream),
				InternalStreams: streams.New(),
				InternalLogger:  &logging.InternalDefaultLogger{},
			}
			c.InternalInitFramerCache()
			return c
		}

		// Well below frm.MaxFrameSize, but far enough above the segment that the budget
		// below cannot be met by accident if the bound is removed.
		const declared = 32 << 20

		t.Run("rejects a header longer than the whole segment", func(t *testing.T) {
			seg := mustUncompressedSegment(t, v5ReadyFrame(declared, nil), true)
			c := newConn(seg)

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := c.InternalRecvSegment(context.Background())
			runtime.ReadMemStats(&after)

			if err == nil || !strings.Contains(err.Error(), "remain in the self-contained segment") {
				t.Fatalf("expected the declared length to be bounded by the segment, got %v", err)
			}
			// The error alone does not prove much: the unbounded path reaches the same
			// failure, just after allocating what the peer asked for.
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > declared/8 {
				t.Errorf("rejecting the frame allocated %d bytes, want well under the %d it declared", allocated, declared)
			}
		})

		t.Run("bounds each frame by what is left, not by the segment", func(t *testing.T) {
			// A complete frame first, so the second header is checked against the
			// remainder rather than the segment's original length.
			payload := v5ReadyFrame(0, nil)
			payload = append(payload, v5ReadyFrame(declared, nil)...)
			c := newConn(mustUncompressedSegment(t, payload, true))

			err := c.InternalRecvSegment(context.Background())
			if err == nil || !strings.Contains(err.Error(), "remain in the self-contained segment") {
				t.Fatalf("expected the second frame to be bounded by the remaining bytes, got %v", err)
			}
		})

		t.Run("accepts a frame that exactly fills the segment", func(t *testing.T) {
			body := bytes.Repeat([]byte{0x5A}, 64)
			c := newConn(mustUncompressedSegment(t, v5ReadyFrame(len(body), body), true))

			if err := c.InternalRecvSegment(context.Background()); err != nil {
				t.Fatalf("a frame whose body ends exactly at the segment boundary must be accepted, got %v", err)
			}
		})
	})

	t.Run("RecvSegment", func(t *testing.T) {
		// Pins FrameHeaderObserver's
		// documented contract on v5: Start and End bracket the header coming off the
		// network, not the parse.
		//
		// On v5 the whole segment is read before any CQL header is looked at, so timing
		// the header read where it happens measures a bytes.Reader — the window collapses
		// to nanoseconds and the network wait, which is the thing worth observing, is
		// reported by nobody.
		t.Run("ObservesHeaderOverNetworkRead", func(t *testing.T) {
			// Long enough that an in-memory parse cannot be mistaken for it, short enough
			// not to slow the suite. Only ever asserted as a lower bound.
			const stall = 30 * time.Millisecond

			newConn := func(r InternalConnReadSource, observer *recordingFrameHeaderObserver) *Conn {
				c := &Conn{
					InternalR:             r,
					InternalStreams:       streams.New(),
					InternalLogger:        &logging.InternalDefaultLogger{},
					InternalFrameObserver: observer,
				}
				c.InternalInitFramerCache()
				return c
			}

			t.Run("self-contained segment", func(t *testing.T) {
				seg := mustUncompressedSegment(t, v5ReadyFrame(0, nil), true)
				observer := &recordingFrameHeaderObserver{t: t}
				// Stall before the very first byte: the whole segment read is inside the
				// window the observer should report.
				c := newConn(&slowSegmentReader{segmentReader: newSegmentReader(seg), delay: stall}, observer)

				if err := c.InternalRecvSegment(context.Background()); err != nil {
					t.Fatalf("recvSegment: %v", err)
				}

				frames := observer.getFrames()
				if len(frames) != 1 {
					t.Fatalf("expected 1 observed header, got %d", len(frames))
				}
				if got := frames[0].End.Sub(frames[0].Start); got < stall {
					t.Errorf("observed header window is %v, want at least the %v the segment read stalled: "+
						"Start/End are timing the parse, not the network read", got, stall)
				}
			})

			t.Run("header split across segments", func(t *testing.T) {
				// Two bytes of the CQL header in the first segment, the rest plus the body
				// in the second, so the header is only complete after a second network read.
				frame := v5ReadyFrame(0, nil)
				first := mustUncompressedSegment(t, frame[:2], false)
				stream := append(append([]byte{}, first...), mustUncompressedSegment(t, frame[2:], false)...)

				observer := &recordingFrameHeaderObserver{t: t}
				// Stall on the second segment only, so the window can only cover it if
				// recvSplitFrame extended End past the first.
				c := newConn(&slowSegmentReader{
					segmentReader: newSegmentReader(stream),
					delayFrom:     int64(len(first)),
					delay:         stall,
				}, observer)

				if err := c.InternalRecvSegment(context.Background()); err != nil {
					t.Fatalf("recvSegment: %v", err)
				}

				frames := observer.getFrames()
				if len(frames) != 1 {
					t.Fatalf("expected 1 observed header, got %d", len(frames))
				}
				if got := frames[0].End.Sub(frames[0].Start); got < stall {
					t.Errorf("observed header window is %v, want at least the %v the second segment stalled: "+
						"End was not extended to when the header finished arriving", got, stall)
				}
			})
		})
		// Drives the connection's receive path with a
		// compressor attached, which nothing else here does.
		//
		// The header reader and the payload reader take their layout from the same field, and
		// this is what holds them together: a compressed header is eight bytes where an
		// uncompressed one is six, so a connection that resolved its compressor late -- or
		// read the layout from a second source -- reads the header at the wrong width and fails
		// its CRC24 rather than mis-framing the rest of the stream quietly.
		t.Run("ReadsCompressedSegment", func(t *testing.T) {
			const payload = "a compressed self-contained segment"

			seg, err := newCompressedSegment([]byte(payload), true, invertingCompressor{})
			if err != nil {
				t.Fatalf("newCompressedSegment: %v", err)
			}

			c := &Conn{
				InternalR:          newSegmentReader(seg),
				InternalVersion:    protocol.ProtoVersion5,
				InternalCompressor: invertingCompressor{},
			}
			if err := c.resolveSegmentCompressor(); err != nil {
				t.Fatalf("resolveSegmentCompressor: %v", err)
			}

			hdr, err := c.InternalReadFirstSegmentHeader()
			if err != nil {
				t.Fatalf("readFirstSegmentHeader: %v", err)
			}
			if !hdr.IsSelfContained {
				t.Error("a self-contained segment was read as a continuation")
			}

			got, err := c.readSegmentPayload(hdr)
			if err != nil {
				t.Fatalf("readSegmentPayload: %v", err)
			}
			if string(got) != payload {
				t.Errorf("payload = %q, want %q", got, payload)
			}
		})
	})
	// Covers the narrowing every v5 segment read and write goes
	// through.
	//
	// The nil case is the one worth pinning: both callers hand it whatever compressor the
	// connection was configured with, nil included, and rely on nil narrowing to a nil
	// segment.Compressor rather than to an error -- that is how internal/segment spells
	// "the uncompressed layout". Returning an error for it instead would break every
	// uncompressed v5 connection.
	t.Run("AsSegmentCompressor", func(t *testing.T) {
		t.Run("nil is the uncompressed layout", func(t *testing.T) {
			comp, err := protocol.AsSegmentCompressor(nil)
			if err != nil {
				t.Fatalf("asSegmentCompressor(nil) returned %v, want no error", err)
			}
			if comp != nil {
				t.Errorf("asSegmentCompressor(nil) = %#v, want a nil segment.Compressor", comp)
			}
		})

		t.Run("a segment compressor narrows to itself", func(t *testing.T) {
			comp, err := protocol.AsSegmentCompressor(testSegmentCompressor{})
			if err != nil {
				t.Fatalf("asSegmentCompressor returned %v, want no error", err)
			}
			if _, ok := comp.(testSegmentCompressor); !ok {
				t.Errorf("asSegmentCompressor returned %T, want the compressor it was given", comp)
			}
		})

		t.Run("a frame-only compressor is refused by name", func(t *testing.T) {
			comp, err := protocol.AsSegmentCompressor(wirecompress.SnappyCompressor{})
			if err == nil {
				t.Fatal("asSegmentCompressor accepted a compressor with no segment support")
			}
			if comp != nil {
				t.Errorf("asSegmentCompressor returned %#v alongside its error, want nil", comp)
			}
			// Naming the offending compressor is the whole reason this narrowing lives in
			// this package rather than in internal/segment, which needs only the two
			// Append methods and so cannot reach Name.
			if !strings.Contains(err.Error(), "snappy") {
				t.Errorf("error %q does not name the compressor it rejected", err)
			}
		})
	})
	// Covers the one place the narrowing now runs.
	//
	// It happens during the handshake rather than once per segment, which puts a version
	// gate on it that the per-segment call never needed: below v5 the segment codec is
	// unreachable, and a compressor without segment support is an ordinary configuration
	// there -- the driver's own integration suite runs protocol v4 with Snappy. Refusing it
	// here would fail every one of those handshakes.
	t.Run("ResolveSegmentCompressor", func(t *testing.T) {
		t.Run("v5 narrows the compressor once", func(t *testing.T) {
			c := &Conn{InternalVersion: protocol.ProtoVersion5, InternalCompressor: testSegmentCompressor{}}
			if err := c.resolveSegmentCompressor(); err != nil {
				t.Fatalf("resolveSegmentCompressor: %v", err)
			}
			if _, ok := c.InternalSegCompressor.(testSegmentCompressor); !ok {
				t.Errorf("segCompressor = %#v, want the compressor the connection was given", c.InternalSegCompressor)
			}
		})

		t.Run("v4 leaves a frame-only compressor alone", func(t *testing.T) {
			c := &Conn{InternalVersion: protocol.ProtoVersion4, InternalCompressor: wirecompress.SnappyCompressor{}}
			if err := c.resolveSegmentCompressor(); err != nil {
				t.Fatalf("a protocol v4 connection with a frame compressor was refused: %v", err)
			}
			if c.InternalSegCompressor != nil {
				t.Errorf("segCompressor = %#v, want nil: v4 never reaches the segment codec", c.InternalSegCompressor)
			}
		})

		t.Run("v5 refuses a frame-only compressor by name", func(t *testing.T) {
			c := &Conn{InternalVersion: protocol.ProtoVersion5, InternalCompressor: wirecompress.SnappyCompressor{}}
			err := c.resolveSegmentCompressor()
			if err == nil {
				t.Fatal("a v5 connection with a frame-only compressor was accepted")
			}
			if !strings.Contains(err.Error(), "snappy") {
				t.Errorf("error %q does not name the compressor it rejected", err)
			}
			if c.InternalSegCompressor != nil {
				t.Errorf("segCompressor = %#v alongside the error, want nil", c.InternalSegCompressor)
			}
		})
	})
}
