// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// recvWire wraps a v5 frame in one self-contained uncompressed segment.
func recvWire(t testing.TB, proto byte, frame []byte) []byte {
	if proto < 5 {
		return frame
	}
	out, err := newUncompressedSegment(frame, true)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// loopNetConn serves an endless repetition of one byte pattern.
type loopNetConn struct {
	data []byte
	off  int
}

func (f *loopNetConn) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		k := copy(p[n:], f.data[f.off:])
		n += k
		f.off = (f.off + k) % len(f.data)
	}
	return n, nil
}
func (f *loopNetConn) Write(p []byte) (int, error)      { return len(p), nil }
func (f *loopNetConn) Close() error                     { return nil }
func (f *loopNetConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (f *loopNetConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (f *loopNetConn) SetDeadline(time.Time) error      { return nil }
func (f *loopNetConn) SetReadDeadline(time.Time) error  { return nil }
func (f *loopNetConn) SetWriteDeadline(time.Time) error { return nil }

// recvLoop is a reader-side connection answering every call with frame.
type recvLoop struct {
	c   *Conn
	ctx context.Context
}

// newRecvLoop serves data, the wire bytes of one or more responses, in a loop.
func newRecvLoop(t testing.TB, proto byte, data []byte) *recvLoop {
	nc := &loopNetConn{data: data}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &Conn{InternalVersion: proto, InternalCtx: ctx, InternalCancel: cancel, InternalR: &InternalConnReader{InternalConn: nc, InternalR: bufio.NewReaderSize(nc, 8<<10)}, InternalCalls: InternalNewCallTable(), InternalStreams: streams.New(), InternalLogger: logging.Nop()}
	c.InternalCfg = new(ConnConfig)
	c.InternalCfg.InternalReceiveCache = new(wire.ReceiveCache)
	c.InternalInitFramerCache()
	return &recvLoop{c: c, ctx: ctx}
}

// one receives one typed response; the caller owns the returned page.
func (l *recvLoop) one(t testing.TB) *InternalResponsePage {
	call := getCallReq(0)
	call.typed = true
	if err := l.c.allocCall(call); err != nil {
		t.Fatal(err)
	}
	if err := l.c.InternalRecv(l.ctx, true); err != nil {
		t.Fatal(err)
	}
	resp := <-call.InternalResp
	if resp.InternalErr != nil {
		t.Fatal(resp.InternalErr)
	}
	call.finishExec()
	l.c.releaseStream(call)
	putCallReq(call)
	return resp.page
}

// BenchmarkRecv counts receive-path allocations per typed response, excluding
// call setup, for v4 and v5 (one uncompressed self-contained segment).
func BenchmarkRecv(b *testing.B) {
	for _, proto := range []byte{4, 5} {
		b.Run(map[byte]string{4: "v4", 5: "v5"}[proto], func(b *testing.B) {
			l := newRecvLoop(b, proto, recvWire(b, proto, invFrame(proto, 0, typedTestRowsBody(bytes.Repeat([]byte{'x'}, 8)))))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l.one(b).Owner.Release()
			}
		})
	}
}

// recvSplitWire wraps a v5 frame in non-self-contained segments of at most
// segment.MaxPayloadSize bytes.
func recvSplitWire(t testing.TB, frame []byte) []byte {
	var out []byte
	for len(frame) > 0 {
		n := min(len(frame), segment.MaxPayloadSize)
		var err error
		if out, err = segment.AppendUncompressed(out, frame[:n], false); err != nil {
			t.Fatal(err)
		}
		frame = frame[n:]
	}
	return out
}

// checkRecvPage fails unless page's single blob cell is size copies of fill.
func checkRecvPage(t *testing.T, page *InternalResponsePage, fill byte, size int) {
	t.Helper()
	if !page.Owner.Alive() {
		t.Fatal("retained page lease released")
	}
	rows := make([]byte, page.Rows.Len())
	if _, err := page.Rows.CopyTo(rows); err != nil {
		t.Fatal(err)
	}
	want := binary.BigEndian.AppendUint32(nil, uint32(size))
	want = append(want, bytes.Repeat([]byte{fill}, size)...)
	if !bytes.Equal(rows, want) {
		t.Fatalf("retained page rows changed: %d bytes, want %d of %q", len(rows), size, fill)
	}
}

// A retained v5 page keeps its segment payload, and its own span index, while
// the connection receives later frames into reused cache buffers.
func TestRetainedV5PageSurvivesLaterFrames(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int
		wrap func(testing.TB, []byte) []byte
	}{
		{"SelfContained", 64, func(t testing.TB, f []byte) []byte { return recvWire(t, 5, f) }},
		{"Split", 3 * segment.MaxPayloadSize, recvSplitWire},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var data []byte
			for _, fill := range []byte{'a', 'b'} {
				data = append(data, tc.wrap(t, invFrame(5, 0, typedTestRowsBody(bytes.Repeat([]byte{fill}, tc.size))))...)
			}
			l := newRecvLoop(t, 5, data)
			kept := l.one(t)
			checkRecvPage(t, kept, 'a', tc.size)
			for i := 0; i < 5; i++ {
				page := l.one(t)
				fill := byte('b')
				if i%2 == 1 {
					fill = 'a'
				}
				checkRecvPage(t, page, fill, tc.size)
				page.Owner.Release()
				checkRecvPage(t, kept, 'a', tc.size)
			}
			kept.Owner.Release()
			if kept.Owner.Alive() {
				t.Fatal("page lease alive after its release")
			}
		})
	}
}

// Corrupt or truncated v5 segments fail the receive with the errors the
// allocating reader returned, byte for byte.
func TestRecvV5CorruptSegmentErrors(t *testing.T) {
	good := recvWire(t, 5, invFrame(5, 0, typedTestRowsBody([]byte("v"))))
	split := recvSplitWire(t, invFrame(5, 0, typedTestRowsBody(bytes.Repeat([]byte{'s'}, segment.MaxPayloadSize))))
	flip := func(b []byte, i int) []byte {
		b = append([]byte(nil), b...)
		b[i] ^= 0xff
		return b
	}
	for _, tc := range []struct {
		name, want string
		data       []byte
	}{
		{"HeaderCRC", "gocql: crc24 mismatch in frame header, computed: 4079509, got: 4112533", flip(good, 4)},
		{"PayloadCRC", "gocql: crc32 mismatch in segment payload", flip(good, len(good)-1)},
		{"TruncatedPayload", "gocql: failed reading segment payload: unexpected EOF", good[:len(good)-6]},
		{"TruncatedTrailer", "gocql: failed reading segment CRC: unexpected EOF", good[:len(good)-2]},
		{"ContinuationCRC", "gocql: failed to read continuation segment payload: gocql: crc32 mismatch in segment payload", flip(split, len(split)-1)},
		{"ContinuationHeaderCRC", "gocql: failed to read continuation segment header: gocql: crc24 mismatch in frame header, computed: 12526257, got: 12574129", flip(split, segment.UncompressedHeaderSize+segment.MaxPayloadSize+segment.Crc32Size+4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			c := &Conn{InternalVersion: 5, InternalR: &InternalConnReader{InternalConn: &loopNetConn{data: []byte{0}}, InternalR: bufio.NewReader(r)}, InternalCalls: InternalNewCallTable(), InternalStreams: streams.New(), InternalLogger: logging.Nop()}
			c.InternalCfg = new(ConnConfig)
			c.InternalCfg.InternalReceiveCache = new(wire.ReceiveCache)
			err := c.InternalRecvSegment(context.Background())
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
