package execution

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
func foundationScalar(t testing.TB, kind cqltype.Type) cqltype.DataType {
	t.Helper()
	d, err := cqltype.ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func streamCell(body []byte) []byte {
	b := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(b, uint32(len(body)))
	copy(b[4:], body)
	return b
}
func streamDecodeFixture[T any](t testing.TB, typ cqltype.DataType, decoder codec.StreamDecoder[T], data []byte, dst *T, step int) error {
	t.Helper()
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: typ})
	r, scope := streamReaderFixture(t, columns, &streamChunkSource{data: data, step: step}, len(data))
	plan, err := codec.StreamOneColumn(decoder).Compile(columns)
	if err != nil {
		return err
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(r, dst); err != nil {
		return err
	}
	if err := r.RequireEnd(); err != nil {
		return err
	}
	return scope.RequireEnd()
}

type streamChunkSource struct {
	data                []byte
	offset, step, reads int
	record              func([]byte, int)
}

func (s *streamChunkSource) Read(p []byte) (int, error) {
	if s.offset == len(s.data) {
		return 0, io.EOF
	}
	n := min(len(p), len(s.data)-s.offset)
	if s.step > 0 {
		n = min(n, s.step)
	}
	if s.record != nil {
		s.record(p[:n], s.offset)
	}
	copy(p, s.data[s.offset:s.offset+n])
	s.offset += n
	s.reads++
	return n, nil
}

func streamReaderFixture(t testing.TB, columns cqltype.Columns, source io.Reader, size int) (*codec.StreamRowReader, *wire.Stream) {
	t.Helper()
	scope, err := wire.NewStream(context.Background(), source, int64(size), wire.StreamLimits{MaxDepth: 130, DiscardBytes: 127})
	if err != nil {
		t.Fatal(err)
	}
	r, err := codec.NewStreamRowReader(columns, scope)
	if err != nil {
		t.Fatal(err)
	}
	return r, scope
}

// profileOf returns a pointer to a copy of p, so later changes to p do not
// reach options that already hold it.
func profileOf(p settings.Profile) *settings.Profile { return &p }

type memoryConn struct {
	source                 *bytes.Reader
	readCalls              int
	directBytes, otherBulk int64
	start, end             uintptr
	closed                 bool
}

func (c *memoryConn) Read(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	c.readCalls++
	if len(p) > 64 {
		address := uintptr(unsafe.Pointer(&p[0]))
		if address >= c.start && address+uintptr(len(p)) <= c.end {
			c.directBytes += int64(len(p))
		} else {
			c.otherBulk += int64(len(p))
		}
	}
	return c.source.Read(p)
}
func (c *memoryConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
func (c *memoryConn) Close() error { c.closed = true; return nil }
func (c *memoryConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 49152}
}
func (c *memoryConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9042}
}
func (c *memoryConn) SetDeadline(time.Time) error      { return nil }
func (c *memoryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memoryConn) SetWriteDeadline(time.Time) error { return nil }
func memoryReservation(version byte, compressor protocol.Compressor, source net.Conn) *transport.InternalExclusiveConn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &transport.Conn{InternalVersion: version, InternalCtx: ctx, InternalCancel: cancel, InternalR: &transport.InternalConnReader{InternalConn: source}, InternalStreams: streams.New(), InternalLogger: logging.InternalNopLogger{}, InternalErrorHandler: transport.InternalConnErrorHandlerFn(func(*transport.Conn, error, bool) {}), InternalCompressor: compressor}
	c.InternalW = &transport.InternalDeadlineContextWriter{InternalW: source, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	if version == 5 && compressor != nil {
		c.InternalSegCompressor = compressor.(segment.Compressor)
	}
	c.InternalInitFramerCache()
	stream, ok := c.InternalStreams.GetStream()
	if !ok || stream != 0 {
		panic("synthetic stream allocation failed")
	}
	c.InternalExclusiveFlag.Store(true)
	r := &transport.InternalExclusiveConn{InternalConn: c, InternalCtx: ctx, InternalComplete: true, InternalStream: stream, InternalDone: make(chan struct{})}
	r.InternalInitStream(transport.StreamLimits{})
	return r
}
