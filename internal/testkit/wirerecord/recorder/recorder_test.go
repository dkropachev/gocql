package recorder

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/testkit/wirerecord"
)

// stubConn is a net.Conn whose Read serves a fixed byte stream and whose Write
// accepts everything, so the recorder's own behaviour is all a test observes.
type stubConn struct {
	readData []byte
	written  []byte
	writeErr error
	closeErr error
	closed   bool
	// closes counts every Close, so a test can tell one teardown from two.
	closes int
}

func (c *stubConn) Read(b []byte) (int, error) {
	if len(c.readData) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.readData)
	c.readData = c.readData[n:]
	return n, nil
}

func (c *stubConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.written = append(c.written, b...)
	return len(b), nil
}
func (c *stubConn) Close() error                     { c.closes++; c.closed = true; return c.closeErr }
func (c *stubConn) LocalAddr() net.Addr              { return nil }
func (c *stubConn) RemoteAddr() net.Addr             { return nil }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

// optionsFrame builds a body-less OPTIONS frame with the given version byte.
func optionsFrame(version byte) []byte {
	return []byte{version, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}
}

// startupFrame builds a protocol v4 STARTUP request whose [string map] carries the
// SCYLLA_USE_METADATA_ID opt-in — the frame the recorder has to see whole for the
// latch to fire. Its body is long enough to split a Write anywhere in it.
func startupFrame() []byte {
	opts := [][2]string{
		{"CQL_VERSION", "3.0.0"},
		{"SCYLLA_USE_METADATA_ID", ""},
	}
	body := []byte{0x00, byte(len(opts))}
	for _, kv := range opts {
		for _, s := range kv {
			body = append(body, byte(len(s)>>8), byte(len(s)))
			body = append(body, s...)
		}
	}
	header := []byte{
		0x04,       // version v4 (request)
		0x00,       // header flags
		0x00, 0x2A, // stream id
		0x01,                              // opStartup
		0x00, 0x00, 0x00, byte(len(body)), // body length
	}
	return append(header, body...)
}

// recordedFrames decodes the newline-delimited records the recorder wrote.
func recordedFrames(t *testing.T, fname string) []wirerecord.Record {
	t.Helper()

	data, err := os.ReadFile(fname)
	if err != nil {
		t.Fatalf("reading %s: %v", fname, err)
	}

	// An empty file is a recording of nothing, which is a frame count the caller can
	// assert on. Splitting it yields one zero-length line that json.Unmarshal refuses,
	// so this used to fail here as "unexpected end of JSON input" instead.
	if len(data) == 0 {
		return nil
	}

	var records []wirerecord.Record
	for _, line := range bytes.Split(bytes.TrimRight(data, "\n"), []byte{'\n'}) {
		var record wirerecord.Record
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decoding %s: %v", fname, err)
		}
		records = append(records, record)
	}
	return records
}

// timeoutErr is a net.Error reporting a timeout -- the shape connReader.Read treats as
// resumable when the read that hit it still delivered bytes.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// scriptedRead is one result a scriptedConn hands back: some bytes, and the error
// returned with them.
type scriptedRead struct {
	data []byte
	err  error
}

// scriptedConn serves a fixed sequence of Read results, so a test can hand the recorder
// the (n > 0, err) pair a real socket is allowed to return.
type scriptedConn struct {
	stubConn
	reads []scriptedRead
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	next := c.reads[0]
	c.reads = c.reads[1:]
	return copy(b, next.data), next.err
}

// endlessConn serves one whole frame per Read until it is closed.
//
// It records being closed in the embedded stubConn's own field rather than adding a
// second one that shadows it: a test reaching for stubConn.closed -- which is what
// TestConnectionRecorder/Close/ClosesEverything asserts on -- would otherwise read a
// field this type never sets and pass for the wrong reason. Both accesses are under
// mu because Close here races the read goroutine, which the unsynchronised
// stubConn.Close this one overrides does not have to consider.
type endlessConn struct {
	stubConn
	frame []byte
	mu    sync.Mutex
}

func (c *endlessConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stubConn.closed {
		return 0, net.ErrClosed
	}
	return copy(b, c.frame), nil
}

func (c *endlessConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stubConn.closed = true
	return nil
}

// startupFrameV2 builds a protocol v2 STARTUP: the handshake the driver opens with, in
// the layout that predates the 2-byte stream id, so the opcode sits at index 3 and the
// body at index 8 rather than 4 and 9.
func startupFrameV2() []byte {
	body := []byte{
		0x00, 0x01, // one option
		0x00, 0x0B, 'C', 'Q', 'L', '_', 'V', 'E', 'R', 'S', 'I', 'O', 'N',
		0x00, 0x05, '3', '.', '0', '.', '0',
	}
	header := []byte{
		0x02,                              // version v2 (request)
		0x00,                              // header flags
		0x00,                              // stream id (1 byte)
		0x01,                              // opStartup
		0x00, 0x00, 0x00, byte(len(body)), // body length
	}
	return append(header, body...)
}

// optionsFrameV2 builds a body-less protocol v2 OPTIONS: eight bytes that are nothing
// but a v1/v2 header, and therefore a complete frame one byte short of the v3+ header
// the splitter slices on.
//
// This is the shape that used to slip the floor entirely, because the check ran only
// once nine bytes had arrived. It is also the shape the driver sends most often --
// controlConn.heartBeat sends an OPTIONS every 30 seconds, and OPTIONS carries no body.
func optionsFrameV2() []byte {
	return []byte{
		0x02,                   // version v2 (request)
		0x00,                   // header flags
		0x00,                   // stream id (1 byte)
		0x05,                   // opOptions
		0x00, 0x00, 0x00, 0x00, // body length
	}
}

// closeRacingConn is a net.Conn whose Write is still in flight when Close arrives and
// fails the way a real socket does once that close has landed.
//
// Close releases the write and then waits for it to have decided what to do with the
// error, so the ordering the test is about is pinned rather than raced: the recorder
// must have marked itself closed before the socket goes, not after.
type closeRacingConn struct {
	stubConn
	inWrite  chan struct{}
	release  chan struct{}
	observed chan struct{}
}

func (c *closeRacingConn) Write([]byte) (int, error) {
	close(c.inWrite)
	<-c.release
	return 0, net.ErrClosed
}

func (c *closeRacingConn) Close() error {
	close(c.release)
	<-c.observed
	return c.stubConn.Close()
}
