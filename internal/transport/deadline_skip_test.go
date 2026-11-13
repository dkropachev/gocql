package transport

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"
)

// deadlineCountingConn serves an endless byte stream and records every deadline
// the connection code applies.
type deadlineCountingConn struct {
	mu        sync.Mutex
	readSets  []time.Time
	writeSets []time.Time
	off       int
}

func (f *deadlineCountingConn) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(f.off)
		f.off++
	}
	return len(p), nil
}
func (f *deadlineCountingConn) Write(p []byte) (int, error) { return len(p), nil }
func (f *deadlineCountingConn) Close() error                { return nil }
func (f *deadlineCountingConn) LocalAddr() net.Addr         { return &net.TCPAddr{} }
func (f *deadlineCountingConn) RemoteAddr() net.Addr        { return &net.TCPAddr{} }
func (f *deadlineCountingConn) SetDeadline(time.Time) error { return nil }
func (f *deadlineCountingConn) SetReadDeadline(t time.Time) error {
	f.mu.Lock()
	f.readSets = append(f.readSets, t)
	f.mu.Unlock()
	return nil
}
func (f *deadlineCountingConn) SetWriteDeadline(t time.Time) error {
	f.mu.Lock()
	f.writeSets = append(f.writeSets, t)
	f.mu.Unlock()
	return nil
}
func (f *deadlineCountingConn) reads() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.readSets...)
}
func (f *deadlineCountingConn) writes() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.writeSets...)
}

func newCountingReader(conn net.Conn) *InternalConnReader {
	return &InternalConnReader{InternalConn: conn, InternalR: bufio.NewReaderSize(conn, 64)}
}

func TestReaderSkipsRedundantZeroDeadline(t *testing.T) {
	nc := new(deadlineCountingConn)
	r := newCountingReader(nc)
	buf := make([]byte, 100) // larger than the buffer: every Read needs a refill
	for i := 0; i < 100; i++ {
		if _, err := r.Read(buf); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(nc.reads()); got > 1 {
		t.Fatalf("SetReadDeadline called %d times with timeout 0, want at most 1", got)
	}
}

func TestReaderDeadlineReappliedAfterChange(t *testing.T) {
	nc := new(deadlineCountingConn)
	r := newCountingReader(nc)
	buf := make([]byte, 100)
	read := func() {
		t.Helper()
		if _, err := r.Read(buf); err != nil {
			t.Fatal(err)
		}
	}
	read()
	read()
	if n := len(nc.reads()); n != 1 {
		t.Fatalf("after zero-timeout reads: %d sets, want 1", n)
	}

	r.SetTimeout(5 * time.Second)
	read()
	sets := nc.reads()
	if len(sets) != 2 || sets[1].IsZero() {
		t.Fatalf("after SetTimeout(5s): %v, want a non-zero deadline", sets)
	}

	r.SetTimeout(0)
	read()
	read()
	sets = nc.reads()
	if len(sets) != 3 || !sets[2].IsZero() {
		t.Fatalf("after SetTimeout(0): %v, want exactly one more, zero", sets)
	}

	// Disarm for the idle wait, then re-arm on the first byte.
	r.InternalSetDisarm(true)
	read() // disarmed: zero deadline, already zero, skipped
	if n := len(nc.reads()); n != 3 {
		t.Fatalf("disarmed zero read: %d sets, want 3", n)
	}
	if err := r.requestPause(true); err != nil {
		t.Fatal(err)
	}
	sets = nc.reads()
	if len(sets) != 4 || sets[3].IsZero() {
		t.Fatalf("requestPause: %v, want a non-zero (interrupt) deadline", sets)
	}
	read() // paused+disarmed: interrupt deadline again
	sets = nc.reads()
	if len(sets) != 5 || sets[4].IsZero() {
		t.Fatalf("paused read: %v, want the interrupt deadline re-applied", sets)
	}
	if err := r.requestPause(false); err != nil {
		t.Fatal(err)
	}
	sets = nc.reads()
	if len(sets) != 6 || !sets[5].IsZero() {
		t.Fatalf("unpause: %v, want a zero deadline", sets)
	}
	read()
	if n := len(nc.reads()); n != 6 {
		t.Fatalf("zero read after unpause: %d sets, want 6 (already zero)", n)
	}
	r.InternalSetDisarm(false)
	r.SetTimeout(time.Second)
	read()
	sets = nc.reads()
	if len(sets) != 7 || sets[6].IsZero() {
		t.Fatalf("re-arm: %v, want non-zero", sets)
	}
}

func TestWriterSkipsRedundantZeroDeadline(t *testing.T) {
	nc := new(deadlineCountingConn)
	w := &InternalDeadlineContextWriter{InternalW: nc, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	ctx := t.Context()
	for i := 0; i < 100; i++ {
		if _, err := w.InternalWriteContext(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := w.InternalWriteSpanSlice(ctx, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(nc.writes()); got > 1 {
		t.Fatalf("SetWriteDeadline called %d times with timeout 0, want at most 1", got)
	}
}

func TestWriterDeadlineReappliedAfterChange(t *testing.T) {
	nc := new(deadlineCountingConn)
	w := &InternalDeadlineContextWriter{InternalW: nc, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	ctx := t.Context()
	write := func() {
		t.Helper()
		if _, err := w.InternalWriteContext(ctx, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	write()
	write()
	w.InternalSetWriteTimeout(5 * time.Second)
	write()
	write()
	sets := nc.writes()
	if len(sets) != 3 || !sets[0].IsZero() || sets[1].IsZero() || sets[2].IsZero() {
		t.Fatalf("writes: %v, want zero, then a fresh non-zero deadline per write", sets)
	}
	w.InternalSetWriteTimeout(0)
	write()
	write()
	sets = nc.writes()
	if len(sets) != 4 || !sets[3].IsZero() {
		t.Fatalf("after SetWriteTimeout(0): %v, want exactly one more, zero", sets)
	}
}
