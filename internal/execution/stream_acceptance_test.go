// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/testkit"
)

// This source retains only ROWS metadata and counters. It generates every cell
// into the receiver's requested range; neither a response fixture nor output
// proportional to a whole page exists on the producing side.
type rowsSource struct {
	prefix                  []byte
	rows, size, row, offset int
	length                  [4]byte
}

func testRows(version byte, rows, size int) *rowsSource {
	p := testkit.BlobRows(version, 0, []byte{})
	p = p[:len(p)-4]
	binary.BigEndian.PutUint32(p[5:9], uint32(len(p)-9+rows*(4+size)))
	binary.BigEndian.PutUint32(p[len(p)-4:], uint32(rows))
	s := &rowsSource{prefix: p, rows: rows, size: size}
	binary.BigEndian.PutUint32(s.length[:], uint32(size))
	return s
}
func (s *rowsSource) Read(p []byte) (int, error) {
	p = p[:min(len(p), 16381)] // intentionally cross scalar/unit boundaries
	if len(s.prefix) > 0 {
		n := copy(p, s.prefix)
		s.prefix = s.prefix[n:]
		return n, nil
	}
	if s.row == s.rows {
		return 0, io.EOF
	}
	if s.offset < 4 {
		n := copy(p, s.length[s.offset:])
		s.offset += n
		return n, nil
	}
	n := min(len(p), s.size-(s.offset-4))
	for i := 0; i < n; i++ {
		p[i] = 0x71
	}
	s.offset += n
	if s.offset == s.size+4 {
		s.row++
		s.offset = 0
	}
	return n, nil
}

type generatedConn struct {
	*memoryConn
	input io.Reader
}

func (c *generatedConn) Read(p []byte) (int, error) { return c.input.Read(p) }

type observedConn struct {
	net.Conn
	observe func()
}

func (s *observedConn) Read(p []byte) (int, error) { s.observe(); return s.Conn.Read(p) }

type unitSource struct {
	input      io.Reader
	compressed bool
	pending    []byte
}

func (s *unitSource) Read(p []byte) (int, error) {
	if len(s.pending) == 0 {
		data := make([]byte, 130000)
		n, err := io.ReadFull(s.input, data)
		if n == 0 {
			return 0, err
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return 0, err
		}
		s.pending = testkit.Unit(data[:n], s.compressed, false)
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// Each fuzz input chooses a real final-write interruption schedule. Close must
// join that write, preserve exact output, retire admission and reject all later
// destination access. Bounded seed/capacity/row sizes prevent allocation abuse.
func FuzzStreamingCancellationLifetime(f *testing.F) {
	for i := byte(0); i < 8; i++ {
		f.Add(i, byte(0), []byte("final"))
	}
	f.Fuzz(func(t *testing.T, which, action byte, payload []byte) {
		if len(payload) > 256 {
			return
		}
		if len(payload) == 0 {
			payload = []byte("x")
		}
		mode := fixtureModes[int(which)%len(fixtureModes)]
		s, c, peer := fixtureStreamSession(t, mode)
		done := make(chan struct{})
		go func() {
			defer close(done)
			stream := typedTestReadRequest(t, c, peer)
			_, _ = peer.Write(fixturePacket(mode, testkit.BlobRows(mode.version, uint16(stream), payload)))
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bound := runtimeQuery(t)
		defer bound.Close()
		decoder := &finalAccess{entered: make(chan struct{}), leave: make(chan struct{})}
		var release sync.Once
		unblock := func() { release.Do(func() { close(decoder.leave) }) }
		defer unblock()
		cursor, err := RunStream(ctx, s, bound, decoder, StreamOptions{Incremental: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { unblock(); cursor.Close() }()
		dst := make([]byte, 0, len(payload))
		next := make(chan struct{})
		go func() { cursor.NextInto(&dst); close(next) }()
		await(t, decoder.entered)
		closed := make(chan struct{})
		go func() {
			if action%2 == 0 {
				cancel()
			}
			cursor.Close()
			close(closed)
		}()
		select {
		case <-closed:
			t.Fatal("Close returned during final destination write")
		default:
		}
		unblock()
		await(t, next)
		await(t, closed)
		await(t, done)
		expected := append([]byte(nil), payload...)
		expected[0] = 'F'
		if decoder.writes.Load() != 1 || !bytes.Equal(dst, expected) {
			t.Fatal("final write not joined")
		}
		if cursor.NextInto(&dst) || !bytes.Equal(dst, expected) {
			t.Fatal("late destination access")
		}
		bound.Close()
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := bound.Wait(wait); err != nil {
			t.Fatal(fmt.Errorf("admission not retired: %w", err))
		}
	})
}

func await(t testing.TB, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded lifetime operation did not join")
	}
}

type decoderError struct{ cause error }

func (d decoderError) Compile(cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	return d, nil
}
func (d decoderError) Reset(dst *[]byte) { *dst = (*dst)[:0] }
func (d decoderError) DecodeInto(r *codec.StreamRowReader, dst *[]byte) error {
	p, err := codec.StreamBlob.Compile(cqltype.BlobType)
	if err != nil {
		return err
	}
	if err = codec.ReadStreamColumn(r, p, dst); err != nil {
		return err
	}
	return d.cause
}
