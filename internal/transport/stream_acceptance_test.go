// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"encoding/binary"
	"io"
	"net"

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
