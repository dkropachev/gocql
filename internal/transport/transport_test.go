// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	implCqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"crypto/tls"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func typedTestResponse(t testing.TB, proto byte, stream int, body []byte) []byte {
	t.Helper()
	out := []byte{proto | 0x80, 0, byte(stream >> 8), byte(stream), byte(frm.OpResult)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}
func typedTestRowsBody(value []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, 2)
	out = binary.BigEndian.AppendUint32(out, 1) // global table
	out = binary.BigEndian.AppendUint32(out, 1)
	for _, s := range []string{"ks", "tbl", "value"} {
		out = binary.BigEndian.AppendUint16(out, uint16(len(s)))
		out = append(out, s...)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(implCqltype.TypeBlob))
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}

type typedTLSRecorder struct {
	conn     *tls.Conn
	expected [][]byte
	next     int
}

func (w *typedTLSRecorder) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }
func (w *typedTLSRecorder) Write(p []byte) (int, error) {
	if w.next >= len(w.expected) || len(p) != len(w.expected[w.next]) || &p[0] != &w.expected[w.next][0] {
		return 0, fmt.Errorf("TLS plaintext was gathered before tls.Conn.Write")
	}
	w.next++
	return w.conn.Write(p)
}
