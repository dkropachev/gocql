// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"testing"
)

func BenchmarkNativeReadAheadScalars(b *testing.B) {
	data := make([]byte, 128<<10)
	conn := &aheadConn{}
	var ahead nativeStreamReadAhead
	raw := &InternalConnReader{InternalConn: conn}
	var out [4]byte
	b.ReportAllocs()
	for range b.N {
		conn.data = data
		ahead.begin(raw, int64(len(data)), 4096)
		for range len(data) / 4 {
			if _, err := ahead.Read(out[:]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(conn.reads)/float64(b.N), "reads/op")
}
