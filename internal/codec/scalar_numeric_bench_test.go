// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

var scalarVarintSink Varint

func BenchmarkV2ScalarVarintDecode(b *testing.B) {
	data := make([]byte, 1<<20)
	data[0] = 0x01
	r := ScalarReader(b, cqltype.TypeVarint, cqltype.Present, data[:123], data[123:])
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for b.Loop() {
		v, err := VarintCodec.Decode(r)
		if err != nil {
			b.Fatal(err)
		}
		scalarVarintSink = v
	}
}
