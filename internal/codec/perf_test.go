//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func sequenceWork[T any](t *testing.T, typ cqltype.DataType, encoder Encoder[[]T], decoder Decoder[Sequence[T]], input []T) {
	t.Helper()
	encoded := CodecTestMust(EncodeValueBorrowed(typ, encoder, input))
	raw := encoded.Bytes().OwnedBytes()
	spans := make([]wire.Span, len(raw))
	for i := range raw {
		spans[i] = wire.Borrow(raw[i : i+1])
	}
	chain := CodecTestMust(wire.NewChain(spans...))
	compiled := CodecTestMust(compileValueDecoder(decoder, typ))
	ResetPerfCounters()
	wire.ResetPerfCounters()
	seq := CodecTestMust(compiled.Decode(NewValueReader(typ, cqltype.Present, chain, nil)))
	it := seq.Iterator()
	n := 0
	for it.Next() {
		_ = it.Value()
		n++
	}
	if it.Err() != nil || n != len(input) {
		t.Fatal(n, it.Err())
	}
	c := wire.ReadPerfCounters()
	comp, maps := ReadPerfCounters()
	if c.CopyBytes != 0 || c.Retains != 0 || c.Releases != 0 || comp != 0 || maps != 0 || ReadPerfTypeChecks() != 0 {
		t.Fatal(c, comp, maps)
	}
	t.Logf("elements=%d bytes=%d counts=%+v compiles=%d name_maps=%d CheckType=%d", n, len(raw), c, comp, maps, ReadPerfTypeChecks())
}

func oneScalarWrite[T any](t *testing.T, typ cqltype.DataType, codec Encoder[T], value T) {
	t.Helper()
	wire.ResetPerfCounters()
	encoded := CodecTestMust(EncodeValue(typ, codec, value))
	c := wire.ReadPerfCounters()
	if c.ScalarWrites != 2 || c.ScalarBytes != encoded.Bytes().Len()+4 || c.CopyBytes != 0 {
		t.Fatal("framing plus one body write", c)
	}
	t.Logf("%s %+v", typ, c)
}
