// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"encoding/binary"
	"testing"

	. "github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func protocolSettings(protocol byte) *Settings {
	return &Settings{Protocol: protocol, MaxFrameBytes: frm.MaxFrameSize, MaxDecompressedBytes: frm.MaxFrameSize, MaxMetadataDepth: 64}
}
func typedTestChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, p := range parts {
		spans[i] = wire.Borrow(p)
	}
	c, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func typedTestBytes(t *testing.T, c wire.Chain) []byte {
	t.Helper()
	p := make([]byte, c.Len())
	if _, err := c.CopyTo(p); err != nil {
		t.Fatal(err)
	}
	return p
}

func typedTestRowsBody(value []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, 2)
	out = binary.BigEndian.AppendUint32(out, 1) // global table
	out = binary.BigEndian.AppendUint32(out, 1)
	for _, s := range []string{"ks", "tbl", "value"} {
		out = binary.BigEndian.AppendUint16(out, uint16(len(s)))
		out = append(out, s...)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(TypeBlob))
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}
