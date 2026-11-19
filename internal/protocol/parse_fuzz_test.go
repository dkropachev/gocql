// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"bytes"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// deepListType is a RESULT/Rows body with one column whose type nests
// list<...> levels deep, in protocol v4.
func deepListType(levels int) []byte {
	f := NewFramer(nil, protoVersion4)
	f.WriteInt(frm.ResultKindRows)
	f.WriteInt(int32(frm.FlagGlobalTableSpec))
	f.WriteInt(1) // one column
	f.WriteString("ks")
	f.WriteString("t")
	f.WriteString("c")
	for range levels {
		f.WriteShort(uint16(cql.TypeList))
	}
	f.WriteShort(uint16(cql.TypeInt))
	f.WriteInt(0) // no rows
	return f.Buf
}

// FuzzParseResponse feeds arbitrary response bodies to both parsers. Neither
// may panic on server data: Framer.ParseFrame returns malformed input as an
// error and re-panics only on a driver bug, and the typed parser never
// panics by design.
func FuzzParseResponse(f *testing.F) {
	ops := []frm.Op{frm.OpError, frm.OpReady, frm.OpResult, frm.OpSupported, frm.OpAuthenticate, frm.OpAuthChallenge, frm.OpAuthSuccess, frm.OpEvent}
	for _, op := range ops {
		f.Add(byte(4), byte(op), byte(0), []byte{})
		f.Add(byte(5), byte(op), byte(0x0f), []byte{0, 0, 0, 1, 0, 0, 0, 0})
	}
	f.Add(byte(4), byte(frm.OpResult), byte(0), deepListType(3))
	f.Add(byte(4), byte(frm.OpResult), byte(0), deepListType(10000))
	f.Add(byte(4), byte(frm.OpError), byte(0), errBody(4, 0x1000, "unavailable", func(f *framer) {
		f.WriteShort(1)
		f.WriteInt(3)
		f.WriteInt(1)
	}))
	f.Add(byte(4), byte(frm.OpEvent), byte(0), bodyOf(func(f *framer) {
		for _, s := range []string{"SCHEMA_CHANGE", "CREATED", "FUNCTION", "ks", "fn"} {
			f.WriteString(s)
		}
		f.WriteStringList([]string{"int", "list<int>"})
	}))

	f.Fuzz(func(t *testing.T, proto, op, flags byte, body []byte) {
		if proto != 4 && proto != 5 {
			proto = 4 + proto%2
		}
		head := frm.FrameHeader{Version: frm.ProtoVersion(proto) | protoDirectionMask, Op: frm.Op(op), Flags: flags, Stream: 1, Length: len(body)}

		fr := NewFramer(nil, proto)
		h := head
		fr.Header = &h
		fr.Buf = bytes.Clone(body)
		_, _ = fr.ParseFrame()

		owner := wire.NewLease(wire.ChainOf(body), nil)
		defer owner.Release()
		_, _ = protocolSettings(proto).ParseTypedResponse(head, wire.ChainOf(body), owner)
	})
}

// TestParseFrameDeepTypeNesting checks that a peer cannot overflow the stack
// through ReadTypeInfo's recursion: the frame is rejected as malformed.
func TestParseFrameDeepTypeNesting(t *testing.T) {
	t.Parallel()
	for _, levels := range []int{maxTypeInfoDepth, maxTypeInfoDepth + 1, 1 << 20} {
		fr := NewFramer(nil, protoVersion4)
		fr.Header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: frm.OpResult}
		fr.Buf = deepListType(levels)
		_, err := fr.ParseFrame()
		if levels <= maxTypeInfoDepth && err != nil {
			t.Fatalf("%d levels: %v", levels, err)
		}
		if levels > maxTypeInfoDepth && err == nil {
			t.Fatalf("%d levels: parsed, want a nesting error", levels)
		}
	}
}
