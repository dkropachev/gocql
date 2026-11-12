// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func reuseMetadataBody(name string, kind uint16) []byte {
	b := binary.BigEndian.AppendUint32(nil, frm.ResultKindRows)
	b = binary.BigEndian.AppendUint32(b, uint32(frm.FlagGlobalTableSpec))
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, s := range []string{"ks", "table", name} {
		b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
		b = append(b, s...)
	}
	b = binary.BigEndian.AppendUint16(b, kind)
	return binary.BigEndian.AppendUint32(b, 0)
}

// TestMetadataReuseInterleaved checks the connection's reuse across several
// statements interleaved on one connection: each keeps its own entry, hits
// return the statement's expected columns without allocating, and a changed
// shape under the same expectation is parsed afresh and replaces only that
// statement's entry.
func TestMetadataReuseInterleaved(t *testing.T) {
	settings := protocolSettings(4)
	settings.MetadataReuse = new(MetadataReuse)
	parse := func(data []byte, expect cqltype.Columns) cqltype.Columns {
		chain, _ := wire.NewChain(wire.Borrow(data))
		lease := wire.NewLease(chain, nil)
		page := &model.ResponseData{Columns: expect}
		if _, err := settings.ParseTypedResponseInto(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, chain, lease, page); err != nil {
			t.Fatal(err)
		}
		lease.Release()
		return page.Columns
	}
	const n = 5
	var bodies [n][]byte
	var expect [n]cqltype.Columns
	for i := range bodies {
		name := fmt.Sprintf("v%d", i)
		bodies[i] = reuseMetadataBody(name, uint16(cqltype.TypeBlob))
		// Expected columns as a prepared statement holds them: equal to the
		// parsed ones, never the same.
		expect[i] = cqltype.NewColumns(cqltype.ColumnSpec{Keyspace: "ks", Table: "table", Name: name, Type: cqltype.BlobType})
	}
	for i := 0; i < 4; i++ {
		parse(bodies[i], expect[i])
	}
	// One statement repeating is the reference: every hit allocates the same.
	base := testing.AllocsPerRun(100, func() { parse(bodies[0], expect[0]) })
	i := 0
	allocs := testing.AllocsPerRun(1000, func() {
		if got := parse(bodies[i&3], expect[i&3]); !got.Same(expect[i&3]) {
			t.Fatal("interleaved hit did not return the expected columns", i&3)
		}
		i++
	})
	if allocs != base {
		t.Fatalf("four interleaved statements allocate %.0f/op, one statement %.0f/op", allocs, base)
	}
	// A fifth statement evicts one entry and is still parsed correctly.
	if got := parse(bodies[4], expect[4]); !got.Same(expect[4]) {
		t.Fatal("fifth statement did not return its expected columns")
	}
	for i := range bodies {
		if got := parse(bodies[i], expect[i]); !got.Equal(expect[i]) {
			t.Fatal("statement parsed to other columns", i)
		}
	}
	// Statement 0's result changes type: the new columns are parsed, never the
	// expected or remembered ones, and the other statements keep their entries.
	for i := 1; i < 4; i++ {
		parse(bodies[i], expect[i])
	}
	changedBody := reuseMetadataBody("v0", uint16(cqltype.TypeInt))
	for range 2 {
		changed := parse(changedBody, expect[0])
		if changed.Equal(expect[0]) {
			t.Fatal("changed metadata returned stale columns")
		}
		if spec, _ := changed.At(0); spec.Type.Kind() != cqltype.TypeInt {
			t.Fatal("changed metadata parsed to", spec.Type)
		}
	}
	i = 1
	allocs = testing.AllocsPerRun(300, func() {
		parse(bodies[i], expect[i])
		i = i%3 + 1
	})
	if allocs != base {
		t.Fatalf("a changed statement evicted another's entry: %.0f allocs/op, one statement %.0f/op", allocs, base)
	}
	if got := parse(bodies[0], expect[0]); !got.Equal(expect[0]) {
		t.Fatal("original shape after a change parsed to other columns")
	}
}
