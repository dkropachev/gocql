// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"testing"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// dirtyRow is decoded by dirtyRows, a plan that sets Seen and leaves the rest
// of the destination alone.
type dirtyRow struct {
	Seen  bool
	Stale int
}

type dirtyRows struct{}
type dirtyPlan struct{}

func (dirtyRows) Compile(cqltype.Columns) (codec.RowPlan[dirtyRow], error) { return dirtyPlan{}, nil }
func (dirtyRows) Clone(v dirtyRow) dirtyRow                                { return v }
func (dirtyPlan) Clone(v dirtyRow) dirtyRow                                { return v }
func (p dirtyPlan) Decode(r *codec.RowReader) (dirtyRow, error) {
	var v dirtyRow
	return v, p.DecodeInto(r, &v)
}
func (dirtyPlan) DecodeInto(r *codec.RowReader, dst *dirtyRow) error {
	dst.Seen = true
	return r.SkipColumn()
}

func dirtyResponse(t *testing.T, rows int) *transport.InternalResponsePage {
	t.Helper()
	var body []byte
	for range rows {
		body = append(body, 0, 0, 0, 1, 7)
	}
	chain := typedTestChain(t, body)
	cols := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: codecTestMust(cqltype.ScalarType(cqltype.TypeBlob))})
	return &transport.InternalResponsePage{ResponseData: model.ResponseData{Columns: cols, Rows: chain, RowCount: int32(rows), Owner: wire.NewLease(chain, nil)}}
}

// TestCustomPlanStillGetsZeroedDestination checks that an application plan
// that does not clear its destination still gets a zeroed row from Page,
// Rows and Each.
func TestCustomPlanStillGetsZeroedDestination(t *testing.T) {
	t.Run("Page", func(t *testing.T) {
		p := codecTestMust(InternalNewPage(dirtyResponse(t, 2), codec.RowDecoder[dirtyRow](dirtyRows{}), nil))
		defer p.Close()
		for i := range 2 {
			dst := dirtyRow{Stale: 9}
			if !p.NextInto(&dst) {
				t.Fatal(p.Err())
			}
			if dst != (dirtyRow{Seen: true}) {
				t.Fatalf("row %d: %+v", i, dst)
			}
		}
	})
	t.Run("Each", func(t *testing.T) {
		rt := &InternalSessionRuntime{Core: new(cluster.Core)}
		response := dirtyResponse(t, 2)
		defer response.Owner.Release()
		dst := dirtyRow{Stale: 9}
		rows := 0
		err := rt.decodeEach(context.Background(), response, codec.RowDecoder[dirtyRow](dirtyRows{}), &InternalResultPlanCache[dirtyRow]{}, &dst, func(row *dirtyRow) error {
			if *row != (dirtyRow{Seen: true}) {
				t.Fatalf("row %d: %+v", rows, *row)
			}
			rows++
			row.Stale = 9
			return nil
		}, &eachScratch{})
		if err != nil || rows != 2 {
			t.Fatal(rows, err)
		}
	})
}

// TestBuiltinPlansManageDestination checks that every built-in row plan
// clears its own destination, so the session skips its per-row clear.
func TestBuiltinPlansManageDestination(t *testing.T) {
	blob := codecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	boolean := codecTestMust(cqltype.ScalarType(cqltype.TypeBoolean))
	one := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: blob})
	two := cqltype.NewColumns(cqltype.ColumnSpec{Name: "a", Type: blob}, cqltype.ColumnSpec{Name: "b", Type: blob})
	cas := cqltype.NewColumns(cqltype.ColumnSpec{Name: "[applied]", Type: boolean}, cqltype.ColumnSpec{Name: "v", Type: blob})
	type row struct{ V cqltype.Bytes }
	field := codec.Column("v", codec.BlobCodec, func(r *row) *cqltype.Bytes { return &r.V })
	single := codec.OneColumn(codec.BlobCodec)
	cases := []struct {
		name string
		plan func() (bool, error)
	}{
		{"single column", func() (bool, error) { return manages(single, one) }},
		{"mapped", func() (bool, error) {
			return manages(codec.ConvertRows(single, func(b cqltype.Bytes) string { return string(b.OwnedBytes()) }, func(s string) string { return s }), one)
		}},
		{"row", func() (bool, error) { return manages(codec.Row(field), one) }},
		{"row into", func() (bool, error) { return manages(codec.RowInto(func(*row) {}, field), one) }},
		{"paired", func() (bool, error) { return manages(codec.PairRows(single, single), two) }},
		{"CAS", func() (bool, error) { return manages(codec.CASRows(codec.Row(field)), cas) }},
		{"dynamic", func() (bool, error) { return manages(codec.DynamicRows, one) }},
		{"exec rows", func() (bool, error) { return manages[codec.EmptyRecord](execRowsDecoder{}, two) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, err := c.plan()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("built-in plan does not manage its destination")
			}
		})
	}
	if ok, _ := manages[dirtyRow](dirtyRows{}, one); ok {
		t.Fatal("application plan claims to manage its destination")
	}
}

func manages[R any](decoder codec.RowDecoder[R], columns cqltype.Columns) (bool, error) {
	plan, err := codec.CompileRowPlan(decoder, columns)
	if err != nil {
		return false, err
	}
	return codec.ManagesRowDestination(plan), nil
}

// TestPageStorageFlagsInPadding checks that the page's cached destination flag
// sits in the padding after index, so Page did not grow.
func TestPageStorageFlagsInPadding(t *testing.T) {
	var s pageStorage[cqltype.Bytes]
	if got, want := unsafe.Sizeof(s), unsafe.Offsetof(s.index)+8; got != want {
		t.Fatalf("pageStorage is %d bytes, want %d", got, want)
	}
	if unsafe.Offsetof(s.rowDest) <= unsafe.Offsetof(s.index) {
		t.Fatal("rowDest is not in the tail padding")
	}
}
