// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"reflect"
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
// that does not clear its destination still gets a zeroed row from Page and
// Each. Rows decodes through its Page.
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

// TestBuiltinPlansClearStaleRow checks the contract behind the marker: a
// built-in plan decoding an all-null row into the previous row's destination
// produces the same value as decoding it into a zeroed destination, so no
// field of the previous row survives the skipped clear.
func TestBuiltinPlansClearStaleRow(t *testing.T) {
	blob := codecTestMust(cqltype.ScalarType(cqltype.TypeBlob))
	boolean := codecTestMust(cqltype.ScalarType(cqltype.TypeBoolean))
	one := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: blob})
	two := cqltype.NewColumns(cqltype.ColumnSpec{Name: "a", Type: blob}, cqltype.ColumnSpec{Name: "b", Type: blob})
	cas := cqltype.NewColumns(cqltype.ColumnSpec{Name: "[applied]", Type: boolean}, cqltype.ColumnSpec{Name: "v", Type: blob})
	type row struct {
		V     cqltype.Nullable[cqltype.Bytes]
		Extra int
	}
	blobs := codec.NullableCodec(codec.BlobCodec)
	field := codec.Column("v", blobs, func(r *row) *cqltype.Nullable[cqltype.Bytes] { return &r.V })
	single := codec.OneColumn(blobs)
	value := []byte{0, 0, 0, 1, 'x'}
	null := []byte{0xff, 0xff, 0xff, 0xff}
	notApplied := []byte{0, 0, 0, 1, 0}
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	oneRows := [][]byte{value, null}
	twoRows := [][]byte{cat(value, value), cat(null, null)}
	casRows := [][]byte{cat(notApplied, value), cat(notApplied, null)}
	// stale sets what no column writes, as an application may between rows.
	stale := func(r *row) { r.Extra = 9 }
	staleNullable := func(v *cqltype.Nullable[cqltype.Bytes]) { v.Valid = true }
	t.Run("single column", func(t *testing.T) { clearsStaleRow(t, single, one, oneRows, staleNullable) })
	t.Run("mapped", func(t *testing.T) {
		mapped := codec.ConvertRows(single, func(b cqltype.Nullable[cqltype.Bytes]) string { return string(b.Value.OwnedBytes()) }, func(s string) string { return s })
		clearsStaleRow(t, mapped, one, oneRows, func(s *string) { *s = "stale" })
	})
	t.Run("row", func(t *testing.T) { clearsStaleRow(t, codec.Row(field), one, oneRows, stale) })
	t.Run("row into", func(t *testing.T) {
		clearsStaleRow(t, codec.RowInto(func(r *row) { *r = row{} }, field), one, oneRows, stale)
	})
	t.Run("paired", func(t *testing.T) {
		clearsStaleRow(t, codec.PairRows(single, single), two, twoRows, func(p *codec.Pair[cqltype.Nullable[cqltype.Bytes], cqltype.Nullable[cqltype.Bytes]]) {
			staleNullable(&p.First)
			staleNullable(&p.Second)
		})
	})
	t.Run("CAS", func(t *testing.T) {
		clearsStaleRow(t, codec.CASRows(codec.Row(field)), cas, casRows, func(r *codec.CASResult[row]) {
			r.Applied = true
			stale(&r.Previous.Value)
		})
	})
	t.Run("dynamic", func(t *testing.T) { clearsStaleRow(t, codec.DynamicRows, one, oneRows, nil) })
	t.Run("exec rows", func(t *testing.T) { clearsStaleRow[codec.EmptyRecord](t, execRowsDecoder{}, two, twoRows, nil) })
}

// clearsStaleRow decodes rows[0] into a destination, dirties it with stale,
// decodes rows[1] into it and compares the result with rows[1] decoded into a
// zeroed destination. The plan must manage its destination, so Page skips its
// own clear.
func clearsStaleRow[R any](t *testing.T, decoder codec.RowDecoder[R], columns cqltype.Columns, rows [][]byte, stale func(*R)) {
	t.Helper()
	if ok, err := manages(decoder, columns); err != nil || !ok {
		t.Fatal("plan does not manage its destination", err)
	}
	var body []byte
	for _, r := range rows {
		body = append(body, r...)
	}
	chain := typedTestChain(t, body)
	response := &transport.InternalResponsePage{ResponseData: model.ResponseData{Columns: columns, Rows: chain, RowCount: int32(len(rows)), Owner: wire.NewLease(chain, nil)}}
	reused := codecTestMust(InternalNewPage(response, decoder, nil))
	defer reused.Close()
	fresh := codecTestMust(InternalNewPage(response, decoder, nil))
	defer fresh.Close()
	var got, skip, want R
	if !reused.NextInto(&got) {
		t.Fatal("first row:", reused.Err())
	}
	if stale != nil {
		stale(&got)
	}
	if !reused.NextInto(&got) {
		t.Fatal("reused destination:", reused.Err())
	}
	if !fresh.NextInto(&skip) || !fresh.NextInto(&want) {
		t.Fatal("zeroed destination:", fresh.Err())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stale row survived: got %+v, want %+v", got, want)
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
