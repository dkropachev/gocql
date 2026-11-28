// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

func TestParameterBuffer(t *testing.T) {
	// TestParameterBuffer/EncodeAppend checks that an encoding appended to
	// caller storage keeps the existing bytes, uses spare capacity in place,
	// and copies only when it outgrows that capacity.
	t.Run("EncodeAppend", func(t *testing.T) {
		columns := cql.NewColumns(cql.ColumnSpec{Name: "value", Type: cql.BlobType})
		plan, err := cql.OneParam(cql.ByteSliceCodec).Compile(columns)
		if err != nil {
			t.Fatal(err)
		}
		var b ParameterBuffer
		for _, spare := range []int{64, 0} {
			storage := make([]byte, 4, 4+spare)
			copy(storage, "keep")
			got, values, err := b.EncodeAppend(plan, columns, []byte("abc"), true, storage)
			if err != nil {
				t.Fatal(err)
			}
			if want := "keep\x00\x00\x00\x03abc"; string(got) != want {
				t.Fatalf("spare %d: storage %q, want %q", spare, got, want)
			}
			if inPlace := &got[0] == &storage[0]; inPlace != (spare > 0) {
				t.Fatalf("spare %d: encoded in place %t", spare, inPlace)
			}
			cell, _ := values.Cells[0].Body.Contiguous()
			if string(cell) != "abc" || values.Framed.Len() != 7 {
				t.Fatalf("spare %d: cell %q, framed %d", spare, cell, values.Framed.Len())
			}
		}
	})

	t.Run("EncodeParameters", func(t *testing.T) {
		t.Run("EmptyHasPresentWireState", func(t *testing.T) {
			typ := cql.IntType
			columns := cql.NewColumns(cql.ColumnSpec{Name: "value", Type: typ})
			plan, err := cql.OneParam(cql.MaybeEmptyCodec(cql.Int32Codec)).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			var storage ParameterBuffer
			value := cql.MaybeEmpty[int32]{Empty: true}
			got, err := storage.Encode(plan, columns, value, true)
			if err != nil {
				t.Fatal(err)
			}
			want, err := InternalEncodeParameters(plan, columns, value, true, true)
			if err != nil {
				t.Fatal(err)
			}
			if got.Cells[0].State != want.Cells[0].State || got.Cells[0].State != cql.CellPresent {
				t.Fatal(got.Cells[0].State, want.Cells[0].State)
			}
		})

		t.Run("MatchesCheckedGenericEncoding", func(t *testing.T) {
			typ := cql.BlobType
			columns := cql.NewColumns(cql.ColumnSpec{Name: "value", Type: typ})
			plan, err := cql.OneParam(cql.BindEncoder(cql.ByteSliceCodec)).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			var storage ParameterBuffer
			for _, state := range []cql.CellState{cql.CellPresent, cql.CellNull, cql.CellUnset} {
				for _, size := range []int{0, 1, 257, 128 << 10} {
					input := cql.Bind(bytes.Repeat([]byte{41}, size))
					switch state {
					case cql.CellNull:
						input = cql.BindNull[[]byte]()
					case cql.CellUnset:
						input = cql.BindUnset[[]byte]()
					}
					got, err := storage.Encode(plan, columns, input, true)
					if err != nil {
						t.Fatal(err)
					}
					want, err := InternalEncodeParameters(plan, columns, input, true, true)
					if err != nil {
						t.Fatal(err)
					}
					g, w := make([]byte, got.Framed.Len()), make([]byte, want.Framed.Len())
					_, _ = got.Framed.CopyTo(g)
					_, _ = want.Framed.CopyTo(w)
					if !bytes.Equal(g, w) || got.Cells[0].State != want.Cells[0].State {
						t.Fatal(state, size, "encoding differs")
					}
					if value, _ := input.Get(); len(value) > 0 {
						value[0] = 99
					}
					owned := make([]byte, got.Framed.Len())
					_, _ = got.Framed.CopyTo(owned)
					if !bytes.Equal(g, owned) {
						t.Fatal("captured input changed")
					}
					storage.Retire()
					if storage.RetainedBytes() > 64<<10 {
						t.Fatal("idle buffer exceeds limit")
					}
				}
			}
		})
	})

	t.Run("Retire", func(t *testing.T) {
		t.Run("BoundsDescriptorStorage", func(t *testing.T) {
			typ := cql.IntType
			columns := make([]cql.ColumnSpec, 2048)
			fields := make([]cql.InputField[cql.EmptyRow], len(columns))
			for i := range columns {
				columns[i] = cql.ColumnSpec{Name: fmt.Sprint(i), Type: typ}
				fields[i] = cql.ParamAt(i, cql.Int32Codec, func(cql.EmptyRow) int32 { return 42 })
			}
			metadata := cql.NewColumns(columns...)
			plan, err := cql.Params(fields...).Compile(metadata)
			if err != nil {
				t.Fatal(err)
			}
			var storage ParameterBuffer
			values, err := storage.Encode(plan, metadata, cql.EmptyRow{}, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(values.Cells) != len(columns) {
				t.Fatal("capture count", len(values.Cells))
			}
			storage.Retire()
			if storage.RetainedBytes() > 64<<10 {
				t.Fatal("idle request descriptors exceed budget", storage.RetainedBytes())
			}
		})
	})
}
