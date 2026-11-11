// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

func TestParameterBuffer(t *testing.T) {
	// TestParameterBuffer/EncodeAppend checks that an encoding appended to
	// caller storage keeps the existing bytes, uses spare capacity in place,
	// and copies only when it outgrows that capacity.
	t.Run("EncodeAppend", func(t *testing.T) {
		columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: cqltype.BlobType})
		plan, err := codec.OneParam(codec.ByteSliceCodec).Compile(columns)
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
			typ := cqltype.IntType
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
			plan, err := codec.OneParam(codec.EmptyCodec(codec.Int32)).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			var storage ParameterBuffer
			value := cqltype.EmptyValueOf[int32]{Empty: true}
			got, err := storage.Encode(plan, columns, value, true)
			if err != nil {
				t.Fatal(err)
			}
			want, err := InternalEncodeParameters(plan, columns, value, true, true)
			if err != nil {
				t.Fatal(err)
			}
			if got.Cells[0].State != want.Cells[0].State || got.Cells[0].State != cqltype.Present {
				t.Fatal(got.Cells[0].State, want.Cells[0].State)
			}
		})

		t.Run("MatchesCheckedGenericEncoding", func(t *testing.T) {
			typ := cqltype.BlobType
			columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
			plan, err := codec.OneParam(codec.BindEncoder(codec.ByteSliceCodec)).Compile(columns)
			if err != nil {
				t.Fatal(err)
			}
			var storage ParameterBuffer
			for _, state := range []cqltype.CellState{cqltype.Present, cqltype.Null, cqltype.Unset} {
				for _, size := range []int{0, 1, 257, 128 << 10} {
					input := cqltype.BindValue[[]byte]{State: state, Value: bytes.Repeat([]byte{41}, size)}
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
					if size > 0 {
						input.Value[0] = 99
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
			typ := cqltype.IntType
			columns := make([]cqltype.ColumnSpec, 2048)
			fields := make([]codec.InputField[codec.EmptyRecord], len(columns))
			for i := range columns {
				columns[i] = cqltype.ColumnSpec{Name: fmt.Sprint(i), Type: typ}
				fields[i] = codec.ParamAt(i, codec.Int32, func(codec.EmptyRecord) int32 { return 42 })
			}
			metadata := cqltype.NewColumns(columns...)
			plan, err := codec.Params(fields...).Compile(metadata)
			if err != nil {
				t.Fatal(err)
			}
			var storage ParameterBuffer
			values, err := storage.Encode(plan, metadata, codec.EmptyRecord{}, true)
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
