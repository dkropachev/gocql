// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package binding

import (
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// ParameterBuffer belongs to one query. Encode must not overlap an execution
// still using its previous snapshot. No caller-owned input survives Encode.
type ParameterBuffer struct {
	storage   []byte
	positions []codec.ParamCell
	cells     []model.EncodedCell
	writer    codec.ParamWriter
	builder   wire.Builder
}

func (b *ParameterBuffer) RetainedBytes() int {
	return cap(b.storage) + cap(b.cells)*int(unsafe.Sizeof(model.EncodedCell{})) + cap(b.positions)*int(unsafe.Sizeof(codec.ParamCell{}))
}

func (b *ParameterBuffer) Encode[P any](plan codec.ParamPlan[P], columns cqltype.Columns, value P, allowUnset bool) (model.EncodedValues, error) {
	if columns.Len() > 65535 {
		return model.EncodedValues{}, cqltype.TypeValidation("parameters", "maximum count is 65535")
	}
	n := columns.Len()
	if cap(b.positions) < n {
		b.positions = make([]codec.ParamCell, n)
	} else {
		b.positions = b.positions[:n]
	}
	if cap(b.cells) < n {
		b.cells = make([]model.EncodedCell, n)
	} else {
		b.cells = b.cells[:n]
	}
	b.builder.BeginOwned(b.storage)
	writer := &b.writer
	codec.ResetParamWriter(writer, &b.builder, columns, allowUnset)
	codec.CaptureParamCells(writer, b.positions)
	// The owned builder copies every borrowed span, including custom encoders.
	err := plan.Encode(writer, value)
	if err == nil {
		err = codec.FinishParamWriter(writer)
	}
	storage := b.builder.OwnedBytes()
	b.storage = storage
	framed, freezeErr := b.builder.Freeze()
	if err != nil {
		return model.EncodedValues{}, err
	}
	if freezeErr != nil {
		return model.EncodedValues{}, freezeErr
	}
	// The owned builder's bytes are one contiguous span, so each cell is a
	// subslice of them.
	for i, position := range b.positions {
		column, _ := columns.At(i)
		if position.Start < 0 || position.End < position.Start || position.End > int64(len(storage)) {
			return model.EncodedValues{}, wire.ErrBounds
		}
		b.cells[i] = model.EncodedCell{Typ: column.Type, State: position.State, Body: wire.ChainOf(storage[position.Start:position.End])}
	}
	return model.EncodedValues{Columns: columns, Cells: b.cells, Framed: framed}, nil
}

// Retire drops large buffers and all views only after transport and callbacks
// have drained. Idle queries retain at most 64 KiB of request backing.
func (b *ParameterBuffer) Retire() {
	b.writer = codec.ParamWriter{}
	clear(b.cells)
	clear(b.positions)
	if cap(b.storage) > 64<<10 {
		b.storage = nil
	} else {
		b.storage = b.storage[:0]
	}
	if b.RetainedBytes() > 64<<10 {
		b.cells = nil
		b.positions = nil
	}
}
