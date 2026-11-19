// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package codec is the engine's entry to package cql's codec runtime: the
// parameter writers, row readers and plan checks the engine drives but users
// never call. cql registers the implementations in internal/link from its
// init, which runs before this package's variables are initialized, because
// this package imports cql.
package codec

import (
	"reflect"
	"unsafe"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/wire"
)

var (
	newParamWriter          = link.Get[func(*wire.Builder, cql.Columns, bool) *cql.ParamWriter](link.NewParamWriter)
	resetParamWriter        = link.Get[func(*cql.ParamWriter, *wire.Builder, cql.Columns, bool)](link.ResetParamWriter)
	finishParamWriter       = link.Get[func(*cql.ParamWriter) error](link.FinishParamWriter)
	captureParamCells       = link.Get[func(*cql.ParamWriter, unsafe.Pointer, int)](link.CaptureParamCells)
	paramCellType           = link.Get[func() reflect.Type](link.ParamCellType)
	setParamOwnership       = link.Get[func(*cql.ParamWriter, bool)](link.SetParamOwnership)
	readValue               = link.Get[func(*wire.Cursor, cql.DataType, *wire.Lease, bool) (cql.ValueReader, error)](link.ReadValue)
	newValueReader          = link.Get[func(cql.DataType, cql.CellState, wire.Chain, *wire.Lease) cql.ValueReader](link.NewValueReader)
	rowReaderFromCursor     = link.Get[func(cql.Columns, *wire.Cursor, *wire.Lease) cql.RowReader](link.RowReaderFromCursor)
	newRowReader            = link.Get[func(cql.Columns, wire.Chain, *wire.Lease) *cql.RowReader](link.NewRowReader)
	finishRowReader         = link.Get[func(*cql.RowReader) error](link.FinishRowReader)
	beginStreamRow          = link.Get[func(*cql.StreamRowReader)](link.BeginStreamRow)
	newStreamRowReader      = link.Get[func(cql.Columns, *wire.Stream, *cql.StreamRowReader) (*cql.StreamRowReader, error)](link.NewStreamRowReader)
	streamReaderBytes       = link.Get[func(*cql.StreamRowReader) int64](link.StreamReaderStorageBytes)
	detachRow               = link.Get[func(any, any) bool](link.DetachRow)
	managesRowDestination   = link.Get[func(any) bool](link.ManagesRowDestination)
	preservesRowDestination = link.Get[func(any) bool](link.PreservesRowDestination)
	cloneResult             = link.Get[func(cql.Result) cql.Result](link.CloneResult)
	mergeResult             = link.Get[func(cql.Result, cql.Result) cql.Result](link.MergeResult)
	addWarnings             = link.Get[func(cql.Result, []string) cql.Result](link.ResultAddWarnings)
	addCQLTrace             = link.Get[func(cql.Result, cql.CQLTraceRef) cql.Result](link.ResultAddCQLTrace)
	addCustomPayload        = link.Get[func(cql.Result, cql.PayloadEntry) cql.Result](link.ResultAddCustomPayload)
	setDiagnostics          = link.Get[func(cql.Result, []string, []cql.CQLTraceRef, []cql.PayloadEntry) cql.Result](link.ResultSetDiagnostics)
)

// ParamCell is where one encoded parameter landed in a writer's buffer.
type ParamCell struct {
	Start, End int64
	State      cql.CellState
}

// NewParamWriter returns a writer that encodes parameters for columns into
// builder.
func NewParamWriter(builder *wire.Builder, columns cql.Columns, allowUnset bool) *cql.ParamWriter {
	return newParamWriter(builder, columns, allowUnset)
}

// ResetParamWriter prepares w for another statement, keeping its storage.
func ResetParamWriter(w *cql.ParamWriter, builder *wire.Builder, columns cql.Columns, allowUnset bool) {
	resetParamWriter(w, builder, columns, allowUnset)
}

// FinishParamWriter checks that every column was written.
func FinishParamWriter(w *cql.ParamWriter) error { return finishParamWriter(w) }

// CaptureParamCells makes w record where each column's value lands, one cell
// per column.
func CaptureParamCells(w *cql.ParamWriter, cells []ParamCell) {
	// unsafe: ParamCell mirrors cql's paramCell (TestParamCellLayout), and
	// w keeps a typed slice of cells' array, so the array stays reachable.
	captureParamCells(w, unsafe.Pointer(unsafe.SliceData(cells)), len(cells))
}

// SetParamOwnership tells w whether the values it borrows are owned by the
// statement, so it need not copy them.
func SetParamOwnership(w *cql.ParamWriter, owned bool) { setParamOwnership(w, owned) }

// ReadValue reads one framed value of typ from cursor.
func ReadValue(cursor *wire.Cursor, typ cql.DataType, lease *wire.Lease, allowUnset bool) (cql.ValueReader, error) {
	return readValue(cursor, typ, lease, allowUnset)
}

// NewValueReader returns a reader over one value's body.
func NewValueReader(typ cql.DataType, state cql.CellState, body wire.Chain, lease *wire.Lease) cql.ValueReader {
	return newValueReader(typ, state, body, lease)
}

// RowReaderFromCursor returns a reader over the row at cursor.
func RowReaderFromCursor(columns cql.Columns, cursor *wire.Cursor, lease *wire.Lease) cql.RowReader {
	return rowReaderFromCursor(columns, cursor, lease)
}

// NewRowReader returns a reader over a row body.
func NewRowReader(columns cql.Columns, body wire.Chain, lease *wire.Lease) *cql.RowReader {
	return newRowReader(columns, body, lease)
}

// FinishRowReader checks that the row's columns were all consumed.
func FinishRowReader(r *cql.RowReader) error { return finishRowReader(r) }

// BeginStreamRow starts the next row of r.
func BeginStreamRow(r *cql.StreamRowReader) { beginStreamRow(r) }

// NewStreamRowReader returns a streaming reader over scope's rows.
func NewStreamRowReader(columns cql.Columns, scope *wire.Stream) (*cql.StreamRowReader, error) {
	return newStreamRowReader(columns, scope, nil)
}

// NewStreamRowReaderReusingValidation is NewStreamRowReader that shares the
// validation plans of prior, a reader of a preceding page whose columns share
// their backing storage with columns.
func NewStreamRowReaderReusingValidation(columns cql.Columns, scope *wire.Stream, prior *cql.StreamRowReader) (*cql.StreamRowReader, error) {
	return newStreamRowReader(columns, scope, prior)
}

// StreamReaderWorkingStorageBytes reports the working storage r holds.
func StreamReaderWorkingStorageBytes(r *cql.StreamRowReader) int64 { return streamReaderBytes(r) }

// CompileParams compiles encoder's plan for columns.
func CompileParams[P any](encoder cql.ParamEncoder[P], columns cql.Columns) (cql.CompiledParams[P], error) {
	if encoder == nil {
		return nil, cqltype.TypeValidation("encoder", "nil parameter encoder")
	}
	plan, err := encoder.Compile(columns)
	if err == nil && plan == nil {
		err = cqltype.TypeValidation("encoder", "Compile returned a nil parameter plan")
	}
	return plan, err
}

// CompileRow compiles decoder's plan for columns.
func CompileRow[R any](decoder cql.RowDecoder[R], columns cql.Columns) (cql.CompiledRow[R], error) {
	if decoder == nil {
		return nil, cqltype.TypeValidation("decoder", "nil row decoder")
	}
	plan, err := decoder.Compile(columns)
	if err == nil && plan == nil {
		err = cqltype.TypeValidation("decoder", "Compile returned a nil row plan")
	}
	return plan, err
}

// DetachRow makes the fields of the row at dst independent of the response
// before its page is released. Built-in plans detach only borrowed fields and
// keep owned buffers; other plans use their Clone.
func DetachRow[R any](plan cql.CompiledRow[R], dst *R) {
	if !detachRow(plan, dst) {
		*dst = plan.Clone(*dst)
	}
}

// ManagedRowDestination marks an engine row plan whose DecodeInto overwrites
// the whole destination. Embed it in such a plan.
type ManagedRowDestination struct{}

func (ManagedRowDestination) managesRowDestination() {}

// ManagesRowDestination reports whether plan clears, or resets, the whole
// destination itself before decoding a row: a built-in plan of package cql or
// an engine plan that embeds ManagedRowDestination.
func ManagesRowDestination[R any](plan cql.CompiledRow[R]) bool {
	if _, ok := plan.(interface{ managesRowDestination() }); ok {
		return true
	}
	return managesRowDestination(plan)
}

// PreservesRowDestination reports whether plan resets the destination with a
// caller's reset function, which may keep owned slices.
func PreservesRowDestination[R any](plan cql.CompiledRow[R]) bool {
	return preservesRowDestination(plan)
}

// CloneResult returns a copy of result that shares nothing mutable with it.
func CloneResult(result cql.Result) cql.Result { return cloneResult(result) }

// MergeResult adds src's counters and diagnostics to dst.
func MergeResult(dst *cql.Result, src cql.Result) { *dst = mergeResult(*dst, src) }

// AddWarnings appends server warnings to r.
func AddWarnings(r *cql.Result, warnings []string) {
	if len(warnings) > 0 {
		*r = addWarnings(*r, warnings)
	}
}

// AddCQLTrace appends a CQL trace reference to r.
func AddCQLTrace(r *cql.Result, trace cql.CQLTraceRef) { *r = addCQLTrace(*r, trace) }

// AddCustomPayload appends a custom payload entry to r.
func AddCustomPayload(r *cql.Result, entry cql.PayloadEntry) { *r = addCustomPayload(*r, entry) }

// SetDiagnostics replaces r's warnings, traces and custom payload, taking
// ownership of the slices.
func SetDiagnostics(r *cql.Result, warnings []string, traces []cql.CQLTraceRef, payload []cql.PayloadEntry) {
	*r = setDiagnostics(*r, warnings, traces, payload)
}
