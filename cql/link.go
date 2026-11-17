// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"reflect"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/link"
	"github.com/scylladb/gocql/v2/internal/structmap"
)

// The engine drives the codec runtime through internal/link; see package
// internal/codec, which calls these.
func init() {
	link.Set(link.NewParamWriter, newParamWriter)
	link.Set(link.ResetParamWriter, resetParamWriter)
	link.Set(link.FinishParamWriter, finishParamWriter)
	link.Set(link.CaptureParamCells, func(w *ParamWriter, cells unsafe.Pointer, n int) {
		captureParamCells(w, unsafe.Slice((*paramCell)(cells), n))
	})
	link.Set(link.ParamCellType, func() reflect.Type { return reflect.TypeFor[paramCell]() })
	link.Set(link.SetParamOwnership, setParamOwnership)
	link.Set(link.ReadValue, readValue)
	link.Set(link.NewValueReader, valueReaderFromLease)
	link.Set(link.RowReaderFromCursor, rowReaderFromCursor)
	link.Set(link.NewRowReader, newRowReader)
	link.Set(link.FinishRowReader, finishRowReader)
	link.Set(link.BeginStreamRow, beginStreamRow)
	link.Set(link.NewStreamRowReader, newStreamRowReaderReusingValidation)
	link.Set(link.StreamReaderStorageBytes, streamReaderWorkingStorageBytes)
	link.Set(link.DetachRow, func(plan any, dst unsafe.Pointer) bool {
		p, ok := plan.(interface{ detachRowAt(unsafe.Pointer) })
		if ok {
			p.detachRowAt(dst)
		}
		return ok
	})
	link.Set(link.ManagesRowDestination, func(plan any) bool {
		_, ok := plan.(rowDestinationManager)
		return ok
	})
	link.Set(link.PreservesRowDestination, func(plan any) bool {
		p, ok := plan.(interface{ preserveDestination() bool })
		return ok && p.preserveDestination()
	})
	link.Set(link.BuiltinFieldTypes, func(stream bool) []structmap.BuiltinFieldType {
		if stream {
			return streamBuiltinFieldTypes()
		}
		return builtinFieldTypes()
	})
	link.Set(link.CloneResult, cloneResult)
	link.Set(link.MergeResult, func(dst, src Result) Result { mergeResult(&dst, src); return dst })
	link.Set(link.ResultAddWarnings, func(r Result, warnings []string) Result { r.addWarnings(warnings...); return r })
	link.Set(link.ResultAddCQLTrace, func(r Result, trace CQLTraceRef) Result { r.addCQLTraces(trace); return r })
	link.Set(link.ResultAddCustomPayload, func(r Result, entry PayloadEntry) Result { r.addCustomPayload(entry); return r })
	link.Set(link.ResultSetDiagnostics, func(r Result, warnings []string, traces []CQLTraceRef, payload []PayloadEntry) Result {
		r.setDiagnostics(warnings, traces, payload)
		return r
	})
}
