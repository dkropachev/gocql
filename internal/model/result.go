// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"slices"
	"time"
)

// Result describes a completed execution. Warnings, trace IDs and custom
// payload are rare, so they live behind one pointer that is nil when absent;
// read them with the methods of the same names. A Result is a value: copies
// never observe each other's later additions.
type Result struct {
	HostID              UUID
	Attempts            int
	Latency             time.Duration
	Rows                int64
	AchievedConsistency Consistency
	diag                *resultDiagnostics
}

type resultDiagnostics struct {
	warnings      []string
	traceIDs      []UUID
	customPayload []PayloadEntry
}

// Warnings returns the server warnings of all attempts and pages.
func (r Result) Warnings() []string {
	if r.diag == nil {
		return nil
	}
	return r.diag.warnings
}

// TraceIDs returns the trace session IDs of traced attempts.
func (r Result) TraceIDs() []UUID {
	if r.diag == nil {
		return nil
	}
	return r.diag.traceIDs
}

// CustomPayload returns the custom payload entries of all responses.
func (r Result) CustomPayload() []PayloadEntry {
	if r.diag == nil {
		return nil
	}
	return r.diag.customPayload
}

// own replaces r's diagnostics with a private copy before a change; slices are
// clipped so that appends never write into storage another copy shares.
func (r *Result) own() *resultDiagnostics {
	d := &resultDiagnostics{}
	if r.diag != nil {
		d.warnings = slices.Clip(r.diag.warnings)
		d.traceIDs = slices.Clip(r.diag.traceIDs)
		d.customPayload = slices.Clip(r.diag.customPayload)
	}
	r.diag = d
	return d
}

// AddWarnings appends server warnings.
func (r *Result) AddWarnings(warnings ...string) {
	if len(warnings) > 0 {
		d := r.own()
		d.warnings = append(d.warnings, warnings...)
	}
}

// AddTraceIDs appends trace session IDs.
func (r *Result) AddTraceIDs(ids ...UUID) {
	if len(ids) > 0 {
		d := r.own()
		d.traceIDs = append(d.traceIDs, ids...)
	}
}

// AddCustomPayload appends custom payload entries.
func (r *Result) AddCustomPayload(entries ...PayloadEntry) {
	if len(entries) > 0 {
		d := r.own()
		d.customPayload = append(d.customPayload, entries...)
	}
}

// SetDiagnostics replaces warnings, trace IDs and custom payload, taking
// ownership of the slices.
func (r *Result) SetDiagnostics(warnings []string, traceIDs []UUID, customPayload []PayloadEntry) {
	if len(warnings) == 0 && len(traceIDs) == 0 && len(customPayload) == 0 {
		r.diag = nil
		return
	}
	r.diag = &resultDiagnostics{warnings: warnings, traceIDs: traceIDs, customPayload: customPayload}
}

func CloneResult(result Result) Result {
	if result.diag != nil {
		result.diag = &resultDiagnostics{
			warnings:      append([]string(nil), result.diag.warnings...),
			traceIDs:      append([]UUID(nil), result.diag.traceIDs...),
			customPayload: append([]PayloadEntry(nil), result.diag.customPayload...),
		}
	}
	return result
}
func MergeResult(dst *Result, src Result) {
	dst.HostID = src.HostID
	dst.Attempts += src.Attempts
	dst.Latency += src.Latency
	dst.Rows += src.Rows
	dst.AddWarnings(src.Warnings()...)
	dst.AddTraceIDs(src.TraceIDs()...)
	dst.AddCustomPayload(src.CustomPayload()...)
	dst.AchievedConsistency = src.AchievedConsistency
}

type PageState struct {
	token []byte
	kind  uint8
}

func StartPage() PageState { return PageState{} }
func RestorePageState(token []byte) PageState {
	return PageState{token: append([]byte{}, token...), kind: 1}
}
func (p PageState) More() bool     { return p.kind == 1 }
func (p PageState) Complete() bool { return p.kind == 2 }
func (p PageState) Bytes() []byte {
	if p.kind != 1 {
		return nil
	}
	return append([]byte{}, p.token...)
}
func CompletePageState() PageState { return PageState{kind: 2} }
