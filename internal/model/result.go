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
	diag                *resultDiagnostics
	HostID              UUID
	Attempts            int
	Latency             time.Duration
	Rows                int64
	AchievedConsistency Consistency
}

type resultDiagnostics struct {
	warnings      []string
	traces        []CQLTraceRef
	customPayload []PayloadEntry
}

// CQLTraceRef names a server-side CQL trace session and the attempt it
// records. Read the session with package cqltrace.
type CQLTraceRef struct {
	// ID is the trace session ID.
	ID UUID
	// HostID is the node that coordinated the attempt.
	HostID UUID
	// Attempt is the attempt's 1-based number within its page.
	Attempt int
	// Failed reports that the attempt ended in an error, such as a timeout
	// the driver then retried. Every page's last reference that has Failed
	// unset is the attempt that served it.
	Failed bool
	// Speculative reports that the attempt was a speculative execution.
	Speculative bool
}

// Warnings returns the server warnings of all attempts and pages.
func (r Result) Warnings() []string {
	if r.diag == nil {
		return nil
	}
	return r.diag.warnings
}

// CQLTraces returns the server-side CQL trace sessions of the execution's
// traced attempts, failed ones included, in the order they finished, page
// after page; package cqltrace reads them.
func (r Result) CQLTraces() []CQLTraceRef {
	if r.diag == nil {
		return nil
	}
	return r.diag.traces
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
		d.traces = slices.Clip(r.diag.traces)
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

// AddCQLTraces appends CQL trace session references.
func (r *Result) AddCQLTraces(traces ...CQLTraceRef) {
	if len(traces) > 0 {
		d := r.own()
		d.traces = append(d.traces, traces...)
	}
}

// AddCustomPayload appends custom payload entries.
func (r *Result) AddCustomPayload(entries ...PayloadEntry) {
	if len(entries) > 0 {
		d := r.own()
		d.customPayload = append(d.customPayload, entries...)
	}
}

// SetDiagnostics replaces warnings, trace references and custom payload,
// taking ownership of the slices.
func (r *Result) SetDiagnostics(warnings []string, traces []CQLTraceRef, customPayload []PayloadEntry) {
	if len(warnings) == 0 && len(traces) == 0 && len(customPayload) == 0 {
		r.diag = nil
		return
	}
	r.diag = &resultDiagnostics{warnings: warnings, traces: traces, customPayload: customPayload}
}

func CloneResult(result Result) Result {
	if result.diag != nil {
		result.diag = &resultDiagnostics{
			warnings:      append([]string(nil), result.diag.warnings...),
			traces:        append([]CQLTraceRef(nil), result.diag.traces...),
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
	dst.AddCQLTraces(src.CQLTraces()...)
	dst.AddCustomPayload(src.CustomPayload()...)
	dst.AchievedConsistency = src.AchievedConsistency
}

// PageState distinguishes the initial page, a continuation (whose token may be
// empty), and completion. Its zero value starts paging.
type PageState struct {
	token []byte
	kind  uint8
}

func StartPage() PageState { return PageState{} }

// PageStateFromToken copies token into a continuation state. Nil and empty
// tokens both represent an empty continuation, never initial or complete states.
func PageStateFromToken(token []byte) PageState {
	return PageState{token: append([]byte{}, token...), kind: 1}
}

// More reports whether this state contains a continuation token.
func (p PageState) More() bool { return p.kind == 1 }

// Complete reports whether the server has finished paging.
func (p PageState) Complete() bool { return p.kind == 2 }

// Token returns an owned copy of a continuation token, including a non-nil
// empty slice for an empty continuation. Initial and complete states return nil;
// use More and Complete to distinguish them. This is not full-state serialization.
func (p PageState) Token() []byte {
	if p.kind != 1 {
		return nil
	}
	return append([]byte{}, p.token...)
}
func CompletePageState() PageState { return PageState{kind: 2} }
