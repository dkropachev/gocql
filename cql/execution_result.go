// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/base64"
	"slices"
	"time"
)

// Result describes a completed execution. Warnings, trace IDs and custom
// payload are rare, so they live behind one pointer that is nil when absent;
// read them with the methods of the same names. A Result is a value: copies
// never observe each other's later additions.
//
// The zero Result is empty: no warnings, CQL traces or custom payload.
type Result struct {
	diag *resultDiagnostics
	// HostID is the host ID of the coordinator of the last completed attempt.
	HostID UUID
	// Attempts is the number of attempts started, speculative ones included.
	Attempts int
	// Latency is the sum of the latencies of the completed attempts.
	Latency time.Duration
	// Rows is the number of rows the response held, or that a stream
	// delivered.
	Rows int64
	// AchievedConsistency is the consistency level of the last completed
	// attempt, which a retry policy may have lowered.
	AchievedConsistency Consistency
}

type resultDiagnostics struct {
	warnings      []string
	traces        []CQLTraceRef
	customPayload []PayloadEntry
}

// CQLTraceRef names a server-side CQL trace session and the attempt it
// records. Read the session with package cqltrace.
//
// The zero CQLTraceRef names no session.
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
func (r *Result) addWarnings(warnings ...string) {
	if len(warnings) > 0 {
		d := r.own()
		d.warnings = append(d.warnings, warnings...)
	}
}

// AddCQLTraces appends CQL trace session references.
func (r *Result) addCQLTraces(traces ...CQLTraceRef) {
	if len(traces) > 0 {
		d := r.own()
		d.traces = append(d.traces, traces...)
	}
}

// AddCustomPayload appends custom payload entries.
func (r *Result) addCustomPayload(entries ...PayloadEntry) {
	if len(entries) > 0 {
		d := r.own()
		d.customPayload = append(d.customPayload, entries...)
	}
}

// SetDiagnostics replaces warnings, trace references and custom payload,
// taking ownership of the slices.
func (r *Result) setDiagnostics(warnings []string, traces []CQLTraceRef, customPayload []PayloadEntry) {
	if len(warnings) == 0 && len(traces) == 0 && len(customPayload) == 0 {
		r.diag = nil
		return
	}
	r.diag = &resultDiagnostics{warnings: warnings, traces: traces, customPayload: customPayload}
}

func cloneResult(result Result) Result {
	if result.diag != nil {
		result.diag = &resultDiagnostics{
			warnings:      append([]string(nil), result.diag.warnings...),
			traces:        append([]CQLTraceRef(nil), result.diag.traces...),
			customPayload: append([]PayloadEntry(nil), result.diag.customPayload...),
		}
	}
	return result
}
func mergeResult(dst *Result, src Result) {
	dst.HostID = src.HostID
	dst.Attempts += src.Attempts
	dst.Latency += src.Latency
	dst.Rows += src.Rows
	dst.addWarnings(src.Warnings()...)
	dst.addCQLTraces(src.CQLTraces()...)
	dst.addCustomPayload(src.CustomPayload()...)
	dst.AchievedConsistency = src.AchievedConsistency
}

// PageState distinguishes the initial page, a continuation (whose token may be
// empty), and completion. Its zero value starts paging.
//
// A continuation's token is the server's paging state, opaque to the driver:
// the driver sends it back unchanged and does not check where it came from.
// It may reveal partition and clustering key values of the rows around the
// page boundary. Before handing a token to an untrusted client, for example as
// a cursor in a web API, authenticate it (an HMAC over the token and the
// statement it belongs to) or encrypt it, and verify it before
// PageStateFromToken; a client could otherwise read key values from it or
// forge one to resume another query's paging. A token resumes the same
// statement with the same parameters on any session of the same cluster and
// protocol version. RunPage rejects a token longer than the session's paging
// state size limit (config.ConfigBuilder.WithMaxPagingStateSize) with an error
// matching cqlerr.ErrPagingStateLimit.
type PageState struct {
	token []byte
	kind  uint8
}

func startPage() PageState { return PageState{} }

// PageStateFromToken copies token into a continuation state. Nil and empty
// inputs both represent an empty continuation. Use StartPage() to begin paging;
// use PageState.More and PageState.Complete to distinguish continuation and completion.
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
func completePageState() PageState { return PageState{kind: 2} }

// MarshalBinary encodes the whole state, start and completion included: one
// byte for its kind, then a continuation's token. UnmarshalBinary reads it
// back. The encoding carries the token, so it needs the same care before it
// reaches an untrusted client.
func (p PageState) MarshalBinary() ([]byte, error) {
	b := make([]byte, 1, 1+len(p.token))
	b[0] = p.kind
	if p.kind == 1 {
		b = append(b, p.token...)
	}
	return b, nil
}

// UnmarshalBinary decodes a state MarshalBinary encoded, copying its token.
// Any other input is rejected with a ValidationError.
func (p *PageState) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return typeValidation("page state", "empty encoding")
	}
	switch kind := data[0]; {
	case kind == 1:
		*p = PageStateFromToken(data[1:])
	case kind > 2:
		return typeValidation("page state", "unknown kind %d", kind)
	case len(data) > 1:
		return typeValidation("page state", "token after a start or complete state")
	default:
		*p = PageState{kind: kind}
	}
	return nil
}

// MarshalText encodes the state as the unpadded URL-safe base64 of its
// MarshalBinary encoding, fit for a URL or JSON cursor.
func (p PageState) MarshalText() ([]byte, error) {
	b, _ := p.MarshalBinary()
	return base64.RawURLEncoding.AppendEncode(nil, b), nil
}

// UnmarshalText decodes a state MarshalText encoded. Any other input is
// rejected with a ValidationError.
func (p *PageState) UnmarshalText(text []byte) error {
	b, err := base64.RawURLEncoding.AppendDecode(nil, text)
	if err != nil {
		return &ValidationError{Field: "page state", Err: err}
	}
	return p.UnmarshalBinary(b)
}
