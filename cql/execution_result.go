// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/base64"
	"slices"
	"strconv"
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
	// AttemptLatency is the sum of the latencies of the completed
	// attempts, from sending each to its response, over every page. It is
	// not the request's wall time: retries add backoff it leaves out, and
	// speculative attempts overlap, so it can exceed the wall time.
	AttemptLatency time.Duration
	// FetchedRows is the number of rows the responses held, over every
	// page, a stream's included. It counts rows fetched, which can exceed
	// the rows a caller read when iteration stopped early; a stream's
	// counts only the rows its NextInto delivered.
	FetchedRows int64
	// AchievedConsistency is the consistency level of the last completed
	// attempt, which a retry policy may have lowered.
	AchievedConsistency Consistency
	// SpeculationSkipped reports why an execution whose options name a
	// speculation policy (config.Profile.Speculation or
	// exec.Options.Speculation) started no speculative attempt because the
	// request is not eligible, and is SpeculationNotSkipped otherwise.
	// Streams never speculate and do not report it.
	SpeculationSkipped SpeculationSkip
}

// SpeculationSkip says why an execution that has a speculation policy is not
// eligible to speculate; see Result.SpeculationSkipped. Later releases may
// add values, so a switch over them needs a default case.
type SpeculationSkip uint8

const (
	// SpeculationNotSkipped: the execution had no speculation policy, or
	// was eligible to speculate.
	SpeculationNotSkipped SpeculationSkip = iota
	// SpeculationSkippedConditional: the statement is conditional (an LWT,
	// or a SERIAL read), or for a batch one of its statements is. Sending a
	// conditional statement twice could apply it twice.
	SpeculationSkippedConditional
	// SpeculationSkippedNotIdempotent: the request is not idempotent, as
	// the driver resolved it (exec.Options.Idempotent, else the statement's
	// or the profile's).
	SpeculationSkippedNotIdempotent
)

// String returns the reason's name, or SpeculationSkip(n) for a value it
// does not define.
func (s SpeculationSkip) String() string {
	switch s {
	case SpeculationNotSkipped:
		return "not skipped"
	case SpeculationSkippedConditional:
		return "conditional"
	case SpeculationSkippedNotIdempotent:
		return "not idempotent"
	default:
		return "SpeculationSkip(" + strconv.Itoa(int(s)) + ")"
	}
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
	dst.AttemptLatency += src.AttemptLatency
	dst.FetchedRows += src.FetchedRows
	dst.addWarnings(src.Warnings()...)
	dst.addCQLTraces(src.CQLTraces()...)
	dst.addCustomPayload(src.CustomPayload()...)
	dst.AchievedConsistency = src.AchievedConsistency
	if src.SpeculationSkipped != SpeculationNotSkipped {
		dst.SpeculationSkipped = src.SpeculationSkipped
	}
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
// ParsePageState; a client could otherwise read key values from it or
// forge one to resume another query's paging. A token resumes the same
// statement with the same parameters on any session of the same cluster and
// protocol version. RunPage rejects a token longer than the session's paging
// state size limit (config.Limits.MaxPagingStateBytes) with an error
// matching cqlerr.ErrPagingStateLimit.
type PageState struct {
	token []byte
	kind  uint8
}

func startPage() PageState { return PageState{} }

// pageStateFromToken copies token into a continuation state. Nil and empty
// inputs both represent an empty continuation.
func pageStateFromToken(token []byte) PageState {
	return PageState{token: append([]byte{}, token...), kind: 1}
}

// More reports whether this state contains a continuation token.
func (p PageState) More() bool { return p.kind == 1 }

// Complete reports whether the server has finished paging.
func (p PageState) Complete() bool { return p.kind == 2 }

func completePageState() PageState { return PageState{kind: 2} }

// MarshalBinary encodes the whole state, start and completion included. The
// start state encodes as empty, so an absent cursor starts paging; any other
// state is one byte for its kind, then a continuation's token. UnmarshalBinary
// reads it back. The encoding carries the token, so it needs the same care
// before it reaches an untrusted client.
func (p PageState) MarshalBinary() ([]byte, error) {
	if p.kind == 0 {
		return []byte{}, nil
	}
	b := make([]byte, 1, 1+len(p.token))
	b[0] = p.kind
	if p.kind == 1 {
		b = append(b, p.token...)
	}
	return b, nil
}

// UnmarshalBinary decodes a state MarshalBinary encoded, copying its token.
// Empty input is the start state, as is the single zero byte earlier
// releases encoded it as. Any other input is rejected with a
// ValidationError.
func (p *PageState) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		*p = PageState{}
		return nil
	}
	switch kind := data[0]; {
	case kind == 1:
		*p = pageStateFromToken(data[1:])
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
// MarshalBinary encoding, fit for a URL or JSON cursor. The start state
// encodes as empty text.
func (p PageState) MarshalText() ([]byte, error) {
	b, _ := p.MarshalBinary()
	return base64.RawURLEncoding.AppendEncode(nil, b), nil
}

// UnmarshalText decodes a state MarshalText encoded. Empty text is the start
// state, so a request without a cursor starts paging, and "AA", the start
// state of earlier releases, still decodes to it. Any other input is
// rejected with a ValidationError.
func (p *PageState) UnmarshalText(text []byte) error {
	b, err := base64.RawURLEncoding.AppendDecode(nil, text)
	if err != nil {
		return &ValidationError{Field: "page state", Err: err}
	}
	return p.UnmarshalBinary(b)
}
