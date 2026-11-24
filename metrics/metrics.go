// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metrics describes the session-wide counters a session keeps.
//
// Session.Metrics returns a Snapshot of them. The counters
// cover every execution of the session, of every statement and batch: they
// start at zero when the session is opened and only grow, except InFlight
// and the latency percentiles, which report the present.
// Reading them is cheap, but a Snapshot is a copy: read it again to see later
// executions. Per-request detail goes to an observe.Observer instead.
package metrics

import "time"

// Snapshot is a copy of a session's counters at one moment.
//
// Later releases may add fields to Snapshot, and that is not a breaking
// change. Read its fields by name: do not build a Snapshot with an unkeyed
// composite literal, and do not compare whole Snapshots with == or use them
// as map keys, since a field added later may not be comparable or may make
// two otherwise equal snapshots differ.
//
// The zero Snapshot counts nothing.
type Snapshot struct {
	// Requests counts executions started, and Attempts the attempts they
	// sent to nodes, including retries and speculative attempts. An
	// execution refused because the session is closing is not counted.
	Requests, Attempts uint64
	// Errors counts executions that ended with an error, and Canceled
	// those of them that ended because their context was canceled or its
	// deadline passed.
	Errors, Canceled uint64
	// Retries counts attempts the retry policy asked for, and Speculations
	// the speculative attempts the speculation policy started.
	Retries, Speculations uint64
	// Orphans counts requests abandoned while a node may still answer them,
	// such as attempts sent and then cancelled, including speculative
	// attempts that lost, or timed out by the client. Each holds its stream
	// until the answer arrives or the connection closes.
	Orphans uint64
	// InFlight is the number of executions started and not yet finished.
	InFlight int64
	// LatencyP50, LatencyP95 and LatencyP99 are percentiles of the latency
	// of the executions that succeeded in the last ten seconds, at most 1024
	// of them, newer seconds first, recomputed once a second. In a second
	// with more than 1024 requests, the samples are spread over its requests
	// rather than being the latest 1024 to finish. Failed executions are
	// not included. When none succeeded since the last computation, the
	// previous values stay. They are zero until the first computation.
	LatencyP50, LatencyP95, LatencyP99 time.Duration
}
