// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metrics describes the session-wide counters a session keeps.
//
// Session.Metrics returns a Snapshot of them, and a speculative execution
// policy receives one with every SpeculationPolicy.Delay call. The counters
// cover every execution of the session, of every statement and batch: they
// start at zero when the session is opened and only grow, except InFlight
// and the receive cache's current retention, which report the present.
// Reading them is cheap, but a Snapshot is a copy: read it again to see later
// executions. Per-request detail goes to an Observer instead.
package metrics

import "time"

// Snapshot is a copy of a session's counters at one moment.
type Snapshot struct {
	// ReceiveCacheHits and ReceiveCacheMisses count response buffers taken
	// from the session's receive cache or allocated because it had none.
	ReceiveCacheHits, ReceiveCacheMisses uint64
	// ReceiveCacheBytes and ReceiveCacheObjects are the size and number of
	// idle buffers the receive cache holds now. Buffers of response pages
	// still in use are not included.
	ReceiveCacheBytes, ReceiveCacheObjects int
	// Requests counts executions started, and Attempts the requests they
	// sent to nodes, including retries and speculative attempts.
	Requests, Attempts uint64
	// Errors counts executions that ended with an error, and Cancelled
	// those of them that ended because their context was cancelled or its
	// deadline passed.
	Errors, Cancelled uint64
	// Retries counts attempts the retry policy asked for, and Speculations
	// the speculative attempts the speculation policy started.
	Retries, Speculations uint64
	// Orphans is reserved for requests abandoned while a node may still
	// answer them. The session does not count them yet, so it reads zero.
	Orphans uint64
	// InFlight is the number of executions started and not yet finished.
	InFlight int64
	// LatencyP50, LatencyP95 and LatencyP99 are percentiles of the latency
	// of the last 1024 finished executions, recomputed once a second. They
	// are zero until the first computation.
	LatencyP50, LatencyP95, LatencyP99 time.Duration
}
