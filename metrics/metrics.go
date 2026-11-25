// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metrics describes the session-wide counters a session keeps.
//
// Session.Metrics returns a Snapshot of them. The counters
// cover every execution of the session, of every statement and batch: they
// start at zero when the session is opened and only grow, except InFlight,
// Connections and the latency percentiles, which report the present.
// Reading them is cheap, but a Snapshot is a copy: read it again to see later
// executions. Per-request detail goes to observe.Hooks instead.
package metrics

import (
	"time"

	"github.com/scylladb/gocql/v2/cqlerr"
)

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
	// ErrorsByKind counts the executions that ended with an error by
	// cqlerr.KindOf of the error. It holds only the kinds counted at least
	// once, and is nil before the first error.
	ErrorsByKind map[cqlerr.Kind]uint64
	// Requests counts executions started, and Attempts the attempts they
	// sent to nodes, including retries and speculative attempts. An
	// execution refused because the session is closing is not counted.
	Requests, Attempts uint64
	// Errors counts executions that ended with an error. Canceled counts
	// those of them whose error is cqlerr.KindCanceled: the caller canceled
	// the context. Timeouts counts those whose error is cqlerr.KindTimeout:
	// the server or the driver gave up waiting, or the caller's deadline
	// passed.
	Errors, Canceled, Timeouts uint64
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
	// Connections is the number of connections the session's pools hold to
	// nodes, the control connection not included. Session.Metrics counts
	// them host by host, so reading it costs a lock per host.
	Connections int
	// LatencyP50, LatencyP95 and LatencyP99 are percentiles of the latency
	// of the executions that succeeded in the last ten seconds, at most 1024
	// of them, newer seconds first, recomputed once a second. In a second
	// with more than 1024 requests, the samples are spread over its requests
	// rather than being the latest 1024 to finish. Failed executions are
	// not included. When none succeeded since the last computation, the
	// previous values stay. They are zero until the first computation.
	//
	// They are meant for decisions that follow the cluster's current
	// latency, such as a retry.SpeculationPolicy that waits LatencyP95
	// before speculating, and for a quick look at a live session. They are
	// not a latency histogram: a sample of ten seconds of successes cannot
	// be summed across sessions or over time. To chart latency, record
	// each request's latency in a histogram of your own from an
	// observe.Hooks Request hook, as Example_prometheus in package observe
	// does.
	LatencyP50, LatencyP95, LatencyP99 time.Duration
}
