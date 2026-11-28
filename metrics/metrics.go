// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package metrics describes the counters a session keeps, session-wide and
// per host.
//
// Session.Metrics returns a Snapshot of the session's, and
// Session.HostMetrics a Host for each host the session has a pool for. The counters
// cover every execution of the session, of every statement and batch: they
// start at zero when the session is opened and only grow, except InFlight,
// Connections and the latency percentiles, which report the present.
// Reading them is cheap, but a Snapshot is a copy: read it again to see later
// executions. Per-request detail goes to observe.Hooks instead.
//
// Package cqltrace's reads of trace sessions are not counted as requests,
// pages, attempts, retries or errors, in the latency percentiles or in a
// Host, so reading a trace does not change the metrics it may be compared
// with.
package metrics

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
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
	// once, and is nil before the first error; ErrorsOf reads one kind.
	ErrorsByKind map[cqlerr.Kind]uint64
	// Requests counts executions started, Pages the pages they fetched, the
	// first included, and Attempts the attempts they sent to nodes,
	// including retries and speculative attempts. An execution refused
	// because the session is closing is not counted.
	Requests, Pages, Attempts uint64
	// Errors counts executions that ended with an error. Canceled and
	// Timeouts break two kinds out of them, and repeat counts ErrorsByKind
	// also holds, rather than adding to it: Canceled is
	// ErrorsOf(cqlerr.KindCanceled), the caller's context was canceled or
	// its own deadline passed, and Timeouts is ErrorsOf(cqlerr.KindTimeout),
	// the server or the driver gave up waiting.
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
	InFlight int
	// Connections is the number of connections the session's pools hold to
	// nodes, the control connection not included. Session.Metrics counts
	// them host by host, so reading it costs a lock per host.
	Connections int
	// LatencyP50, LatencyP95 and LatencyP99 are percentiles of the latency
	// of the pages that succeeded in the last ten seconds, at most 1024 of
	// them, newer seconds first, recomputed once a second. A page's latency
	// runs from sending its first attempt to the response that served it,
	// retries and speculative attempts included; the time the caller spends
	// reading rows is not, so a speculation policy reading them waits for
	// the cluster, not for the application. In a second with more than 1024
	// pages, the samples are spread over its requests rather than being the
	// latest 1024 to finish. Failed pages are not included. When none
	// succeeded since the last computation, the previous values stay. They
	// are zero until the first computation.
	//
	// They are meant for decisions that follow the cluster's current
	// latency, such as a retry.SpeculationPolicy that waits LatencyP95
	// before speculating (retry.Speculation carries them), and for a quick look at a live session. They are
	// not a latency histogram: a sample of ten seconds of successes cannot
	// be summed across sessions or over time. To chart latency, record
	// each request's latency in a histogram of your own from an
	// observe.Hooks Request hook, as Example_prometheus in package observe
	// does.
	LatencyP50, LatencyP95, LatencyP99 time.Duration
}

// ErrorsOf returns how many executions ended with an error of kind, as
// ErrorsByKind counts them.
func (s Snapshot) ErrorsOf(kind cqlerr.Kind) uint64 { return s.ErrorsByKind[kind] }

// Host is a copy of the counters a session keeps for one host, at one
// moment. The counters start at zero when the session opens a pool for the
// host, and start again if the host leaves the cluster and returns.
//
// Later releases may add fields to Host, as to Snapshot, so read its fields
// by name.
//
// The zero Host counts nothing.
type Host struct {
	// Address is the host:port the session connects to.
	Address string
	// HostID is the host's ID.
	HostID cql.UUID
	// Attempts counts the attempts sent to the host, retries and
	// speculative attempts included. Errors counts those of them that
	// failed, and Timeouts those of the failures whose error is
	// cqlerr.KindTimeout.
	Attempts, Errors, Timeouts uint64
	// Connections is the number of connections the host's pool holds, and
	// InFlight the requests in flight on them.
	Connections, InFlight int
}
