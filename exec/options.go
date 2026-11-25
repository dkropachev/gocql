// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package exec holds the per-execution settings of gocql statements: Options
// and its compiled form, the options of preparation and of streaming queries.
//
// # Which setting wins
//
// A request starts from the session's default profile
// (config.ConfigBuilder.WithDefaultProfile). Options.Profile, when set,
// replaces that profile whole, and then each Options field that is set
// overrides the matching field of the profile: a cql.Option whose Set is
// true, a non-nil Retry, a non-zero PageSize, and a SerialConsistency other
// than SerialInherit. Idempotent, when set, overrides the statement's
// cql.Statement.Idempotent.
//
// Options.Timeout, when set, replaces the profile's Timeout, so it can
// lengthen the limit as well as shorten it, and cql.Some(time.Duration(0))
// removes it. The resulting limit and the call's context both bound the whole
// execution, retries and every page Rows fetches included, and whichever
// expires first ends it. Options.PageTimeout replaces the profile's
// PageTimeout the same way; it bounds each page fetch on its own, within the
// execution's deadline.
//
// Preparation takes no Options. Session.Prepare and Session.PrepareStream are
// bounded by their context and the session's default profile Timeout,
// whichever expires first. A statement the driver prepares again during an
// execution, after an UNPREPARED error or on a host that has not seen it, is
// bounded by that execution's deadline.
package exec

import (
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
)

// Options overrides execution settings. Its zero value uses session defaults.
// Query constructors validate and snapshot Options once. Compile recurring
// overrides with gocql.Session.CompileOptions for concurrent one-shot calls.
// Use context deadlines for request time limits without rebuilding options.
type Options struct {
	// Retry replaces the profile's retry policy; nil keeps it. Use
	// retry.Never{} to disable retries.
	Retry retry.Policy
	// Observer replaces the session's observer for the execution; nil keeps
	// it. To add to the session's observer rather than replace it, join the
	// two: observe.Join(sessionObserver, extra), where sessionObserver is
	// the one given to config.ConfigBuilder.WithObserver.
	Observer observe.Observer
	// Profile replaces the session's default profile before other overrides.
	// It must come from config.ProfileBuilder.Build; a config.Profile literal
	// is rejected.
	Profile *config.Profile
	// Keyspace replaces the session's keyspace for the execution; empty keeps
	// it.
	Keyspace string
	// Timestamp, when set, is the write timestamp of the execution, sent in
	// microseconds since the Unix epoch, the unit of CQL's USING TIMESTAMP;
	// finer precision is dropped. It replaces the session's client
	// timestamp. A worker query from Prepared.Query or PreparedStream.Query
	// would stamp every write it runs with the same time, so they reject
	// it with a cql.ValidationError: pass it per call, through
	// Session.CompileOptions.
	Timestamp cql.Option[time.Time]
	// CustomPayload is copied, including borrowed payload bytes, when compiled.
	CustomPayload []cql.PayloadEntry
	// Timeout and PageTimeout, when set, replace the profile's limits:
	// Timeout bounds the whole execution, retries and pages included, and
	// PageTimeout each page fetch. A set zero means no limit.
	Timeout, PageTimeout cql.Option[time.Duration]
	// Prefetch replaces the profile's prefetch, zero or one, when set.
	Prefetch cql.Option[int]
	// PageSize is the rows per page; zero keeps the profile's, and
	// cql.Unpaged requests the whole result in one response. A negative
	// value is rejected.
	PageSize int32
	// NowInSeconds sets the server's current time, in seconds since the Unix
	// epoch, for TTL and expiry. Only native protocol 5 carries it: on a
	// session of another dialect, Session.CompileOptions and every execution
	// fail with a cqlerr.UnsupportedSettingError, and nothing is sent.
	NowInSeconds cql.Option[int32]
	// ShardID sends the request on a connection of this shard of the node
	// HostID names; it requires HostID. Only ScyllaDB has shards: on a
	// session of another dialect, Session.CompileOptions and every execution
	// fail with a cqlerr.UnsupportedSettingError, and nothing is sent.
	ShardID cql.Option[uint32]
	// Consistency replaces the profile's consistency level when set.
	Consistency cql.Option[cql.Consistency]
	// HostID sends the request to the host with this host ID when set.
	HostID cql.Option[cql.UUID]
	// SerialConsistency overrides the profile's serial consistency of
	// conditional statements. The zero value, SerialInherit, keeps it.
	SerialConsistency SerialOverride
	// IncludeMetadata asks the server for full result metadata even when the
	// driver has validated metadata cached.
	IncludeMetadata bool
	// Idempotent, when set, replaces the statement's cql.Statement.Idempotent
	// for the execution: cql.Some(true) marks it safe to run more than once,
	// cql.Some(false) marks it unsafe. A batch is idempotent when every
	// member is, unless this overrides it.
	Idempotent cql.Option[bool]
	// CQLTrace asks the cluster to record a CQL trace session of every
	// attempt in system_traces. cql.Result.CQLTraces names the sessions, failed
	// attempts' included; package cqltrace reads them. This is server-side tracing, unrelated to client-side
	// instrumentation such as OpenTelemetry, which hooks observe.Observer.
	CQLTrace bool
}

// SerialOverride chooses the serial consistency an execution sends, relative to
// its profile's config.Profile.SerialConsistency. The zero value,
// SerialInherit, inherits it. Later releases may add values, so a switch over
// them needs a default case.
type SerialOverride uint8

const (
	// SerialInherit keeps the profile's serial consistency.
	SerialInherit = SerialOverride(implSession.SerialInherit)
	// SerialSerial sends SERIAL.
	SerialSerial = SerialOverride(implSession.SerialSerial)
	// SerialLocal sends LOCAL_SERIAL.
	SerialLocal = SerialOverride(implSession.SerialLocal)
	// SerialOmit sends no serial consistency, so the server applies its own
	// default (SERIAL).
	SerialOmit = SerialOverride(implSession.SerialOmit)
)

// String returns the serial override name, or SerialOverride(n) for a value
// it does not define.
func (s SerialOverride) String() string {
	switch s {
	case SerialInherit:
		return "inherit"
	case SerialSerial:
		return "serial"
	case SerialLocal:
		return "local serial"
	case SerialOmit:
		return "omit"
	default:
		return fmt.Sprintf("SerialOverride(%d)", uint8(s))
	}
}

// CompiledOptions is an immutable snapshot made by
// gocql.Session.CompileOptions. Reuse it for recurring one-shot execution
// settings; the zero value uses session defaults. Compiling validates
// overrides, copies payloads and precomputes request settings. Reuse on another
// session resolves the original overrides against that session's defaults
// instead. CompiledOptions controls execution; PrepareOptions controls
// statement preparation.
type CompiledOptions implSession.CompiledOptions

// Values returns an independent copy of the original overrides for deriving
// another set. It does not include inherited session defaults. Policy and
// observer objects remain shared and must support concurrent use.
func (o CompiledOptions) Values() Options {
	return publicOptions(implSession.CompiledOptions(o).Values())
}

// PrepareOptions selects the keyspace, host and tracing of preparation.
// The zero value uses the session's keyspace and any eligible host.
// Use CompiledOptions for execution settings.
type PrepareOptions struct {
	// Keyspace is the keyspace to prepare in; empty uses the session's.
	Keyspace string
	// HostID prepares on the host with this host ID when set, and on any
	// eligible host otherwise.
	HostID cql.Option[cql.UUID]
	// CQLTrace records a server-side CQL trace session of the PREPARE. The
	// driver does not report its session ID.
	CQLTrace bool
}

// StreamOptions chooses caller-storage decoding and its receive mode. The zero
// value buffers one response page on a multiplexed connection. Incremental
// reserves a connection for bounded receive scratch. Both modes require seeded
// output capacity and disable prefetch and speculation. Cancelling a stream
// while it waits for a response never reuses its stream ID early: a buffered
// stream leaves the ID in use until the response arrives, while an
// incremental stream, which reads its reserved connection itself, closes that
// connection. Closing an incremental stream between rows instead reads the
// rest of a current response of up to 1 MiB and keeps the connection.
//
// Back-pressure comes from paging: a stream requests the next page only when
// NextInto reaches the end of the current one. An incremental stream also
// reads the current response from its socket only as the decoder needs it,
// never in the background, so a slow consumer keeps its reserved connection
// and lets the transport's flow control hold back the server.
type StreamOptions struct {
	// Options holds the execution overrides.
	Options Options
	// Limits bounds an incremental stream's receive scratch.
	Limits StreamLimits
	// Incremental decodes the response as it arrives on a reserved
	// connection, instead of buffering one page.
	Incremental bool
}

// StreamLimits bounds the receive scratch of an incremental stream, which
// holds no response page: its memory is these budgets, whatever the size of
// the result. A buffered stream instead holds one response page at a time,
// bounded by the session's frame and decompression limits. Streams never
// prefetch.
//
// Zero fields take defaults, so the zero StreamLimits is ready to use. A
// negative field, or a MaxTransformBytes below 16, is rejected with a
// cql.ValidationError.
type StreamLimits struct {
	// MaxMetadataBytes caps a page's result metadata. Default 1 MiB.
	MaxMetadataBytes int64
	// ReadAheadBytes is how much the stream reads ahead of the decoder.
	// Default 32 KiB.
	ReadAheadBytes int
	// DiscardBytes is the scratch used to skip unread data. Default 32 KiB.
	DiscardBytes int
	// MaxTransformBytes caps the scratch a decompressing stream holds.
	// Default two maximum protocol v5 segment payloads, about 256 KiB.
	MaxTransformBytes int64
}

func publicOptions(o implSession.Options) Options {
	v := Options{
		Retry: o.Retry, Observer: o.Observer, Keyspace: o.Keyspace,
		CustomPayload: o.CustomPayload, Timeout: o.Timeout, PageTimeout: o.PageTimeout,
		Prefetch: o.Prefetch, PageSize: o.PageSize,
		NowInSeconds: o.NowInSeconds, ShardID: o.ShardID,
		SerialConsistency: SerialOverride(o.SerialConsistency), Consistency: o.Consistency,
		HostID: o.HostID, IncludeMetadata: o.IncludeMetadata,
		Idempotent: o.Idempotent, CQLTrace: o.CQLTrace,
	}
	if o.Timestamp.Set {
		v.Timestamp = cql.Some(time.UnixMicro(o.Timestamp.Value).UTC())
	}
	if o.Profile != nil {
		profile := implSettings.InternalProfileToPublic[config.Profile](*o.Profile)
		v.Profile = &profile
	}
	return v
}
