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
// true, a non-nil Retry, and a SerialConsistency other than SerialInherit.
//
// Time limits combine rather than replace each other. The call's context,
// the profile's Timeout and Options.Timeout all bound the whole execution,
// retries and every page Rows fetches included, and whichever expires first
// ends it. Options.Timeout therefore only tightens the profile's limit: a
// longer one, or a zero one, leaves the profile's in force. PageTimeout
// combines the same way, the earliest of the profile's and
// Options.PageTimeout bounding each page fetch on its own, within the
// execution's deadline.
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
	Retry    retry.Policy
	Observer observe.Observer
	// Profile replaces the session's default profile before other overrides.
	// It must come from config.ProfileBuilder.Build; a config.Profile literal
	// is rejected.
	Profile  *config.Profile
	Keyspace string
	// CustomPayload is copied, including borrowed payload bytes, when compiled.
	CustomPayload []cql.PayloadEntry
	// Timeout and PageTimeout add a limit to the profile's: the earlier one
	// applies, so a longer or zero value leaves the profile's limit in force.
	Timeout, PageTimeout cql.Option[time.Duration]
	Prefetch             cql.Option[int]
	// Timestamp is the write timestamp in microseconds since the Unix epoch,
	// the unit CQL's USING TIMESTAMP takes. Note that cql.Timestamp values
	// are milliseconds: multiply one by 1000 to use it here.
	Timestamp cql.Option[int64]
	// PageSize is the rows per page; cql.Unpaged requests the whole result
	// in one response.
	PageSize cql.Option[int32]
	// NowInSeconds sets the server's current time, in seconds since the Unix
	// epoch, for TTL and expiry. Only native protocol 5 carries it: on
	// another dialect compiling or executing the options fails with a
	// cqlerr.UnsupportedSettingError, and nothing is sent.
	NowInSeconds cql.Option[int32]
	// ShardID sends the request on a connection of this shard of the node
	// HostID names; it requires HostID. Only ScyllaDB has shards: on another
	// dialect compiling or executing the options fails with a
	// cqlerr.UnsupportedSettingError, and nothing is sent.
	ShardID     cql.Option[uint32]
	Consistency cql.Option[cql.Consistency]
	HostID      cql.Option[cql.UUID]
	// SerialConsistency overrides the profile's serial consistency of
	// conditional statements. The zero value, SerialInherit, keeps it.
	SerialConsistency SerialOverride
	IncludeMetadata   bool
	Idempotent        bool
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
	Keyspace string
	HostID   cql.Option[cql.UUID]
	// CQLTrace records a server-side CQL trace session of the PREPARE. The
	// driver does not report its session ID.
	CQLTrace bool
}

// StreamOptions chooses caller-storage decoding and its receive mode. The zero
// value buffers one response page on a multiplexed connection. Incremental
// reserves a connection for bounded receive scratch. Both modes require seeded
// output capacity and disable prefetch and speculation.
type StreamOptions struct {
	Options     Options
	Limits      StreamLimits
	Incremental bool
}

// StreamLimits bounds the receive scratch of an incremental stream.
//
// Zero fields take defaults, so the zero StreamLimits is ready to use.
type StreamLimits struct {
	// MaxMetadataBytes caps a page's result metadata. Default 1 MiB.
	MaxMetadataBytes int64
	// ReadAheadBytes is how much the stream reads ahead of the decoder.
	// Default 32 KiB.
	ReadAheadBytes int
	// DiscardBytes is the scratch used to skip unread data.
	DiscardBytes int
	// MaxTransformBytes caps the scratch a decompressing stream holds.
	MaxTransformBytes int64
}

func publicOptions(o implSession.Options) Options {
	v := Options{
		Retry: o.Retry, Observer: o.Observer, Keyspace: o.Keyspace,
		CustomPayload: o.CustomPayload, Timeout: o.Timeout, PageTimeout: o.PageTimeout,
		Prefetch: o.Prefetch, Timestamp: o.Timestamp, PageSize: o.PageSize,
		NowInSeconds: o.NowInSeconds, ShardID: o.ShardID,
		SerialConsistency: SerialOverride(o.SerialConsistency), Consistency: o.Consistency,
		HostID: o.HostID, IncludeMetadata: o.IncludeMetadata,
		Idempotent: o.Idempotent, CQLTrace: o.CQLTrace,
	}
	if o.Profile != nil {
		profile := implSettings.InternalProfileToPublic[config.Profile](*o.Profile)
		v.Profile = &profile
	}
	return v
}
