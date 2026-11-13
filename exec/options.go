// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package exec holds the per-execution settings of gocql statements: Options
// and its compiled form, the options of preparation and of streaming queries.
package exec

import (
	"time"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/retry"
)

// Options overrides execution settings. Its zero value uses session defaults.
// Query constructors validate and snapshot Options once. Compile recurring
// overrides with gocql.Session.CompileOptions for concurrent one-shot calls. Use context
// deadlines for request time limits without rebuilding options.
type Options struct {
	Retry    retry.Policy
	Observer config.Observer
	// Profile replaces the session's default profile before other overrides.
	// It must come from config.ProfileBuilder.Build; a config.Profile literal
	// is rejected.
	Profile  *config.Profile
	Keyspace string
	// CustomPayload is copied, including borrowed payload bytes, when compiled.
	CustomPayload        []cql.PayloadEntry
	Timeout, PageTimeout cql.Option[time.Duration]
	Prefetch             cql.Option[int]
	// Timestamp is the write timestamp in microseconds since the Unix epoch,
	// the unit CQL's USING TIMESTAMP takes. Note that cql.Timestamp values
	// are milliseconds: multiply one by 1000 to use it here.
	Timestamp cql.Option[int64]
	// PageSize is the rows per page; cql.Unpaged requests the whole result
	// in one response.
	PageSize     cql.Option[int32]
	NowInSeconds cql.Option[int32]
	ShardID      cql.Option[uint32]
	Consistency  cql.Option[cql.Consistency]
	HostID       cql.Option[cql.UUID]
	// SerialConsistency overrides the profile's serial consistency of
	// conditional statements. The zero value, SerialInherit, keeps it.
	SerialConsistency SerialOverride
	IncludeMetadata   bool
	Idempotent        bool
	// CQLTrace asks the cluster to record a CQL trace session of every
	// attempt in system_traces. Result.CQLTraceIDs and
	// cqlerr.AttemptFailure.CQLTraceID name the sessions; package cqltrace
	// reads them. This is server-side tracing, unrelated to client-side
	// instrumentation such as OpenTelemetry, which hooks config.Observer.
	CQLTrace bool
}

// SerialOverride chooses the serial consistency an execution sends, relative
// to its profile's config.Profile.SerialConsistency. The zero value inherits it.
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

// CompiledOptions is an immutable snapshot made by gocql.Session.CompileOptions. Reuse it
// for recurring one-shot execution settings; the zero value uses session
// defaults. Compiling validates overrides, copies payloads and precomputes request
// settings. Reuse on another session resolves the original overrides against
// that session's defaults instead.
// CompiledOptions controls execution; PrepareOptions controls statement preparation.
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
type StreamLimits = implTransport.StreamLimits

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
