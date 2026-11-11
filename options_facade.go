// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	implBinding "github.com/scylladb/gocql/v2/internal/binding"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	"github.com/scylladb/gocql/v2/retry"
)

// Options overrides execution settings. Its zero value uses session defaults.
// Query constructors validate and snapshot Options once. Compile recurring
// overrides with Session.CompileOptions for concurrent one-shot calls. Use context
// deadlines for request time limits without rebuilding options.
type Options struct {
	Retry    retry.Policy
	Observer Observer
	// Profile replaces the session's default profile before other overrides.
	// It must come from ProfileBuilder.Build; a Profile literal is rejected.
	Profile  *Profile
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
	Trace             bool
}

// SerialOverride chooses the serial consistency an execution sends, relative
// to its profile's Profile.SerialConsistency. The zero value inherits it.
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

// CompiledOptions is an immutable snapshot made by Session.CompileOptions. Reuse it
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
	Trace    bool
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

func internalOptions(o Options) implSession.Options {
	v := implSession.Options{
		Retry: o.Retry, Observer: o.Observer, Keyspace: o.Keyspace,
		CustomPayload: o.CustomPayload, Timeout: o.Timeout, PageTimeout: o.PageTimeout,
		Prefetch: o.Prefetch, Timestamp: o.Timestamp, PageSize: o.PageSize,
		NowInSeconds: o.NowInSeconds, ShardID: o.ShardID,
		SerialConsistency: implSession.SerialOverride(o.SerialConsistency), Consistency: o.Consistency,
		HostID: o.HostID, IncludeMetadata: o.IncludeMetadata,
		Idempotent: o.Idempotent, Trace: o.Trace,
	}
	if o.Profile != nil {
		// The only copy of the caller's profile: compiling keeps it.
		profile := internalProfile(*o.Profile)
		v.Profile = &profile
	}
	return v
}

func publicOptions(o implSession.Options) Options {
	v := Options{
		Retry: o.Retry, Observer: o.Observer, Keyspace: o.Keyspace,
		CustomPayload: o.CustomPayload, Timeout: o.Timeout, PageTimeout: o.PageTimeout,
		Prefetch: o.Prefetch, Timestamp: o.Timestamp, PageSize: o.PageSize,
		NowInSeconds: o.NowInSeconds, ShardID: o.ShardID,
		SerialConsistency: SerialOverride(o.SerialConsistency), Consistency: o.Consistency,
		HostID: o.HostID, IncludeMetadata: o.IncludeMetadata,
		Idempotent: o.Idempotent, Trace: o.Trace,
	}
	if o.Profile != nil {
		profile := publicProfile(*o.Profile)
		v.Profile = &profile
	}
	return v
}

func internalStreamOptions(o StreamOptions) implSession.StreamOptions {
	return implSession.StreamOptions{Options: internalOptions(o.Options), Limits: o.Limits, Incremental: o.Incremental}
}

func internalPrepareOptions(options []PrepareOptions) (implSession.PrepareOptions, error) {
	if len(options) > 1 {
		return implSession.PrepareOptions{}, &cqlerr.ValidationError{Field: "options", Err: errors.New("at most one PrepareOptions")}
	}
	if len(options) == 0 {
		return implSession.PrepareOptions{}, nil
	}
	o := options[0]
	return implSession.PrepareOptions{Keyspace: o.Keyspace, HostID: o.HostID, Trace: o.Trace}, nil
}

// internalOptionList converts the optional Options of a throwaway call; the
// internal call rejects more than one.
func internalOptionList(options []Options) []implSession.Options {
	if len(options) == 0 {
		return nil
	}
	out := make([]implSession.Options, len(options))
	for i, o := range options {
		out[i] = internalOptions(o)
	}
	return out
}

func internalCompiledOptions(options []CompiledOptions) (implSession.CompiledOptions, error) {
	if len(options) > 1 {
		return implSession.CompiledOptions{}, &cqlerr.ValidationError{Field: "options", Err: errors.New("at most one CompiledOptions")}
	}
	if len(options) == 0 {
		return implSession.CompiledOptions{}, nil
	}
	return implSession.CompiledOptions(options[0]), nil
}

func internalStatement[P, R any](s cql.Statement[P, R]) implBinding.Statement[P, R] {
	return implBinding.Statement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, BindMarkers: s.BindMarkers, Kind: s.Kind}
}

func internalStreamStatement[P, R any](s cql.StreamStatement[P, R]) implSession.StreamStatement[P, R] {
	return implSession.StreamStatement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, Kind: s.Kind}
}
