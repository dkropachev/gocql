// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/stream"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/exec"
	implBinding "github.com/scylladb/gocql/v2/internal/binding"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

func internalOptions(o exec.Options) implSession.Options {
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
		profile := implSettings.InternalProfileFromPublic(*o.Profile)
		v.Profile = &profile
	}
	return v
}

func internalStreamOptions(o exec.StreamOptions) implSession.StreamOptions {
	return implSession.StreamOptions{Options: internalOptions(o.Options), Limits: o.Limits, Incremental: o.Incremental}
}

func internalPrepareOptions(options []exec.PrepareOptions) (implSession.PrepareOptions, error) {
	if len(options) > 1 {
		return implSession.PrepareOptions{}, &cqlerr.ValidationError{Field: "options", Err: errors.New("at most one PrepareOptions")}
	}
	if len(options) == 0 {
		return implSession.PrepareOptions{}, nil
	}
	o := options[0]
	return implSession.PrepareOptions{Keyspace: o.Keyspace, HostID: o.HostID, Trace: o.Trace}, nil
}

// internalOptionList converts the optional exec.Options of a throwaway call; the
// internal call rejects more than one.
func internalOptionList(options []exec.Options) []implSession.Options {
	if len(options) == 0 {
		return nil
	}
	out := make([]implSession.Options, len(options))
	for i, o := range options {
		out[i] = internalOptions(o)
	}
	return out
}

func internalCompiledOptions(options []exec.CompiledOptions) (implSession.CompiledOptions, error) {
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

func internalStreamStatement[P, R any](s stream.Statement[P, R]) implSession.StreamStatement[P, R] {
	return implSession.StreamStatement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, Kind: s.Kind}
}
