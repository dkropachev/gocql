// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	implBinding "github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/link"
	implSession "github.com/scylladb/gocql/v2/internal/session"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

func init() { link.Set(link.CompileUntracked, compileUntracked) }

// compileUntracked is Session.CompileOptions for executions that the
// session's metrics leave out, such as package cqltrace's reads.
func compileUntracked(s *Session, options exec.Options) (exec.CompiledOptions, error) {
	o := internalOptions(options)
	implSession.OptionsData(&o).InternalUntracked = true
	compiled, err := s.internal().CompileOptions(o)
	return exec.CompiledOptions(compiled), err
}

func internalOptions(o exec.Options) implSession.Options {
	v := implSession.Options{
		Retry: o.Retry, Speculation: o.Speculation, Observer: o.Observer, Keyspace: o.Keyspace,
		CustomPayload: o.CustomPayload, Timeout: o.Timeout, PageTimeout: o.PageTimeout,
		Prefetch: prefetchPages(o.Prefetch), PageSize: o.PageSize,
		NowInSeconds: o.NowInSeconds, ShardID: o.ShardID,
		SerialConsistency: implSession.SerialOverride(o.SerialConsistency), Consistency: o.Consistency,
		HostID: o.HostID, IncludeMetadata: o.IncludeMetadata,
		Idempotent: o.Idempotent, CQLTrace: o.CQLTrace,
		NoSessionObserver: o.NoSessionObserver, AnyShardFallback: o.AnyShardFallback,
	}
	if o.Timestamp.Set {
		v.Timestamp = cql.Some(o.Timestamp.Value.UnixMicro())
	}
	if o.Profile != nil {
		// The only copy of the caller's profile: compiling keeps it.
		profile := implSettings.InternalProfileFromPublic(*o.Profile)
		v.Profile = &profile
	}
	return v
}

// pageSize32 checks a RunPage page size, which the protocol carries as an
// int32: a negative one, or one above cql.Unpaged, is a cql.ValidationError.
func pageSize32(pageSize int) (int32, error) {
	if pageSize < 0 || pageSize > cql.Unpaged {
		return 0, pageSizeRange()
	}
	return int32(pageSize), nil
}

// pageSizeRange is the error of a page size out of range.
//
//go:noinline
func pageSizeRange() error {
	return &cql.ValidationError{Field: "page size", Err: errors.New("must not be negative or above cql.Unpaged")}
}

// tooManyPrepareOptions is the error of a call given more than one
// PrepareOptions.
//
//go:noinline
func tooManyPrepareOptions() error {
	return &cql.ValidationError{Field: "options", Err: errors.New("at most one PrepareOptions")}
}

// tooManyStreamWorkerOptions is the error of a PreparedStream.Worker call
// given more than one StreamOptions.
//
//go:noinline
func tooManyStreamWorkerOptions() error {
	return &cql.ValidationError{Field: "options", Err: errors.New("at most one StreamOptions")}
}

// fixedWorkerTimestamp is the error of a worker given a fixed
// exec.Options.Timestamp, which would stamp each of its writes alike.
//
//go:noinline
func fixedWorkerTimestamp() error {
	return &cql.ValidationError{Field: "Timestamp", Err: errors.New("a worker would give every write the same timestamp; pass it per call through Session.CompileOptions")}
}

// internalOptionList converts the optional exec.CompiledOptions of a
// throwaway call to the values it resolves for the session; the internal
// call rejects more than one.
func internalOptionList(options []exec.CompiledOptions) []implSession.Options {
	if len(options) == 0 {
		return nil
	}
	out := make([]implSession.Options, len(options))
	for i, o := range options {
		out[i] = implSession.CompiledOptions(o).Values()
	}
	return out
}

// tooManyOptions is the error of a call given more than one CompiledOptions.
// It and the helpers below stay out of line to keep the error path out of the
// facade methods.
//
//go:noinline
func tooManyOptions() error {
	return &cql.ValidationError{Field: "options", Err: errors.New("at most one CompiledOptions")}
}

// tooManyOptionsResult returns tooManyOptions with its execution result.
//
//go:noinline
func tooManyOptionsResult() (cql.Result, error) {
	err := tooManyOptions()
	return implSession.InternalExecutionErrorResult(err), err
}

// tooManyOptionsFound returns tooManyOptionsResult for a call that reports
// whether a row was found.
//
//go:noinline
func tooManyOptionsFound() (bool, cql.Result, error) {
	r, err := tooManyOptionsResult()
	return false, r, err
}

func internalStatement[P, R any](s cql.Statement[P, R]) implBinding.Statement[P, R] {
	return implBinding.Statement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, BindMarkers: s.BindMarkers, Kind: s.Kind, Idempotent: s.Idempotent}
}

func internalStreamStatement[P, R any](s cql.StreamStatement[P, R]) implSession.StreamStatement[P, R] {
	return implSession.StreamStatement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, Kind: s.Kind, Idempotent: s.Idempotent}
}

// prefetchPages is the internal prefetch depth of a public override.
func prefetchPages(on cql.Option[bool]) cql.Option[int] {
	if !on.Set {
		return cql.Option[int]{}
	}
	if on.Value {
		return cql.Some(1)
	}
	return cql.Some(0)
}
