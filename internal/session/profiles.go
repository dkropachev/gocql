// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
	// IncludeMetadata requests full result metadata even when a validated cache exists.
)

// Options lists execution overrides. Query constructors resolve them once;
// Session.CompileOptions resolves recurring overrides for one-shot calls.
type Options struct {
	Retry                retry.Policy
	Speculation          retry.SpeculationPolicy
	Observer             observe.Hooks
	Profile              *settings.Profile
	privateStorage       OptionsStorage
	Keyspace             string
	CustomPayload        []cql.PayloadEntry
	Timeout, PageTimeout cql.Option[time.Duration]
	Prefetch             cql.Option[int]
	Timestamp            cql.Option[int64]
	PageSize             int
	NowInSeconds         cql.Option[int32]
	ShardID              cql.Option[uint32]
	Consistency          cql.Option[cql.Consistency]
	HostID               cql.Option[cql.UUID]
	// SerialConsistency, when set, replaces the profile's serial
	// consistency; it must be SERIAL or LOCAL_SERIAL.
	SerialConsistency cql.Option[cql.Consistency]
	IncludeMetadata   bool
	Idempotent        cql.Option[bool]
	CQLTrace          bool
	// AnyShardFallback lets a request pinned to ShardID use another shard
	// of HostID.
	AnyShardFallback bool
	// NoSessionObserver leaves the session's observer out of the
	// execution, so Observer alone, or nothing, observes it.
	NoSessionObserver bool
}

type InternalResolvedOptions struct {
	// templates holds the options' parameters blocks, encoded when they were
	// resolved; nil for options resolved for one execution.
	templates *optionTemplates
	// observerHooks are the hooks an execution with these options calls on
	// the session they were resolved for, as executionHooks finds them; nil
	// when the options leave the session's hooks, or were resolved for
	// another session.
	observerHooks   *observe.Hooks
	InternalProfile settings.Profile
	InternalOptions Options
}

// resolveOptionValues merges options over base, the session's default
// profile, or the default profile when base is nil.
func resolveOptionValues(base *settings.Profile, options Options) (InternalResolvedOptions, error) {
	p := settings.InternalDefaultProfile()
	if base != nil {
		p = *base
	}
	if options.Profile != nil {
		if err := (*options.Profile).ValidateProfile(); err != nil {
			return InternalResolvedOptions{}, err
		}
		p = *options.Profile
	}
	if options.Consistency.Set {
		p.Consistency = options.Consistency.Value
	}
	if options.SerialConsistency.Set {
		if !options.SerialConsistency.Value.IsSerial() {
			return InternalResolvedOptions{}, cqltype.TypeValidation("SerialConsistency", "must be SERIAL or LOCAL_SERIAL, got %v", options.SerialConsistency.Value)
		}
		p.SerialConsistency = options.SerialConsistency
	}
	// A set timeout replaces the profile's, and a set zero removes it.
	if options.Timeout.Set {
		p.Timeout = options.Timeout.Value
	}
	if options.PageTimeout.Set {
		p.PageTimeout = options.PageTimeout.Value
	}
	if options.PageSize < 0 || options.PageSize > cql.Unpaged {
		return InternalResolvedOptions{}, cqltype.TypeValidation("page size", "must be positive and at most cql.Unpaged")
	}
	if options.PageSize != 0 {
		p.PageSize = int32(options.PageSize)
	}
	if options.Prefetch.Set {
		p.Prefetch = options.Prefetch.Value
	}
	if options.Retry != nil {
		p.Retry = options.Retry
	}
	switch options.Speculation.(type) {
	case nil:
	case retry.NoSpeculation, *retry.NoSpeculation:
		// Disabled speculation allocates no scheduler or timer.
		p.Speculation = nil
	default:
		p.Speculation = options.Speculation
	}
	if options.Consistency.Set && !settings.InternalValidConsistency(p.Consistency) {
		return InternalResolvedOptions{}, cqltype.TypeValidation("consistency", "invalid consistency")
	}
	if options.Timeout.Set && options.Timeout.Value < 0 || options.PageTimeout.Set && options.PageTimeout.Value < 0 {
		return InternalResolvedOptions{}, cqltype.TypeValidation("timeout", "must not be negative")
	}
	if p.PageSize == cql.Unpaged {
		if options.Prefetch.Set && options.Prefetch.Value != 0 {
			return InternalResolvedOptions{}, &settings.UnsupportedSettingError{Setting: "Prefetch", Reason: "an unpaged result has no next page"}
		}
		// The resolved profile carries no page size, so requests omit it.
		p.PageSize = 0
	}
	if options.Prefetch.Set && (p.Prefetch < 0 || p.Prefetch > 1) {
		return InternalResolvedOptions{}, cqltype.TypeValidation("prefetch", "must be zero or one")
	}
	if options.HostID.Set && options.HostID.Value.IsZero() {
		return InternalResolvedOptions{}, cqltype.TypeValidation("host ID", "empty UUID")
	}
	if options.ShardID.Set && !options.HostID.Set {
		return InternalResolvedOptions{}, cqltype.TypeValidation("shard", "requires a host ID")
	}
	if options.AnyShardFallback && !options.ShardID.Set {
		return InternalResolvedOptions{}, cqltype.TypeValidation("AnyShardFallback", "requires a shard ID")
	}
	keys := make(map[string]bool, len(options.CustomPayload))
	for _, entry := range options.CustomPayload {
		if len(entry.Key) > 65535 || keys[entry.Key] {
			return InternalResolvedOptions{}, cqltype.TypeValidation("custom payload", "invalid or duplicate key")
		}
		keys[entry.Key] = true
		if entry.Value.Valid {
			if err := cqltype.CheckBytesAlive(entry.Value.Value); err != nil {
				return InternalResolvedOptions{}, err
			}
		}
	}
	// The payload is not copied: NewCompiledOptions copied it already, and
	// a resolution for one execution reads the compiled options' copy.
	return InternalResolvedOptions{InternalProfile: p, InternalOptions: options}, nil
}

type OptionsStorage struct {
	InternalConnection *transport.Conn
	// InternalUntracked leaves the execution out of the session's metrics,
	// as package cqltrace's reads are.
	InternalUntracked bool
}
