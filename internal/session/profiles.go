// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
	// IncludeMetadata requests full result metadata even when a validated cache exists.
)

// Options lists execution overrides. Query constructors resolve them once;
// Session.CompileOptions resolves recurring overrides for one-shot calls.
type Options struct {
	Retry                retry.Policy
	Observer             settings.Observer
	Profile              *settings.Profile
	privateStorage       OptionsStorage
	Keyspace             string
	CustomPayload        []model.PayloadEntry
	Timeout, PageTimeout cqltype.Option[time.Duration]
	Prefetch             cqltype.Option[int]
	Timestamp            cqltype.Option[int64]
	PageSize             cqltype.Option[int32]
	NowInSeconds         cqltype.Option[int32]
	ShardID              cqltype.Option[uint32]
	Consistency          cqltype.Option[cqltype.Consistency]
	HostID               cqltype.Option[cqltype.UUID]
	// SerialConsistency overrides the profile's serial consistency.
	SerialConsistency SerialOverride
	IncludeMetadata   bool
	Idempotent        bool
	Trace             bool
}

// SerialOverride chooses the serial consistency of an execution relative to
// its profile.
type SerialOverride uint8

const (
	// SerialInherit keeps the profile's serial consistency.
	SerialInherit SerialOverride = iota
	// SerialSerial sends SERIAL.
	SerialSerial
	// SerialLocal sends LOCAL_SERIAL.
	SerialLocal
	// SerialOmit sends no serial consistency.
	SerialOmit
)

type InternalResolvedOptions struct {
	// templates holds the options' parameters blocks, encoded when they were
	// resolved; nil for options resolved for one execution.
	templates       *optionTemplates
	InternalProfile settings.Profile
	InternalOptions Options
}

func InternalResolveOptions(s *implCluster.Core, options Options) (InternalResolvedOptions, error) {
	var base *settings.Profile
	if s != nil {
		base = InternalRuntimeFor(s).InternalProfile
	}
	return resolveOptionValues(base, options)
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
	switch options.SerialConsistency {
	case SerialInherit:
	case SerialSerial:
		p.SerialConsistency = cqltype.Some(cqltype.Serial)
	case SerialLocal:
		p.SerialConsistency = cqltype.Some(cqltype.LocalSerial)
	case SerialOmit:
		p.SerialConsistency = cqltype.Option[cqltype.Consistency]{}
	default:
		return InternalResolvedOptions{}, cqltype.TypeValidation("serial consistency", "unknown override %d", options.SerialConsistency)
	}
	if options.Timeout.Set {
		p.Timeout = options.Timeout.Value
	}
	if options.PageTimeout.Set {
		p.PageTimeout = options.PageTimeout.Value
	}
	if options.PageSize.Set {
		p.PageSize = options.PageSize.Value
	}
	if options.Prefetch.Set {
		p.Prefetch = options.Prefetch.Value
	}
	if options.Retry != nil {
		p.Retry = options.Retry
	}
	if options.Consistency.Set && !settings.InternalValidConsistency(p.Consistency) {
		return InternalResolvedOptions{}, cqltype.TypeValidation("consistency", "invalid consistency")
	}
	if options.Timeout.Set && p.Timeout < 0 || options.PageTimeout.Set && p.PageTimeout < 0 {
		return InternalResolvedOptions{}, cqltype.TypeValidation("timeout", "must not be negative")
	}
	if options.PageSize.Set && p.PageSize <= 0 {
		return InternalResolvedOptions{}, cqltype.TypeValidation("page size", "must be positive")
	}
	if p.PageSize == cqltype.Unpaged {
		if options.Prefetch.Set && options.Prefetch.Value != 0 {
			return InternalResolvedOptions{}, &settings.UnsupportedSettingError{Setting: "prefetch", Reason: "an unpaged result has no next page"}
		}
		// The resolved profile carries no page size, so requests omit it.
		p.PageSize = 0
	}
	if options.Prefetch.Set && (p.Prefetch < 0 || p.Prefetch > 1) {
		return InternalResolvedOptions{}, cqltype.TypeValidation("prefetch", "must be zero or one")
	}
	if options.HostID.Set && options.HostID.Value.IsEmpty() {
		return InternalResolvedOptions{}, cqltype.TypeValidation("host ID", "empty UUID")
	}
	if options.ShardID.Set && !options.HostID.Set {
		return InternalResolvedOptions{}, cqltype.TypeValidation("shard", "requires a host ID")
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
