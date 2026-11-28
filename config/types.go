// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/link"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

// Profile supplies reusable execution defaults. Build one with NewProfile and
// ProfileBuilder.Build. A Profile written as a literal, such as
// config.Profile{PageSize: 1000}, would leave every other field zero instead of
// the driver's default (consistency ANY, no retries, no timeout, no prefetch,
// no serial consistency), so ConfigBuilder.WithDefaultProfile, gocql's query
// constructors and gocql.Session.CompileOptions reject it with a validation error.
// A built Profile may be copied and its fields changed: the copy stays built.
// Config and query construction snapshot its fields; policy objects stay shared.
//
// The zero Profile is such a literal, and is rejected the same way.
type Profile struct {
	// Retry is the retry policy; nil is the default, retry.Safe{MaxAttempts:
	// 3}, as for ProfileBuilder.WithRetry. retry.Never{} disables retries.
	Retry retry.Policy
	// Routing is the routing policy; nil is the default policy, as for
	// ProfileBuilder.WithRouting.
	Routing routing.Policy
	// Speculation is the speculative execution policy; nil disables it.
	Speculation retry.SpeculationPolicy
	// CQLTraceSampler, when not nil, is asked once per request, as it is
	// admitted, whether to record a CQL trace session of its attempts, as
	// exec.Options.CQLTrace does for every request it is set on; see
	// ProfileBuilder.WithCQLTraceSampler.
	CQLTraceSampler func(routing.Info) bool
	// Timeout bounds one execution, retries included, and PageTimeout the
	// fetch of one page. Zero means no limit.
	Timeout, PageTimeout time.Duration
	// PageSize is the rows per page, 1 to cql.Unpaged; cql.Unpaged requests
	// the whole result in one response.
	PageSize int
	// SerialConsistency is the serial consistency of conditional statements,
	// SERIAL or LOCAL_SERIAL; unset sends none.
	SerialConsistency cql.Option[cql.Consistency]
	// Consistency is the consistency level.
	Consistency cql.Consistency
	// Idempotent marks every ordinary statement the profile runs safe to
	// run more than once, as cql.Statement.Idempotent marks one; false
	// leaves each statement's own. It does not mark a conditional
	// statement, a SERIAL or LOCAL_SERIAL read, or a counter write, which
	// replaying could apply twice: only the statement's own Idempotent or
	// exec.Options.Idempotent marks those. exec.Options.Idempotent
	// overrides it for one execution.
	Idempotent bool
	// Prefetch makes Rows request the next page as soon as a page arrives,
	// as ProfileBuilder.WithPrefetch describes.
	Prefetch bool
	// built reports that ProfileBuilder.Build made the profile.
	built bool
}

// Builder returns a ProfileBuilder that starts from p's values, so a profile
// can be derived from another, such as Config.DefaultProfile:
//
//	reads := cfg.DefaultProfile().Builder().WithConsistency(cql.One).MustBuild()
//
// Changes to the builder do not reach p. Build rejects a builder derived
// from a Profile that ProfileBuilder.Build did not make, such as a literal
// or the zero Profile, as WithDefaultProfile rejects that Profile.
func (p Profile) Builder() *ProfileBuilder {
	return &ProfileBuilder{profile: internalProfile(p), started: true}
}

// Limits caps what a session accepts from servers and keeps for paging, as
// ConfigBuilder.WithLimits sets. A zero field keeps the driver's default for
// that limit, so the zero Limits is the defaults.
type Limits struct {
	// MaxFrameBytes caps a received frame's size, at most 2 GiB - 1.
	// Default 256 MiB.
	MaxFrameBytes int64
	// MaxDecompressedBytes caps a frame's decompressed size, at most
	// 2 GiB - 1. Default 256 MiB.
	MaxDecompressedBytes int64
	// MaxPagingStateBytes caps the paging states one execution keeps to
	// detect a server that repeats a state, and the token a RunPage call may
	// resume from. Default 16 MiB.
	MaxPagingStateBytes int64
	// MaxMetadataDepth caps the nesting depth of result type metadata, at
	// most 128. Default 64.
	MaxMetadataDepth int
	// MaxPagingStates caps the paging states a session keeps. Default 65536.
	MaxPagingStates int
}

// SchemaChangeWait is what a statement that changes the schema waits for
// before it returns, as ConfigBuilder.WithSchemaChangeWait chooses. The zero
// SchemaChangeWait is SchemaChangeWaitNone; the default is
// SchemaChangeWaitRefresh. Later releases may add values, so a switch over
// them needs a default case.
type SchemaChangeWait uint8

const (
	// SchemaChangeWaitNone returns as soon as the coordinator answers. Call
	// gocql.Session.AwaitSchemaAgreement where agreement matters; schema
	// events still reload metadata in the background.
	SchemaChangeWaitNone SchemaChangeWait = iota
	// SchemaChangeWaitAgreement waits for the nodes to agree on the schema,
	// so a statement that uses the change on any node finds it.
	SchemaChangeWaitAgreement
	// SchemaChangeWaitRefresh waits for agreement and then reloads the
	// changed keyspace's metadata, so Session.Metadata and routing see the
	// change when the statement returns.
	SchemaChangeWaitRefresh
)

// String returns "none", "agreement" or "refresh", or SchemaChangeWait(n)
// for a value it does not define.
func (w SchemaChangeWait) String() string {
	switch w {
	case SchemaChangeWaitNone:
		return "none"
	case SchemaChangeWaitAgreement:
		return "agreement"
	case SchemaChangeWaitRefresh:
		return "refresh"
	default:
		return fmt.Sprintf("SchemaChangeWait(%d)", uint8(w))
	}
}

// defaultRouting is the policy of a Profile whose Routing was set to nil
// after Build, shared so that converting such a profile allocates none.
var defaultRouting = sync.OnceValue(implSettings.DefaultRouting)

func internalProfile(p Profile) implSettings.Profile {
	v := implSettings.Profile{Retry: p.Retry, Routing: implRouting.EngineOf(p.Routing), Speculation: p.Speculation,
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: prefetchPages(p.Prefetch),
		SerialConsistency: p.SerialConsistency, Consistency: p.Consistency, Idempotent: p.Idempotent,
		CQLTraceSampler: p.CQLTraceSampler}
	v.InternalSetPageSize(p.PageSize)
	if v.Retry == nil {
		v.Retry = defaultRetry
	}
	if v.Routing == nil {
		v.Routing = defaultRouting()
	}
	if !p.built {
		v.InternalMarkUnbuilt()
	}
	return v
}

// defaultRetry is the retry policy of a Profile whose Retry is nil.
var defaultRetry = implSettings.InternalDefaultProfile().Retry

func publicProfile(p implSettings.Profile) Profile {
	return Profile{Retry: p.Retry, Routing: implRouting.PublicPolicy(p.Routing), Speculation: p.Speculation,
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch != 0,
		PageSize: int(p.PageSize), SerialConsistency: p.SerialConsistency, Consistency: p.Consistency,
		Idempotent: p.Idempotent, CQLTraceSampler: p.CQLTraceSampler, built: p.InternalBuilt()}
}

// prefetchPages is the internal prefetch depth of the public flag.
func prefetchPages(on bool) int {
	if on {
		return 1
	}
	return 0
}

func init() {
	link.Set(link.ConfigResolve, Config.resolved)
	link.Set(link.ProfileToEngine, internalProfile)
	link.Set(link.ProfileFromEngine, publicProfile)
}
