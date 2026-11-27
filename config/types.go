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
	// Timeout bounds one execution, retries included, and PageTimeout the
	// fetch of one page. Zero means no limit.
	Timeout, PageTimeout time.Duration
	// Prefetch is how many continuation pages Rows prefetch: zero or one.
	Prefetch int
	// PageSize is the rows per page; cql.Unpaged requests the whole result
	// in one response.
	PageSize int32
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
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch,
		PageSize: p.PageSize, SerialConsistency: p.SerialConsistency, Consistency: p.Consistency, Idempotent: p.Idempotent}
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
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch,
		PageSize: p.PageSize, SerialConsistency: p.SerialConsistency, Consistency: p.Consistency,
		Idempotent: p.Idempotent, built: p.InternalBuilt()}
}

func init() {
	link.Set(link.ConfigResolve, Config.resolved)
	link.Set(link.ProfileToEngine, internalProfile)
	link.Set(link.ProfileFromEngine, publicProfile)
}
