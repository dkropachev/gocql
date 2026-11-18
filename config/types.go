// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
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
	Retry                retry.Policy
	Routing              routing.Policy
	Speculation          retry.SpeculationPolicy
	Timeout, PageTimeout time.Duration
	Prefetch             int
	// PageSize is the rows per page; cql.Unpaged requests the whole result
	// in one response.
	PageSize          int32
	SerialConsistency cql.Option[cql.Consistency]
	Consistency       cql.Consistency
	// built reports that ProfileBuilder.Build made the profile.
	built bool
}

func internalProfile(p Profile) implSettings.Profile {
	v := implSettings.Profile{Retry: p.Retry, Routing: implRouting.EngineOf(p.Routing), Speculation: p.Speculation,
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch,
		PageSize: p.PageSize, SerialConsistency: p.SerialConsistency, Consistency: p.Consistency}
	if !p.built {
		v.InternalMarkUnbuilt()
	}
	return v
}

func publicProfile(p implSettings.Profile) Profile {
	return Profile{Retry: p.Retry, Routing: implRouting.PublicPolicy(p.Routing), Speculation: p.Speculation,
		Timeout: p.Timeout, PageTimeout: p.PageTimeout, Prefetch: p.Prefetch,
		PageSize: p.PageSize, SerialConsistency: p.SerialConsistency, Consistency: p.Consistency,
		built: p.InternalBuilt()}
}

func init() {
	link.Set(link.ConfigResolve, Config.resolved)
	link.Set(link.ProfileToEngine, internalProfile)
	link.Set(link.ProfileFromEngine, publicProfile)
}
