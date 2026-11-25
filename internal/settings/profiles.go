// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/retry"
)

type Profile struct {
	Retry                retry.Policy
	Routing              routing.RoutingPolicy
	Speculation          retry.SpeculationPolicy
	Timeout, PageTimeout time.Duration
	Prefetch             int
	PageSize             int32
	SerialConsistency    cql.Option[cql.Consistency]
	Consistency          cql.Consistency
	// Idempotent marks every statement the profile runs idempotent.
	Idempotent bool
	// unbuilt marks a public Profile that ProfileBuilder.Build did not make,
	// such as a struct literal: its unset fields are zero, not the driver's
	// defaults, so validation rejects it.
	unbuilt bool
}

// InternalMarkUnbuilt records that p comes from a public Profile that
// ProfileBuilder.Build did not make.
func (p *Profile) InternalMarkUnbuilt() { p.unbuilt = true }

// InternalBuilt reports whether p was not marked by InternalMarkUnbuilt.
func (p *Profile) InternalBuilt() bool { return !p.unbuilt }

func InternalDefaultProfile() Profile {
	return Profile{Consistency: cql.LocalQuorum, SerialConsistency: cql.Option[cql.Consistency]{Value: cql.LocalSerial, Set: true}, Timeout: 30 * time.Second, PageSize: 5000, Prefetch: 1, Retry: retry.Safe{MaxAttempts: 3}}
}

// ValidateProfile checks the profile. A cql.ValidationError names, in its
// Field, the config.ProfileBuilder setter that chose the invalid value.
func (p Profile) ValidateProfile() error {
	if p.unbuilt {
		return cqltype.TypeValidation("Profile", "not built: a Profile literal leaves unset fields zero instead of the driver's defaults; build it with config.NewProfile")
	}
	if !InternalValidConsistency(p.Consistency) {
		return cqltype.TypeValidation("WithConsistency", "unknown consistency %v", p.Consistency)
	}
	if p.SerialConsistency.Set && p.SerialConsistency.Value != cql.Serial && p.SerialConsistency.Value != cql.LocalSerial {
		return cqltype.TypeValidation("WithSerialConsistency", "must be SERIAL or LOCAL_SERIAL, got %v", p.SerialConsistency.Value)
	}
	if p.Timeout < 0 {
		return cqltype.TypeValidation("WithTimeout", "must not be negative, got %v", p.Timeout)
	}
	if p.PageTimeout < 0 {
		return cqltype.TypeValidation("WithPageTimeout", "must not be negative, got %v", p.PageTimeout)
	}
	if p.PageSize <= 0 {
		return cqltype.TypeValidation("WithPageSize", "must be positive, got %d", p.PageSize)
	}
	if p.Prefetch < 0 || p.Prefetch > 1 {
		return cqltype.TypeValidation("WithPrefetch", "must be zero or one, got %d", p.Prefetch)
	}
	return nil
}
func InternalValidConsistency(c cql.Consistency) bool {
	return c >= cql.Any && c <= cql.LocalOne
}

// SpeculationPolicy schedules additional attempts of eligible executions.
// Delay receives the number of attempts started so far and returns how long
// to wait before starting another, and whether to start one at all.
