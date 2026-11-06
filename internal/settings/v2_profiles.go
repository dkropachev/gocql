// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"

	"github.com/scylladb/gocql/v2/retry"
)

type Profile struct {
	Retry                retry.Policy
	Routing              RoutingPolicy
	Speculation          SpeculationPolicy
	Timeout, PageTimeout time.Duration
	Prefetch             int
	PageSize             int32
	SerialConsistency    Option[Consistency]
	Consistency          Consistency
}

func InternalDefaultProfile() Profile {
	return Profile{Consistency: LocalQuorum, SerialConsistency: Option[Consistency]{Value: LocalSerial, Set: true}, Timeout: 30 * time.Second, PageSize: 5000, Prefetch: 1, Retry: retry.Safe{MaxAttempts: 3}}
}
func InternalValidateProfile(p Profile) error {
	if !InternalValidConsistency(p.Consistency) {
		return typeValidation("consistency", "invalid consistency")
	}
	if p.SerialConsistency.Set && p.SerialConsistency.Value != Serial && p.SerialConsistency.Value != LocalSerial {
		return typeValidation("serial consistency", "must be SERIAL or LOCAL_SERIAL")
	}
	if p.Timeout < 0 || p.PageTimeout < 0 {
		return typeValidation("timeout", "must not be negative")
	}
	if p.PageSize <= 0 {
		return typeValidation("page size", "must be positive")
	}
	if p.Prefetch < 0 || p.Prefetch > 1 {
		return typeValidation("prefetch", "must be zero or one")
	}
	return nil
}
func InternalValidConsistency(c Consistency) bool { return c >= Any && c <= LocalOne }

type SpeculationPolicy interface {
	Delay(attempt int, metrics MetricsSnapshot) (time.Duration, bool)
}

type Observer interface {
	ObserveRequest(RequestEvent)
	ObserveAttempt(AttemptEvent)
}
type RequestEvent struct {
	Err             error
	Statement       string
	BatchStatements []string
	Parameters      Columns
	Result          Result
	Routing         RoutingInfo
	ID              uint64
}
