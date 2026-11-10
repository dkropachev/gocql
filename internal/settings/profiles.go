// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/retry"
)

type Profile struct {
	Retry                retry.Policy
	Routing              routing.RoutingPolicy
	Speculation          SpeculationPolicy
	Timeout, PageTimeout time.Duration
	Prefetch             int
	PageSize             int32
	SerialConsistency    cqltype.Option[cqltype.Consistency]
	Consistency          cqltype.Consistency
}

func InternalDefaultProfile() Profile {
	return Profile{Consistency: cqltype.LocalQuorum, SerialConsistency: cqltype.Option[cqltype.Consistency]{Value: cqltype.LocalSerial, Set: true}, Timeout: 30 * time.Second, PageSize: 5000, Prefetch: 1, Retry: retry.Safe{MaxAttempts: 3}}
}
func InternalValidateProfile(p Profile) error {
	if !InternalValidConsistency(p.Consistency) {
		return cqltype.TypeValidation("consistency", "invalid consistency")
	}
	if p.SerialConsistency.Set && p.SerialConsistency.Value != cqltype.Serial && p.SerialConsistency.Value != cqltype.LocalSerial {
		return cqltype.TypeValidation("serial consistency", "must be SERIAL or LOCAL_SERIAL")
	}
	if p.Timeout < 0 || p.PageTimeout < 0 {
		return cqltype.TypeValidation("timeout", "must not be negative")
	}
	if p.PageSize <= 0 {
		return cqltype.TypeValidation("page size", "must be positive")
	}
	if p.Prefetch < 0 || p.Prefetch > 1 {
		return cqltype.TypeValidation("prefetch", "must be zero or one")
	}
	return nil
}
func InternalValidConsistency(c cqltype.Consistency) bool {
	return c >= cqltype.Any && c <= cqltype.LocalOne
}

type SpeculationPolicy interface {
	Delay(attempt int, metrics routing.MetricsSnapshot) (time.Duration, bool)
}

type Observer interface {
	ObserveRequest(RequestEvent)
	ObserveAttempt(routing.AttemptEvent)
}
type RequestEvent struct {
	Err             error
	Statement       string
	BatchStatements []string
	Parameters      cqltype.Columns
	Result          model.Result
	Routing         model.RoutingInfo
	ID              uint64
}
