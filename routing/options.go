// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"errors"
	"math"
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

// Options configures NewPolicy.
//
// The zero Options leaves every option off.
type Options struct {
	// Datacenter and Rack name the local datacenter and rack. Replicas there
	// are tried first. A rack requires a datacenter.
	Datacenter, Rack string
	// Latency configures latency-aware routing.
	Latency LatencyOptions
	// TokenAware routes a request to the replicas of its partition.
	TokenAware bool
	// AllowRemote lets requests fall back to hosts outside Datacenter. With
	// TokenAware, replicas there are tried before local hosts that are not
	// replicas.
	AllowRemote bool
	// ShuffleReplicas spreads requests over replicas at random rather than in
	// a fixed order per token.
	ShuffleReplicas bool
	// LeaderAware routes requests of strongly consistent tables to the
	// tablet's leader first. Only ScyllaDB has tablets; on Cassandra it has
	// no effect.
	LeaderAware bool
}

// LatencyOptions configures latency-aware routing: hosts much slower than
// the fastest one are tried last for a while. Zero fields take defaults.
type LatencyOptions struct {
	// Enabled turns latency-aware routing on.
	Enabled bool
	// ExclusionThreshold is how many times slower than the fastest host a
	// host may be before it is tried last. Default 2.
	ExclusionThreshold float64
	// MinimumSamples is how many measurements a host needs before it can be
	// penalized. Default 50.
	MinimumSamples int
	// Scale weighs older measurements against new ones. Default 100ms.
	Scale time.Duration
	// RetryPeriod is how long a penalized host is tried last before it gets
	// another chance. Default 10s.
	RetryPeriod time.Duration
	// UpdateInterval is how often the fastest latency is recomputed.
	// Default 1s.
	UpdateInterval time.Duration
}

// normalized returns o with latency defaults applied, or a validation error.
func (o Options) normalized() (Options, error) {
	if o.Rack != "" && o.Datacenter == "" {
		return o, &cql.ValidationError{Field: "routing rack", Err: errors.New("requires a datacenter")}
	}
	if !o.Latency.Enabled {
		return o, nil
	}
	l := &o.Latency
	if l.ExclusionThreshold == 0 {
		l.ExclusionThreshold = 2
	}
	if l.MinimumSamples == 0 {
		l.MinimumSamples = 50
	}
	if l.Scale == 0 {
		l.Scale = 100 * time.Millisecond
	}
	if l.RetryPeriod == 0 {
		l.RetryPeriod = 10 * time.Second
	}
	if l.UpdateInterval == 0 {
		l.UpdateInterval = time.Second
	}
	if l.ExclusionThreshold < 1 || math.IsNaN(l.ExclusionThreshold) || math.IsInf(l.ExclusionThreshold, 0) ||
		l.MinimumSamples < 1 || l.Scale <= 0 || l.RetryPeriod <= 0 || l.UpdateInterval <= 0 {
		return o, &cql.ValidationError{Field: "latency routing", Err: errors.New("invalid configuration")}
	}
	return o, nil
}
