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
// The zero Options is the session's default policy: token-aware, with
// replicas shuffled per request and leader-aware for strongly consistent
// tables, in the session's local datacenter (config.ConfigBuilder's
// WithLocalDatacenter) or, without one, over every datacenter.
type Options struct {
	// Datacenter and Rack name the local datacenter and rack. Replicas there
	// are tried first. An empty Datacenter takes the session's local
	// datacenter, and with it, when Rack is empty too, the session's local
	// rack; a Rack without a Datacenter is a rack of the session's local
	// datacenter, and Validate fails when the session has none.
	Datacenter, Rack string
	// Latency configures latency-aware routing.
	Latency LatencyOptions
	// MaxRemoteHostsPerDC, when positive, is the most hosts of each other
	// datacenter a plan tries, replicas included, when AllowRemote is set.
	// Zero tries every one; a negative value is rejected.
	MaxRemoteHostsPerDC int
	// NoTokenAware routes a request without regard to the replicas of its
	// partition. It also turns leader-aware routing off.
	NoTokenAware bool
	// AllowRemote lets requests fall back to hosts outside the local
	// datacenter. Unless NoTokenAware is set, replicas there are tried before
	// local hosts that are not replicas. A request at LOCAL_ONE,
	// LOCAL_QUORUM or LOCAL_SERIAL consistency stays in the local datacenter
	// unless RemoteForLocalConsistency is set: a remote coordinator would
	// count the replicas of its own datacenter, not of the local one.
	AllowRemote bool
	// RemoteForLocalConsistency lets requests at a LOCAL_* consistency fall
	// back to other datacenters too, when AllowRemote is set.
	RemoteForLocalConsistency bool
	// FixedReplicaOrder tries a token's replicas in a fixed order per token
	// rather than shuffling them per request.
	FixedReplicaOrder bool
	// NoLeaderAware turns off leader-aware routing, which routes requests of
	// strongly consistent tables (a ScyllaDB keyspace created with
	// consistency = 'global') to the tablet's leader first, ahead of nearer
	// replicas. Leader-aware routing applies only once a response has
	// reported the tablet over the TABLETS_ROUTING_V2_EXPERIMENTAL extension;
	// requests at ONE or LOCAL_ONE and eventually consistent keyspaces keep
	// normal replica order. A leader the policy would not otherwise use (host
	// filter, another datacenter without AllowRemote, down or unconnected) is
	// not preferred, and latency-aware routing tries a slow leader last. Only
	// ScyllaDB has tablets; on Cassandra it has no effect.
	NoLeaderAware bool
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
	if o.MaxRemoteHostsPerDC < 0 {
		return o, &cql.ValidationError{Field: "routing remote hosts", Err: errors.New("must not be negative")}
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
