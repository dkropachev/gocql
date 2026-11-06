// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package routing chooses the hosts and shards a request is sent to.
package routing

import (
	implModel "github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
)

type (
	Policy           = implRouting.RoutingPolicy
	Options          = implRouting.RoutingOptions
	LatencyOptions   = implRouting.LatencyOptions
	Info             = implModel.RoutingInfo
	Plan             = implRouting.QueryPlan
	Route            = implRouting.Route
	Target           = implModel.Target
	PartitionKey     = implRouting.PartitionKey
	TokenPartitioner = implRouting.TokenPartitioner
	TokenRange       = implRouting.TokenRange
	HostSnapshot     = implModel.HostSnapshot
	MetricsSnapshot  = implRouting.MetricsSnapshot
)

const AnyShard = implRouting.AnyShard

var (
	Murmur3TokenPartitioner = implRouting.Murmur3TokenPartitioner
	CDCTokenPartitioner     = implRouting.CDCTokenPartitioner
)

// NewPolicy builds the default token- and tablet-aware policy.
func NewPolicy(options Options) (Policy, error) { return implRouting.NewRoutingPolicy(options) }

// NewPlan returns the hosts policy would try for a request described by info.
func NewPlan(policy Policy, info Info, m *metadata.Snapshot) Plan {
	return implRouting.NewPlan(policy, info, m)
}

// SingleTarget routes every request to target.
func SingleTarget(target Target, strictShard bool) Policy {
	return implRouting.SingleTargetRouting(target, strictShard)
}
