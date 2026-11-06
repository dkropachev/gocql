// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	model "github.com/scylladb/gocql/v2/internal/model"

	routing "github.com/scylladb/gocql/v2/internal/routing"
)

type AttemptEvent = routing.AttemptEvent

var CDCTokenPartitioner = routing.CDCTokenPartitioner

type HostSnapshot = model.HostSnapshot

type MetricsSnapshot = routing.MetricsSnapshot

var Murmur3TokenPartitioner = routing.Murmur3TokenPartitioner

func NewRoutingPolicy(a0 RoutingOptions) (RoutingPolicy, error) { return routing.NewRoutingPolicy(a0) }

type RoutingInfo = routing.RoutingInfo
type RoutingOptions = routing.RoutingOptions
type RoutingPolicy = routing.RoutingPolicy

type TokenPartitioner = routing.TokenPartitioner
