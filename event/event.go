// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package event holds the cluster events a session delivers through
// gocql.Session.Subscribe.
package event

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

// Event is one cluster change delivered to a Subscription.
type Event = implCluster.Event

// Filter selects the kinds of events a Subscription receives and its queue
// capacity.
type Filter = implCluster.EventFilter

// Kind classifies an Event.
type Kind = implCluster.EventKind

// Subscription owns a bounded queue of events. Overflow discards queued
// deltas and emits Overflow, then closes. Subscribe again before reading
// gocql.Session.Metadata.
type Subscription = implCluster.Subscription

const (
	// Topology reports a node joining or leaving the cluster.
	Topology = implCluster.TopologyEvent
	// Status reports a node going up or down.
	Status = implCluster.StatusEvent
	// Schema reports a keyspace, table, type, function or aggregate change.
	Schema = implCluster.SchemaEvent
	// Control reports that the control connection reconnected to another host.
	Control = implCluster.ControlEvent
	// ClientRoute reports a change of the cluster's client routes.
	ClientRoute = implCluster.ClientRouteEvent
	// Overflow is the last event of a Subscription, sent when its queue
	// overflows or when the session fails to publish a metadata change.
	Overflow = implCluster.OverflowEvent
)
