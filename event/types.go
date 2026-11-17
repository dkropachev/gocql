// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package event

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// Kind classifies an Event.
type Kind uint8

const (
	// Topology reports a node joining or leaving the cluster.
	Topology Kind = iota
	// Status reports a node going up or down.
	Status
	// Schema reports a keyspace, table, type, function or aggregate change.
	Schema
	// Control reports that the control connection reconnected to another host.
	Control
	// ClientRoute reports a change of the cluster's client routes.
	ClientRoute
	// Overflow is the last event of a Subscription, sent when its queue
	// overflows or when the session fails to publish a metadata change.
	Overflow
)

// Event is one cluster change delivered to a Subscription.
type Event struct {
	Address                          netip.AddrPort
	Change, Target, Keyspace, Object string
	ArgumentTypes                    []string
	Generation                       uint64
	HostID                           cql.UUID
	Kind                             Kind
}

// Filter selects the kinds of events a Subscription receives and its queue
// capacity.

// Filter selects the kinds of events a Subscription receives and its queue
// capacity.
type Filter struct {
	Keyspace string
	Kinds    []Kind
	Capacity int
}
