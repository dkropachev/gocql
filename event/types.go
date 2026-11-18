// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package event

import (
	"fmt"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// Kind classifies an Event. The zero Kind is Unknown. Later releases may add
// values, so a switch over them needs a default case.
type Kind uint8

const (
	// Unknown is the zero Kind. No delivered event has it, and a
	// subscription filter or config.WithIgnoreEvents rejects it.
	Unknown Kind = iota
	// Topology reports a node joining or leaving the cluster.
	Topology
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

// String returns the event kind name, or Kind(n) for a value it does not
// define.
func (k Kind) String() string {
	switch k {
	case Unknown:
		return "unknown"
	case Topology:
		return "topology"
	case Status:
		return "status"
	case Schema:
		return "schema"
	case Control:
		return "control"
	case ClientRoute:
		return "client route"
	case Overflow:
		return "overflow"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

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
