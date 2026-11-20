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
//
// The driver fills it; its zero value is empty, of Kind Unknown.
type Event struct {
	// Address is the node's address for a Topology, Status or Control event.
	Address netip.AddrPort
	// Change is what happened, as the server names it, such as "NEW_NODE",
	// "UP" or "CREATED", or as the driver does: "RECONNECTED" for Control,
	// "PUBLICATION_RESYNC" or "TABLET_RESYNC" for Overflow.
	Change string
	// Target is what a Schema event changed: "KEYSPACE", "TABLE", "TYPE",
	// "FUNCTION" or "AGGREGATE".
	Target string
	// Keyspace is the keyspace a Schema event concerns.
	Keyspace string
	// Object names the changed table, type, function or aggregate of a
	// Schema event, and is empty for a keyspace.
	Object string
	// ArgumentTypes lists the argument types of a changed function or
	// aggregate. For a ClientRoute event it lists the changed connection IDs,
	// then the changed host IDs.
	ArgumentTypes []string
	// Generation is the metadata generation when the event was published.
	Generation uint64
	// HostID is the new control host of a Control event.
	HostID cql.UUID
	// Kind classifies the event.
	Kind Kind
}

// Filter selects the kinds of events a Subscription receives and its queue
// capacity.
//
// The zero Filter receives every kind of event, of every keyspace, through a
// queue of 64 events.
type Filter struct {
	// Keyspace, when not empty, delivers only events of that keyspace, which
	// excludes events without one, such as Topology events. Overflow events
	// are always delivered.
	Keyspace string
	// Kinds, when not empty, lists the kinds to deliver. Overflow events are
	// always delivered.
	Kinds []Kind
	// Capacity is the queue length; zero means 64, and a negative value is
	// rejected.
	Capacity int
}
