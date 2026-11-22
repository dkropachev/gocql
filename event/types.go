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

// Change is what an Event reports happened. The driver keeps the server's
// text, so a change it does not name below still arrives, unchanged. The
// zero Change is empty and names no change. Later releases may add values,
// so a switch over them needs a default case.
type Change string

const (
	// ChangeNewNode is a Topology event for a node that joined the cluster.
	ChangeNewNode Change = "NEW_NODE"
	// ChangeRemovedNode is a Topology event for a node that left the cluster.
	ChangeRemovedNode Change = "REMOVED_NODE"
	// ChangeMovedNode is a Topology event for a node that moved its tokens.
	ChangeMovedNode Change = "MOVED_NODE"
	// ChangeUp is a Status event for a node that came up.
	ChangeUp Change = "UP"
	// ChangeDown is a Status event for a node that went down.
	ChangeDown Change = "DOWN"
	// ChangeCreated is a Schema event for a created object.
	ChangeCreated Change = "CREATED"
	// ChangeUpdated is a Schema event for an altered object, or a
	// ClientRoute event for changed routes.
	ChangeUpdated Change = "UPDATED"
	// ChangeDropped is a Schema event for a dropped object.
	ChangeDropped Change = "DROPPED"
	// ChangeRefreshed is a ClientRoute event sent after the driver reloaded
	// the client routes itself.
	ChangeRefreshed Change = "REFRESHED"
	// ChangeReconnected is a Control event: the control connection moved to
	// another host.
	ChangeReconnected Change = "RECONNECTED"
	// ChangeResync is an Overflow event sent when the subscription's queue
	// overflowed.
	ChangeResync Change = "RESYNC"
	// ChangePublicationResync is an Overflow event sent when the session
	// fails to publish a metadata change.
	ChangePublicationResync Change = "PUBLICATION_RESYNC"
	// ChangeTabletResync is an Overflow event sent when the session dropped
	// pending tablet routing updates.
	ChangeTabletResync Change = "TABLET_RESYNC"
)

// Target is the kind of object a Schema event changed. The zero Target is
// empty, as on every event that is not a Schema event. Later releases may
// add values, so a switch over them needs a default case.
type Target string

const (
	// TargetKeyspace is a keyspace change.
	TargetKeyspace Target = "KEYSPACE"
	// TargetTable is a table or materialized view change.
	TargetTable Target = "TABLE"
	// TargetType is a user-defined type change.
	TargetType Target = "TYPE"
	// TargetFunction is a user-defined function change.
	TargetFunction Target = "FUNCTION"
	// TargetAggregate is a user-defined aggregate change.
	TargetAggregate Target = "AGGREGATE"
)

// Event is one cluster change delivered to a Subscription.
//
// The driver fills it; its zero value is empty, of Kind Unknown.
type Event struct {
	// Address is the node's address for a Topology, Status or Control event.
	Address netip.AddrPort
	// Change is what happened, as the server names it, or as the driver
	// does for a Control or Overflow event.
	Change Change
	// Target is what a Schema event changed, and empty otherwise.
	Target Target
	// Keyspace is the keyspace a Schema event concerns.
	Keyspace string
	// Object names the changed table, type, function or aggregate of a
	// Schema event, and is empty for a keyspace.
	Object string
	// ArgumentTypes lists the argument types of a changed function or
	// aggregate, and is nil otherwise.
	ArgumentTypes []string
	// ConnectionIDs lists the connection IDs whose routes a ClientRoute
	// event changed, and is nil otherwise.
	ConnectionIDs []string
	// HostIDs lists the hosts whose routes a ClientRoute event changed, and
	// is nil otherwise.
	HostIDs []cql.UUID
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
