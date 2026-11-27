// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package event

import (
	"fmt"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// Kind classifies an Event. The zero Kind is KindUnknown. Later releases may
// add values, so a switch over them needs a default case.
type Kind uint8

const (
	// KindUnknown is the zero Kind. No delivered event has it, and a
	// subscription filter or config.WithIgnoreEvents rejects it.
	KindUnknown Kind = iota
	// KindTopology reports a node joining or leaving the cluster.
	KindTopology
	// KindStatus reports a node going up or down, as the server reports it.
	// The session's own view of whether it can reach a host is KindPool.
	// The session debounces status events: of several changes of one node
	// that arrive together, it applies, and delivers, only the latest.
	KindStatus
	// KindSchema reports a keyspace, table, view, type, function or aggregate
	// change.
	KindSchema
	// KindControl reports that the control connection reconnected to
	// another host.
	KindControl
	// KindClientRoute reports a change of the cluster's client routes.
	KindClientRoute
	// KindPool reports the session's connection pool to a host connecting
	// (ChangePoolUp) or the session marking the host down (ChangePoolDown).
	// It is how to watch whether the session can reach a host: a Status
	// event is only the server's report, which the session may not act on.
	// The driver raises it, not the server, so config.WithIgnoreEvents does
	// not take it.
	KindPool
	// KindOverflow reports that events were lost: the subscription's queue
	// overflowed, the session dropped events it could not process in time,
	// or it failed to publish a metadata change. The subscription stays
	// open; read Session.Metadata again to catch up.
	KindOverflow
)

// String returns the event kind name, or Kind(n) for a value it does not
// define.
func (k Kind) String() string {
	switch k {
	case KindUnknown:
		return "unknown"
	case KindTopology:
		return "topology"
	case KindStatus:
		return "status"
	case KindSchema:
		return "schema"
	case KindControl:
		return "control"
	case KindClientRoute:
		return "client route"
	case KindPool:
		return "pool"
	case KindOverflow:
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
	// ChangeDown is a Status event for a node the server reports down.
	ChangeDown Change = "DOWN"
	// ChangePoolUp is a Pool event for a host whose pool connected and that
	// the session marked up.
	ChangePoolUp Change = "POOL_UP"
	// ChangePoolDown is a Pool event for a host the session marked down and
	// dropped the pool of.
	ChangePoolDown Change = "POOL_DOWN"
	// ChangeCreated is a Schema event for a created object.
	ChangeCreated Change = "CREATED"
	// ChangeUpdated is a Schema event for an altered object.
	ChangeUpdated Change = "UPDATED"
	// ChangeDropped is a Schema event for a dropped object.
	ChangeDropped Change = "DROPPED"
	// ChangeRoutesUpdated is a ClientRoute event: the server reported that
	// client routes changed.
	ChangeRoutesUpdated Change = "ROUTES_UPDATED"
	// ChangeRoutesRefreshed is a ClientRoute event sent after the driver
	// reloaded the client routes itself.
	ChangeRoutesRefreshed Change = "ROUTES_REFRESHED"
	// ChangeReconnected is a Control event: the control connection moved to
	// another host.
	ChangeReconnected Change = "RECONNECTED"
	// ChangeResync is an Overflow event sent when the subscription's queue
	// overflowed.
	ChangeResync Change = "RESYNC"
	// ChangePublicationResync is an Overflow event sent when the session
	// fails to publish a metadata change, and when a schema refresh retried
	// after an Event.Incomplete event published the schema that event
	// lacked.
	ChangePublicationResync Change = "PUBLICATION_RESYNC"
)

// ObjectKind is the kind of object a Schema event changed. The zero
// ObjectKind is empty, as on every event that is not a Schema event. Later
// releases may add values, so a switch over them needs a default case.
type ObjectKind string

const (
	// ObjectKeyspace is a keyspace change.
	ObjectKeyspace ObjectKind = "KEYSPACE"
	// ObjectTable is a table change.
	ObjectTable ObjectKind = "TABLE"
	// ObjectView is a materialized view change. The server reports a view as
	// a table; the driver names it a view when the metadata snapshot of the
	// event, or the one before it for a drop, holds a view of that name.
	ObjectView ObjectKind = "VIEW"
	// ObjectType is a user-defined type change.
	ObjectType ObjectKind = "TYPE"
	// ObjectFunction is a user-defined function change.
	ObjectFunction ObjectKind = "FUNCTION"
	// ObjectAggregate is a user-defined aggregate change.
	ObjectAggregate ObjectKind = "AGGREGATE"
)

// Event is one cluster change delivered to a Subscription.
//
// The driver fills it; its zero value is empty, of KindUnknown.
type Event struct {
	// Address is the node's address for a Topology, Status or Control
	// event, and the address the driver connects to for a Pool event.
	Address netip.AddrPort
	// Change is what happened, as the server names it, or as the driver
	// does for a Control or Overflow event.
	Change Change
	// ObjectKind is what a Schema event changed, and empty otherwise.
	ObjectKind ObjectKind
	// Keyspace is the keyspace a Schema event concerns.
	Keyspace string
	// Object names the changed table, view, type, function or aggregate of
	// a Schema event, and is empty for a keyspace. It is the server's
	// internal name, as metadata.Snapshot lookups take it.
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
	// The session publishes the metadata that reflects a change before it
	// delivers the change's event, so a snapshot of this generation or later
	// already holds it, unless the event is Incomplete. An Overflow event's is at least the highest
	// generation among the events it replaced.
	Generation uint64
	// HostID is the node of a Topology, Status or Pool event, or the new
	// control host of a Control event, and zero when the driver does not
	// know the node's ID. A REMOVED_NODE event takes it from the metadata
	// before the removal. It is the key that correlates an event with
	// metadata.Snapshot.Host and with other events of the node: a node's
	// Address can change, and a Pool event's is the address the driver
	// connects to, which a translator can make differ from the address the
	// server reports.
	HostID cql.UUID
	// Kind classifies the event.
	Kind Kind
	// Incomplete reports a Schema event whose metadata refresh failed: the
	// snapshot of Generation does not hold the change yet. The session
	// retries the refresh in the background and, once it publishes the
	// schema, delivers an Overflow event with ChangePublicationResync.
	Incomplete bool
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
