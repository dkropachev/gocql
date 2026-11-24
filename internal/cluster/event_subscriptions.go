// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"net"
	"net/netip"
	"slices"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	oldevents "github.com/scylladb/gocql/v2/internal/events"
	"github.com/scylladb/gocql/v2/internal/link"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
)

// Kind classifies an Event.

// Event is one cluster change delivered to a Subscription.
// Filter selects the kinds of events a Subscription receives and its queue
// capacity.

// Subscription owns a bounded queue. Overflow discards queued deltas and emits
// OverflowEvent, then closes. Subscribe again before reading Session.Metadata.
type Subscription struct {
	// public is the handle package event exposes.
	public         *event.Subscription
	privateStorage SubscriptionStorage
}

var newPublicSubscription = link.Get[func(<-chan event.Event, func()) *event.Subscription](link.NewSubscription)

// Public returns the event.Subscription handle of s.
func (s *Subscription) Public() *event.Subscription {
	if s == nil {
		return nil
	}
	return s.public
}

func (s *Subscription) Events() <-chan event.Event {
	if s == nil {
		return nil
	}
	return s.privateStorage.events
}
func (s *Subscription) Close() {
	if s == nil {
		return
	}
	s.privateStorage.mu.Lock()
	if !s.privateStorage.closed {
		s.privateStorage.closed = true
		close(s.privateStorage.events)
	}
	s.privateStorage.mu.Unlock()
	if s.privateStorage.parent != nil {
		s.privateStorage.parent.InternalMu.Lock()
		delete(s.privateStorage.parent.InternalSubscribers, s)
		s.privateStorage.parent.InternalMu.Unlock()
	}
}
func (s *Subscription) accepts(e event.Event) bool {
	if e.Kind == event.KindOverflow {
		return true
	}
	if s.privateStorage.filter.Keyspace != "" && e.Keyspace != s.privateStorage.filter.Keyspace {
		return false
	}
	if len(s.privateStorage.filter.Kinds) == 0 {
		return true
	}
	for _, kind := range s.privateStorage.filter.Kinds {
		if kind == e.Kind {
			return true
		}
	}
	return false
}
func InternalSubscriptionPublish(s *Subscription, e event.Event) {
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	if s.privateStorage.closed || !s.accepts(e) {
		return
	}
	e.ArgumentTypes = append([]string(nil), e.ArgumentTypes...)
	e.ConnectionIDs = append([]string(nil), e.ConnectionIDs...)
	e.HostIDs = append([]cql.UUID(nil), e.HostIDs...)
	if e.Kind != event.KindOverflow {
		select {
		case s.privateStorage.events <- e:
			return
		default:
		}
	}
	highest := e.Generation
	for {
		select {
		case discarded := <-s.privateStorage.events:
			if discarded.Generation > highest {
				highest = discarded.Generation
			}
		default:
			change := event.ChangeResync
			if e.Kind == event.KindOverflow && e.Change != "" {
				change = e.Change
			}
			s.privateStorage.events <- event.Event{Kind: event.KindOverflow, Generation: highest, Change: change}
			s.privateStorage.closed = true
			close(s.privateStorage.events)
			return
		}
	}
}
func (s *Core) Subscribe(filter event.Filter) (*Subscription, error) {
	if s == nil || s.Closed() {
		return nil, cqlerr.ErrClosed
	}
	if filter.Capacity < 0 {
		return nil, cqltype.TypeValidation("subscription capacity", "must not be negative")
	}
	if filter.Capacity == 0 {
		filter.Capacity = 64
	}
	for _, kind := range filter.Kinds {
		if kind == event.KindUnknown || kind > event.KindOverflow {
			return nil, cqltype.TypeValidation("event filter", "unknown event kind")
		}
	}
	filter.Kinds = append([]event.Kind(nil), filter.Kinds...)
	p := s.Peripheral()
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed {
		return nil, cqlerr.ErrClosed
	}
	sub := func() *Subscription {
		opaqueStorageValue := new(Subscription)
		opaqueStorageValue.privateStorage.events = make(chan event.Event, filter.Capacity)
		opaqueStorageValue.privateStorage.filter = filter
		opaqueStorageValue.privateStorage.parent = p
		opaqueStorageValue.public = newPublicSubscription(opaqueStorageValue.privateStorage.events, opaqueStorageValue.Close)
		return opaqueStorageValue
	}()
	p.InternalSubscribers[sub] = struct{}{}
	return sub, nil
}
func (s *Core) CloseV2Peripheral() {
	p := s.Peripheral()
	if p == nil {
		return
	}
	p.InternalMu.Lock()
	if p.closed {
		p.InternalMu.Unlock()
		return
	}
	p.closed = true
	close(p.InternalDone)
	subs := make([]*Subscription, 0, len(p.InternalSubscribers))
	for sub := range p.InternalSubscribers {
		subs = append(subs, sub)
	}
	p.InternalMu.Unlock()
	for _, sub := range subs {
		sub.Close()
	}
	sessionObserveV2Lifecycle(s, observe.LifecycleEvent{Kind: observe.KindSession, Stage: observe.StageClosed, At: time.Now()})
}
func eventV2Address(host net.IP, port int) netip.AddrPort {
	ip, ok := netip.AddrFromSlice(host)
	if !ok || port < 0 || port > 65535 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(port))
}
func (s *Core) PublishV2Event(source oldevents.Event) {
	s.publishV2EventMode(source, true, true, nil)
}
func (s *Core) recordV2Event(source oldevents.Event) { s.publishV2EventMode(source, true, false, nil) }
func (s *Core) emitV2Event(source oldevents.Event)   { s.publishV2EventMode(source, false, true, nil) }

// emitV2NodeEvent delivers a topology or status event, taking a host ID the
// current snapshot no longer has, such as a removed node's, from prev.
func (s *Core) emitV2NodeEvent(source oldevents.Event, prev *routing.MetadataSnapshot) {
	s.publishV2EventMode(source, false, true, prev)
}

func (s *Core) publishV2EventMode(source oldevents.Event, invalidate, deliver bool, prev *routing.MetadataSnapshot) {
	p := s.Peripheral()
	if p == nil {
		return
	}
	e := event.Event{Generation: s.Metadata().Generation()}
	switch v := source.(type) {
	case *oldevents.TopologyChangeEvent:
		e.Kind = event.KindTopology
		e.Change = event.Change(v.Change)
		e.Address = eventV2Address(v.Host, v.Port)
	case *oldevents.StatusChangeEvent:
		e.Kind = event.KindStatus
		e.Change = event.Change(v.Change)
		e.Address = eventV2Address(v.Host, v.Port)
	case *oldevents.SchemaChangeKeyspaceEvent:
		e.Kind = event.KindSchema
		e.ObjectKind = event.ObjectKeyspace
		e.Change = event.Change(v.Change)
		e.Keyspace = v.Keyspace
	case *oldevents.SchemaChangeTableEvent:
		e.Kind = event.KindSchema
		e.ObjectKind = event.ObjectTable
		e.Change = event.Change(v.Change)
		e.Keyspace = v.Keyspace
		e.Object = v.Table
	case *oldevents.SchemaChangeTypeEvent:
		e.Kind = event.KindSchema
		e.ObjectKind = event.ObjectType
		e.Change = event.Change(v.Change)
		e.Keyspace = v.Keyspace
		e.Object = v.TypeName
	case *oldevents.SchemaChangeFunctionEvent:
		e.Kind = event.KindSchema
		e.ObjectKind = event.ObjectFunction
		e.Change = event.Change(v.Change)
		e.Keyspace = v.Keyspace
		e.Object = v.Function
		e.ArgumentTypes = v.Arguments
	case *oldevents.SchemaChangeAggregateEvent:
		e.Kind = event.KindSchema
		e.ObjectKind = event.ObjectAggregate
		e.Change = event.Change(v.Change)
		e.Keyspace = v.Keyspace
		e.Object = v.Aggregate
		e.ArgumentTypes = v.Arguments
	case *oldevents.ClientRoutesChangedEvent:
		e.Kind = event.KindClientRoute
		e.Change = event.Change(v.ChangeType)
		e.ConnectionIDs = append([]string(nil), v.ConnectionIDs...)
		e.HostIDs = s.clientRouteHostIDs(v.HostIDs)
	case *oldevents.ControlConnectionRecreatedEvent:
		e.Kind = event.KindControl
		e.Change = event.ChangeReconnected
		e.Address = eventV2Address(v.NewHost.Host, v.NewHost.Port)
		e.HostID, _ = cql.ParseUUID(v.NewHost.HostID)
	default:
		return
	}
	if e.Kind == event.KindSchema && invalidate {
		s.InvalidatePreparedSchema(e)
		p.InternalMu.Lock()
		p.InternalPublicationRevision++
		p.bumpSchemaEpochLocked(e.Keyspace)
		p.schemaGeneration.Add(1)
		if e.ObjectKind == event.ObjectKeyspace {
			if e.Change == "DROPPED" {
				p.schemaDropped[e.Keyspace] = true
			}
			if e.Change == "CREATED" {
				delete(p.schemaDropped, e.Keyspace)
			}
		}
		if e.Change == "DROPPED" && s.privateStorage.InternalMetadataDescriber != nil {
			md := s.privateStorage.InternalMetadataDescriber.InternalMetadata
			if e.ObjectKind == event.ObjectKeyspace {
				md.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(e.Keyspace)
			} else {
				md.privateStorage.InternalKeyspaceMetadata.InternalUpdateKeyspace(e.Keyspace, func(ks *metadata.Keyspace) {
					switch e.ObjectKind {
					case "TABLE":
						implMetadata.InternalKeyspaceMetadataRemoveTable(ks, e.Object)
					case "TYPE":
						delete(ks.Types, e.Object)
					case "FUNCTION":
						for key, f := range ks.Functions {
							if f.Name == e.Object && slices.Equal(f.ArgumentTypes, e.ArgumentTypes) {
								delete(ks.Functions, key)
							}
						}
					case "AGGREGATE":
						for key, a := range ks.Aggregates {
							if a.Name == e.Object && slices.Equal(a.ArgumentTypes, e.ArgumentTypes) {
								delete(ks.Aggregates, key)
							}
						}
					}
				})
			}
		}
		p.InternalMu.Unlock()
		if deliver || e.Change == "DROPPED" {
			if !s.PublishV2Metadata() {
				s.PublishV2LocalEvent(event.Event{Kind: event.KindOverflow, Generation: s.Metadata().Generation(), Change: event.ChangePublicationResync})
				return
			}
			e.Generation = s.Metadata().Generation()
		}
	}
	if !deliver {
		return
	}
	if e.HostID == (cql.UUID{}) && e.Address.IsValid() {
		snapshots := []*routing.MetadataSnapshot{s.Metadata(), prev}
		if e.Change == event.ChangeRemovedNode {
			// The ring update has already dropped a removed node.
			snapshots[0], snapshots[1] = prev, snapshots[0]
		}
		e.HostID = eventHostID(e.Address, snapshots...)
	}
	p.InternalMu.Lock()
	subs := make([]*Subscription, 0, len(p.InternalSubscribers))
	for sub := range p.InternalSubscribers {
		subs = append(subs, sub)
	}
	p.InternalMu.Unlock()
	for _, sub := range subs {
		InternalSubscriptionPublish(sub, e)
		sub.privateStorage.mu.Lock()
		closed := sub.privateStorage.closed
		sub.privateStorage.mu.Unlock()
		if closed {
			p.InternalMu.Lock()
			delete(p.InternalSubscribers, sub)
			p.InternalMu.Unlock()
		}
	}
}

// eventHostID returns the ID of the host at addr in the first snapshot that
// has one, or the zero UUID.
func eventHostID(addr netip.AddrPort, snapshots ...*routing.MetadataSnapshot) cql.UUID {
	for _, m := range snapshots {
		if m == nil {
			continue
		}
		for _, host := range routing.SnapshotData(m).InternalHosts {
			if host.Address == addr {
				return host.HostID
			}
		}
	}
	return cql.UUID{}
}

func (s *Core) PublishV2LocalEvent(event event.Event) {
	p := s.Peripheral()
	if p == nil {
		return
	}
	p.InternalMu.Lock()
	subs := make([]*Subscription, 0, len(p.InternalSubscribers))
	for sub := range p.InternalSubscribers {
		subs = append(subs, sub)
	}
	p.InternalMu.Unlock()
	for _, sub := range subs {
		InternalSubscriptionPublish(sub, event)
		sub.privateStorage.mu.Lock()
		closed := sub.privateStorage.closed
		sub.privateStorage.mu.Unlock()
		if closed {
			p.InternalMu.Lock()
			delete(p.InternalSubscribers, sub)
			p.InternalMu.Unlock()
		}
	}
}

// clientRouteHostIDs parses the host IDs of a CLIENT_ROUTES_CHANGE event. The
// server sends UUID text; an ID that does not parse is logged and dropped.
func (s *Core) clientRouteHostIDs(ids []string) []cql.UUID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]cql.UUID, 0, len(ids))
	for _, id := range ids {
		u, err := cql.ParseUUID(id)
		if err != nil {
			s.logger().Debug("dropping invalid host ID of a client routes event", "host_id", id, "err", err)
			continue
		}
		out = append(out, u)
	}
	return out
}
