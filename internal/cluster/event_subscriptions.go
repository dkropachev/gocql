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
	if e.Kind == event.Overflow {
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
	if e.Kind != event.Overflow {
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
			change := "RESYNC"
			if e.Kind == event.Overflow && e.Change != "" {
				change = e.Change
			}
			s.privateStorage.events <- event.Event{Kind: event.Overflow, Generation: highest, Change: change}
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
		if kind > event.Overflow {
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
	sessionObserveV2Lifecycle(s, observe.LifecycleEvent{Kind: "session", Stage: "closed", At: time.Now()})
}
func eventV2Address(host net.IP, port int) netip.AddrPort {
	ip, ok := netip.AddrFromSlice(host)
	if !ok || port < 0 || port > 65535 {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ip.Unmap(), uint16(port))
}
func (s *Core) PublishV2Event(source oldevents.Event) {
	s.publishV2EventMode(source, true, true)
}
func (s *Core) recordV2Event(source oldevents.Event) { s.publishV2EventMode(source, true, false) }
func (s *Core) emitV2Event(source oldevents.Event)   { s.publishV2EventMode(source, false, true) }
func (s *Core) publishV2EventMode(source oldevents.Event, invalidate, deliver bool) {
	p := s.Peripheral()
	if p == nil {
		return
	}
	e := event.Event{Generation: s.Metadata().Generation()}
	switch v := source.(type) {
	case *oldevents.TopologyChangeEvent:
		e.Kind = event.Topology
		e.Change = v.Change
		e.Address = eventV2Address(v.Host, v.Port)
	case *oldevents.StatusChangeEvent:
		e.Kind = event.Status
		e.Change = v.Change
		e.Address = eventV2Address(v.Host, v.Port)
	case *oldevents.SchemaChangeKeyspaceEvent:
		e.Kind = event.Schema
		e.Target = "KEYSPACE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
	case *oldevents.SchemaChangeTableEvent:
		e.Kind = event.Schema
		e.Target = "TABLE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.Table
	case *oldevents.SchemaChangeTypeEvent:
		e.Kind = event.Schema
		e.Target = "TYPE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.TypeName
	case *oldevents.SchemaChangeFunctionEvent:
		e.Kind = event.Schema
		e.Target = "FUNCTION"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.Function
		e.ArgumentTypes = v.Arguments
	case *oldevents.SchemaChangeAggregateEvent:
		e.Kind = event.Schema
		e.Target = "AGGREGATE"
		e.Change = v.Change
		e.Keyspace = v.Keyspace
		e.Object = v.Aggregate
		e.ArgumentTypes = v.Arguments
	case *oldevents.ClientRoutesChangedEvent:
		e.Kind = event.ClientRoute
		e.Change = v.ChangeType
		e.ArgumentTypes = append(append([]string(nil), v.ConnectionIDs...), v.HostIDs...)
	case *oldevents.ControlConnectionRecreatedEvent:
		e.Kind = event.Control
		e.Change = "RECONNECTED"
		e.Address = eventV2Address(v.NewHost.Host, v.NewHost.Port)
		e.HostID, _ = cql.ParseUUID(v.NewHost.HostID)
	default:
		return
	}
	if e.Kind == event.Schema && invalidate {
		s.InvalidatePreparedSchema(e)
		p.InternalMu.Lock()
		p.InternalPublicationRevision++
		p.bumpSchemaEpochLocked(e.Keyspace)
		p.schemaGeneration.Add(1)
		if e.Target == "KEYSPACE" {
			if e.Change == "DROPPED" {
				p.schemaDropped[e.Keyspace] = true
			}
			if e.Change == "CREATED" {
				delete(p.schemaDropped, e.Keyspace)
			}
		}
		if e.Change == "DROPPED" && s.privateStorage.InternalMetadataDescriber != nil {
			md := s.privateStorage.InternalMetadataDescriber.InternalMetadata
			if e.Target == "KEYSPACE" {
				md.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(e.Keyspace)
			} else {
				md.privateStorage.InternalKeyspaceMetadata.InternalUpdateKeyspace(e.Keyspace, func(ks *metadata.Keyspace) {
					switch e.Target {
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
				s.PublishV2LocalEvent(event.Event{Kind: event.Overflow, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
				return
			}
			e.Generation = s.Metadata().Generation()
		}
	}
	if !deliver {
		return
	}
	if e.HostID == (cql.UUID{}) && e.Address.IsValid() {
		for _, host := range routing.SnapshotData(s.Metadata()).InternalHosts {
			if host.Address == e.Address {
				e.HostID = host.ID
				break
			}
		}
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
