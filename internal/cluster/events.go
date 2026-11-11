/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"log/slog"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/events"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type InternalEventDebouncer struct {
	InternalLogger   *slog.Logger
	InternalTimer    *time.Timer
	callback         func([]protocol.Frame)
	quit             chan struct{}
	done             chan struct{}
	InternalResync   func()
	name             string
	InternalEvents   []protocol.Frame
	releases         []func()
	stopOnce         sync.Once
	InternalMu       sync.Mutex
	stopped          bool
	InternalOverflow bool
}

func InternalNewEventDebouncer(name string, eventHandler func([]protocol.Frame), logger *slog.Logger) *InternalEventDebouncer {
	logger = logging.Or(logger)
	e := &InternalEventDebouncer{
		name: name,
		quit: make(chan struct{}), done: make(chan struct{}),
		InternalTimer:  time.NewTimer(eventDebounceTime),
		callback:       eventHandler,
		InternalLogger: logger,
	}
	e.InternalTimer.Stop()
	go e.flusher()

	return e
}

func (e *InternalEventDebouncer) InternalStop() {
	e.stopOnce.Do(func() {
		e.InternalMu.Lock()
		e.stopped = true
		e.InternalTimer.Stop()
		close(e.quit)
		e.InternalMu.Unlock()
	})
	<-e.done
}
func (e *InternalEventDebouncer) flusher() {
	defer close(e.done)
	defer func() {
		e.InternalMu.Lock()
		releases := e.releases
		e.releases = nil
		e.InternalEvents = nil
		e.InternalMu.Unlock()
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}()
	for {
		select {
		case <-e.quit:
			return
		case <-e.InternalTimer.C:
			e.InternalMu.Lock()
			pending := e.InternalEvents
			releases := e.releases
			e.releases = nil
			overflow := e.InternalOverflow
			e.InternalOverflow = false
			e.InternalEvents = nil
			stopped := e.stopped
			e.InternalMu.Unlock()
			func() {
				defer func() {
					for _, release := range releases {
						if release != nil {
							release()
						}
					}
				}()
				if !stopped && len(pending) > 0 {
					e.callback(pending)
				}
			}()
			if !stopped && overflow && e.InternalResync != nil {
				e.InternalResync()
			}
		}
	}
}

const (
	InternalEventBufferSize = 1000
	eventDebounceTime       = 1 * time.Second
)

// flush must be called with mu locked
func (e *InternalEventDebouncer) flush() {
	if len(e.InternalEvents) == 0 {
		return
	}

	// if the flush interval is faster than the callback then we will end up calling
	// the callback multiple times, probably a bad idea. In this case we could drop
	// frames?
	pending := e.InternalEvents
	releases := e.releases
	e.InternalEvents = nil
	e.releases = nil
	e.InternalMu.Unlock()
	func() {
		defer func() {
			for _, release := range releases {
				if release != nil {
					release()
				}
			}
		}()
		e.callback(pending)
	}()
	e.InternalMu.Lock()
}

func (e *InternalEventDebouncer) InternalDebounce(frame protocol.Frame) {
	e.InternalDebounceOwned(frame, nil)
}
func (e *InternalEventDebouncer) InternalDebounceOwned(frame protocol.Frame, release func()) bool {
	return e.debounceOwnedContext(context.Background(), frame, release)
}
func (e *InternalEventDebouncer) debounceOwnedContext(ctx context.Context, frame protocol.Frame, release func()) bool {
	e.InternalMu.Lock()
	if e.stopped || ctx.Err() != nil {
		e.InternalMu.Unlock()
		return false
	}
	e.InternalTimer.Reset(eventDebounceTime)

	// TODO: probably need a warning to track if this threshold is too low
	if len(e.InternalEvents) < InternalEventBufferSize {
		e.InternalEvents = append(e.InternalEvents, frame)
		e.releases = append(e.releases, release)
		e.InternalMu.Unlock()
		return true
	} else {
		e.InternalOverflow = true
	}
	e.InternalMu.Unlock()
	return false
}

func sessionPublishEvent(s *Core, event events.Event) {
	s.PublishV2Event(event)
	if s.privateStorage.InternalClientRoutesHandler != nil && s.privateStorage.InternalClientRoutesHandler.privateStorage.initialized.Load() {
		s.privateStorage.InternalClientRoutesHandler.handleRouteEvent(event)
	}
	if s.privateStorage.eventBus == nil {
		return
	}

	if !s.privateStorage.eventBus.PublishEvent(event) {
		s.logger().Warn("eventbus is full; event dropped (increase EventBusConfig.InputEventsQueueSize)")
	}
}

func (s *Core) HandleEvent(frame protocol.Frame) { s.handleEventOwned(frame, nil) }
func (s *Core) handleEventOwned(frame protocol.Frame, release func()) bool {
	return s.handleEventOwnedContext(context.Background(), frame, release)
}
func (s *Core) handleEventOwnedContext(ctx context.Context, frame protocol.Frame, release func()) bool {
	switch frame.(type) {
	case frm.SchemaChange:
		if s.privateStorage.InternalCfg.Events.DisableSchemaEvents {
			return false
		}
	case *frm.TopologyChangeEventFrame:
		if s.privateStorage.InternalCfg.Events.DisableTopologyEvents {
			return false
		}
	case *frm.StatusChangeEventFrame:
		if s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
			return false
		}
	case *frm.ClientRoutesChanged:
		if s.privateStorage.InternalCfg.privateStorage.InternalDisableClientRouteEvents {
			return false
		}
	}
	s.logger().Debug("handling event frame", "frame", frame)

	source := events.FrameToEvent(frame)
	if source != nil {
		s.recordV2Event(source)
	}
	switch e := source.(type) {
	case *events.TopologyChangeEvent:
		s.RecordV2Topology(e)
	case *events.StatusChangeEvent:
		if e.Change == "DOWN" && !s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
			s.fenceV2Down(e.Host, e.Port)
		}
	}
	switch f := frame.(type) {
	case frm.SchemaChange:
		return s.privateStorage.InternalSchemaEvents.debounceOwnedContext(ctx, frame, release)
	case *frm.TopologyChangeEventFrame, *frm.StatusChangeEventFrame:
		return s.privateStorage.InternalNodeEvents.debounceOwnedContext(ctx, frame, release)
	case *frm.ClientRoutesChanged:
		if s.privateStorage.InternalClientRoutesHandler != nil {
			return s.privateStorage.InternalClientRoutesHandler.HandleRouteEventOwned(source, release)
		}
	default:
		s.logger().Warn("invalid event frame", "type", fmt.Sprintf("%T", f), "frame", f)
	}
	return false

}

func sessionHandleSchemaEvent(s *Core, frames []protocol.Frame) {
	s.PublishV2Metadata()
	defer s.PublishV2Metadata()
	defer s.scheduleV2SchemaRetry()
	// TODO: debounce events
	for _, frame := range frames {
		switch f := frame.(type) {
		case *frm.SchemaChangeKeyspace:
			s.MarkV2SchemaStale(f.Keyspace, f.Change == "DROPPED")
			s.privateStorage.InternalMetadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
			s.handleKeyspaceChange(f.Keyspace, f.Change)
		case *frm.SchemaChangeTable:
			s.privateStorage.InternalMetadataDescriber.invalidateTableSchema(f.Keyspace, f.Object)
			s.handleTableChange(f.Keyspace, f.Object, f.Change)
		case *frm.SchemaChangeAggregate:
			s.MarkV2SchemaStale(f.Keyspace, false)
			s.privateStorage.InternalMetadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
		case *frm.SchemaChangeFunction:
			s.MarkV2SchemaStale(f.Keyspace, false)
			s.privateStorage.InternalMetadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
		case *frm.SchemaChangeType:
			s.MarkV2SchemaStale(f.Keyspace, false)
			s.privateStorage.InternalMetadataDescriber.invalidateKeyspaceSchema(f.Keyspace)
		}
	}
}

func (s *Core) handleSchemaEventAndPublish(frames []protocol.Frame) {
	sessionHandleSchemaEvent( // Coalesce relevant schema work while keeping each bounded event's publication
		// behind the resulting visible metadata (including diagnostics on failure).
		s, frames)
	scopes := make(map[string]bool)
	for _, f := range frames {
		if v, ok := f.(frm.SchemaChange); ok {
			scopes[v.SchemaKeyspace()] = true
		}
	}
	for scope := range scopes {
		if err := s.RefreshV2Keyspace(metadata.InternalV2SchemaContext(s.NewSchemaSource()), scope); err != nil {
			s.scheduleV2SchemaRetry()
		}
	}
	if !s.PublishV2Metadata() {
		s.PublishV2LocalEvent(Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
		return
	}
	for _, f := range frames {
		if event := events.FrameToEvent(f); event != nil {
			s.emitV2Event(event)
			if s.privateStorage.eventBus != nil {
				s.privateStorage.eventBus.PublishEvent(event)
			}
		}
	}
}

func (s *Core) handleKeyspaceChange(keyspace, change string) {
	if change == "DROPPED" || change == "UPDATED" {
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(keyspace)
	}
	s.privateStorage.InternalPolicy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace, Change: change})
}

func (s *Core) handleTableChange(keyspace, table, change string) {
	if change == "DROPPED" || change == "UPDATED" {
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithTable(keyspace, table)
	}
}

// handleNodeEvent handles inbound status and topology change events.
//
// Status events are debounced by host IP; only the latest event is processed.
//
// Topology events are debounced by performing a single full topology refresh
// whenever any topology event comes in.
//
// Topology events are tracked separately from that map, rather than sharing it,
// so that a NEW_NODE is not dropped in favor of a newer UP for the same host
// (which would itself be dropped/ignored, as the node is not yet known). They
// are also refreshed regardless of DisableNodeStatusEvents -- only
// DisableTopologyEvents suppresses them.
func (s *Core) HandleNodeEvent(frames []protocol.Frame) {
	type nodeEvent struct {
		change string
		host   net.IP
		port   int
	}

	topologyEventReceived := false
	// status change events
	sEvents := make(map[string]*nodeEvent)

	for _, frame := range frames {
		switch f := frame.(type) {
		case *frm.TopologyChangeEventFrame:
			topologyEventReceived = true
		case *frm.StatusChangeEventFrame:
			event, ok := sEvents[eventV2Address(f.Host, f.Port).String()]
			if !ok {
				event = &nodeEvent{change: f.Change, host: f.Host, port: f.Port}
				sEvents[eventV2Address(f.Host, f.Port).String()] = event
			}
			event.change = f.Change
		}
	}

	if topologyEventReceived && !s.privateStorage.InternalCfg.Events.DisableTopologyEvents {
		_ = s.RefreshRingNow()
	}

	for _, f := range sEvents {
		s.logger().Debug("dispatching status change event", "change", f.change, logging.KeyHost, net.JoinHostPort(f.host.String(), strconv.Itoa(f.port)))

		// ignore events we received if they were disabled
		// see https://github.com/apache/cassandra-gocql-driver/issues/1591
		switch f.change {
		case "UP":
			if !s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
				sessionHandleNodeUp(s, f.host, f.port)
			}
		case "DOWN":
			if !s.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
				s.HandleNodeDown(f.host, f.port)
			}
		}
	}
	if !s.PublishV2Metadata() {
		s.PublishV2LocalEvent(Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "PUBLICATION_RESYNC"})
		return
	}
	for _, f := range frames {
		if event := events.FrameToEvent(f); event != nil {
			s.emitV2Event(event)
			if s.privateStorage.eventBus != nil {
				s.privateStorage.eventBus.PublishEvent(event)
			}
		}
	}

}

func sessionHandleNodeUp(s *Core, eventIp net.IP, eventPort int) {
	s.logger().Info("node up", logging.KeyHost, net.JoinHostPort(eventIp.String(), strconv.Itoa(eventPort)))

	host, ok := s.privateStorage.InternalHostSource.getHostByIP(eventIp.String())
	if !ok {
		s.debounceRingRefresh()
		return
	}

	if s.privateStorage.InternalCfg.FilterHost(host) {
		return
	}

	s.startPoolFill(host)
}

func (s *Core) startPoolFill(host *implEndpoint.HostInfo) {
	if s.Closed() {
		return
	}
	p := s.Peripheral()
	p.hostMu.Lock()
	if s.privateStorage.InternalHostSource.InternalGetHost(host.HostID()) != host {
		p.hostMu.Unlock()
		return
	}
	pool := s.privateStorage.InternalPool.InternalEnsureHost(host)
	p.hostMu.Unlock()
	s.privateStorage.InternalPolicy.AddHost(host)
	pool.InternalFill_debounce()
}

func (s *Core) HandlePoolConnected(pool *pool.InternalHostConnPool) {
	if s == nil || s.Closed() || s.privateStorage.InternalPool == nil || s.privateStorage.InternalHostSource == nil {
		return
	}
	p := s.Peripheral()
	p.hostMu.Lock()
	current, ok := s.privateStorage.InternalPool.InternalGetPool(pool.InternalHost)
	known := s.privateStorage.InternalHostSource.InternalGetHost(pool.InternalHost.HostID())
	if !ok || current != pool || known != pool.InternalHost || pool.Size() == 0 {
		p.hostMu.Unlock()
		return
	}
	implEndpoint.InternalHostInfoSetState(pool.InternalHost, implEndpoint.NodeUp)
	p.InternalMu.Lock()
	p.InternalPublicationRevision++
	p.InternalMu.Unlock()
	p.hostMu.Unlock()
	s.PublishV2Metadata()
	sessionObserveV2Lifecycle(s, settings.LifecycleEvent{Kind: "host", Stage: "connected", HostID: implEndpoint.InternalHostInfoHostUUID(pool.InternalHost), At: time.Now()})
	s.synchronizeV2HostPolicy(pool.InternalHost)
}
func sessionHandleNodeConnected(s *Core, host *implEndpoint.HostInfo) {
	if s.privateStorage.InternalPool != nil {
		if pool, ok := s.privateStorage.InternalPool.InternalGetPool(host); ok {
			s.HandlePoolConnected(pool)
		}
		return
	}
	defer s.PublishV2Metadata()
	s.logger().Debug("node connected", logging.KeyHost, net.JoinHostPort(host.ConnectAddress().String(), strconv.Itoa(host.Port())))
	implEndpoint.InternalHostInfoSetState(host, implEndpoint.NodeUp)
	sessionObserveV2Lifecycle(s, settings.LifecycleEvent{Kind: "host", Stage: "connected", HostID: implEndpoint.InternalHostInfoHostUUID(host), At: time.Now()})

	if !s.privateStorage.InternalCfg.FilterHost(host) {
		s.privateStorage.InternalPolicy.HostUp(host)
	}
}

func (s *Core) HandleNodeDown(ip net.IP, port int) {
	defer s.PublishV2Metadata()
	s.logger().Info("node down", logging.KeyHost, net.JoinHostPort(ip.String(), strconv.Itoa(port)))

	host, ok := s.privateStorage.InternalHostSource.getHostByIP(ip.String())
	if ok {
		filtered := s.privateStorage.InternalCfg.FilterHost(host)
		p := s.Peripheral()
		p.hostMu.Lock()
		implEndpoint.InternalHostInfoSetState(host, implEndpoint.NodeDown)
		p.InternalMu.Lock()
		p.InternalPublicationRevision++
		p.InternalMu.Unlock()
		if !filtered && s.privateStorage.InternalPool != nil {
			s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(host))
		}
		p.hostMu.Unlock()
		sessionObserveV2Lifecycle(s, settings.LifecycleEvent{Kind: "host", Stage: "down", HostID: implEndpoint.InternalHostInfoHostUUID(host), At: time.Now()})
		if filtered {
			return
		}
		s.synchronizeV2HostPolicy(host)
	}
}

func (s *Core) RecordV2Topology(event *events.TopologyChangeEvent) {
	if s.privateStorage.InternalHostSource == nil || s.privateStorage.InternalCfg.Events.DisableTopologyEvents {
		return
	}
	r := s.privateStorage.InternalHostSource
	r.mu.Lock()
	r.eventEpoch++
	id := r.hostIPToUUID[event.Host.String()]
	if r.tombstones == nil {
		r.tombstones = make(map[string]bool)
	}
	if r.tombstoneAddresses == nil {
		r.tombstoneAddresses = make(map[string]string)
	}
	var removed *implEndpoint.HostInfo
	if event.Change == "REMOVED_NODE" && id != "" {
		r.tombstones[id] = true
		r.tombstoneAddresses[event.Host.String()] = id
		removed = r.InternalHosts[id]
		delete(r.InternalHosts, id)
		delete(r.hostIPToUUID, event.Host.String())
		r.bumpRevisionLocked()
	}
	if event.Change == "NEW_NODE" {
		delete(r.tombstones, id)
		delete(r.tombstones, r.tombstoneAddresses[event.Host.String()])
		delete(r.tombstoneAddresses, event.Host.String())
	}
	r.mu.Unlock()
	if removed != nil {
		if s.privateStorage.InternalPool != nil {
			s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(removed))
		}
		if s.privateStorage.InternalPolicy != nil {
			s.privateStorage.InternalPolicy.RemoveHost(removed)
		}
		if s.privateStorage.InternalMetadataDescriber != nil {
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithHost(removed)
		}
	}
	s.PublishV2Metadata()
}
func (s *Core) fenceV2Down(ip net.IP, port int) {
	if s.privateStorage.InternalHostSource == nil || s.privateStorage.InternalPool == nil {
		return
	}
	host, ok := s.privateStorage.InternalHostSource.getHostByIP(ip.String())
	if !ok {
		return
	}
	filtered := s.privateStorage.InternalCfg.FilterHost(host)
	p := s.Peripheral()
	p.hostMu.Lock()
	implEndpoint.InternalHostInfoSetState(host, implEndpoint.NodeDown)
	p.InternalMu.Lock()
	p.InternalPublicationRevision++
	p.InternalMu.Unlock()
	if !filtered {
		s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(host))
	}
	p.hostMu.Unlock()
	s.PublishV2Metadata()
}

// A notification may run user code and race a newer DOWN/removal/replacement.
// Reconcile against the current host/pool after each callback; a stale callback
// cannot leave the policy containing the superseded emitting host.
func (s *Core) synchronizeV2HostPolicy(fallback *implEndpoint.HostInfo) {
	type stamp struct {
		host       *implEndpoint.HostInfo
		pool       *pool.InternalHostConnPool
		state      implEndpoint.InternalNodeState
		generation uint64
	}
	capture := func() stamp {
		p := s.Peripheral()
		p.hostMu.Lock()
		defer p.hostMu.Unlock()
		h := s.privateStorage.InternalHostSource.InternalGetHost(fallback.HostID())
		if h == nil {
			return stamp{}
		}
		pool, _ := s.privateStorage.InternalPool.InternalGetPool(h)
		implEndpoint.HostInfoData(h).InternalMu.RLock()
		out := stamp{h, pool, implEndpoint.HostInfoData(h).InternalState.Load(), implEndpoint.HostInfoData(h).InternalLifecycleGeneration}
		implEndpoint.HostInfoData(h).InternalMu.RUnlock()
		return out
	}
	for !s.Closed() {
		before := capture()
		if before.host == nil {
			s.privateStorage.InternalPolicy.RemoveHost(fallback)
		} else if before.state == implEndpoint.NodeUp && before.pool != nil && before.pool.Size() > 0 && !s.privateStorage.InternalCfg.FilterHost(before.host) {
			s.privateStorage.InternalPolicy.HostUp(before.host)
		} else {
			s.privateStorage.InternalPolicy.HostDown(before.host)
		}
		if before == capture() {
			return
		}
	}
}
