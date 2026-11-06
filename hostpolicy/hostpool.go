/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package hostpolicy provides an optional health-weighted routing policy.
package hostpolicy

import (
	"fmt"
	"sync"

	"github.com/hailocab/go-hostpool"

	"github.com/scylladb/gocql/v2"
)

// HostPool adapts go-hostpool to the typed routing and attempt-observation API.
// Each instance belongs to one session. Metadata generations update membership;
// attempts report their outcome through RoutingPolicy.Observe.
func HostPool(hp hostpool.HostPool) gocql.RoutingPolicy {
	return &hostPoolHostPolicy{hp: hp}
}

type hostPoolHostPolicy struct {
	hp          hostpool.HostPool
	byAddress   map[string]gocql.HostSnapshot
	routes      map[gocql.UUID]gocql.Route
	responses   map[gocql.UUID]hostpool.HostPoolResponse
	hosts       []gocql.HostSnapshot
	generation  uint64
	mu          sync.Mutex
	initialized bool
}

func (p *hostPoolHostPolicy) Validate(_ *gocql.MetadataSnapshot) error {
	if p.hp == nil {
		return &gocql.ValidationError{Field: "hostpool", Err: fmt.Errorf("nil host pool")}
	}
	return nil
}

// setHosts takes hosts and their routes, in the same order.
func (p *hostPoolHostPolicy) setHosts(hosts []gocql.HostSnapshot, routes []gocql.Route) {
	addresses := make([]string, 0, len(hosts))
	p.hosts = nil
	p.byAddress = make(map[string]gocql.HostSnapshot, len(hosts))
	p.routes = make(map[gocql.UUID]gocql.Route, len(hosts))
	p.responses = make(map[gocql.UUID]hostpool.HostPoolResponse, len(hosts))
	for i, h := range hosts {
		if h.Enabled && h.Up {
			p.routes[h.ID] = routes[i]
			address := h.Address.String()
			addresses = append(addresses, address)
			p.byAddress[address] = h
			p.hosts = append(p.hosts, h)
		}
	}
	p.hp.SetHosts(addresses)
}

// refresh takes m's hosts when they changed. p.mu must be held.
func (p *hostPoolHostPolicy) refresh(m *gocql.MetadataSnapshot) {
	if m != nil && (!p.initialized || p.generation != m.Generation()) {
		p.setHosts(m.Hosts(), m.HostRoutes())
		p.generation = m.Generation()
		p.initialized = true
	}
}

// Pick routes to the host go-hostpool chooses, when the request may go there.
func (p *hostPoolHostPolicy) Pick(info gocql.RoutingInfo, m *gocql.MetadataSnapshot) (gocql.Route, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refresh(m)
	if len(p.hosts) == 0 {
		return gocql.Route{}, false
	}
	response := p.hp.Get()
	h, ok := p.byAddress[response.Host()]
	if !ok || info.HostID.Set && info.HostID.Value != h.ID {
		return gocql.Route{}, false
	}
	p.responses[h.ID] = response
	r := p.routes[h.ID]
	if info.ShardID.Set {
		r = r.WithShard(int32(info.ShardID.Value))
	}
	return r, true
}

// Fallback returns go-hostpool's choices, then every other host in order.
func (p *hostPoolHostPolicy) Fallback(info gocql.RoutingInfo, m *gocql.MetadataSnapshot) gocql.QueryPlan {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refresh(m)
	return &hostPoolPlan{policy: p, hosts: p.hosts, byAddress: p.byAddress, routes: p.routes, info: info, seen: make(map[gocql.UUID]bool)}
}
func (p *hostPoolHostPolicy) Observe(event gocql.AttemptEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r := p.responses[event.Target.HostID]; r != nil && !event.Cancelled {
		r.Mark(event.Err)
	}
}

type hostPoolPlan struct {
	policy    *hostPoolHostPolicy
	byAddress map[string]gocql.HostSnapshot
	routes    map[gocql.UUID]gocql.Route
	seen      map[gocql.UUID]bool
	hosts     []gocql.HostSnapshot
	info      gocql.RoutingInfo
	fallback  int
}

func (p *hostPoolPlan) Next() (gocql.Route, bool) {
	policy := p.policy
	policy.mu.Lock()
	defer policy.mu.Unlock()
	emit := func(h gocql.HostSnapshot) (gocql.Route, bool) {
		if p.seen[h.ID] || p.info.HostID.Set && p.info.HostID.Value != h.ID {
			return gocql.Route{}, false
		}
		p.seen[h.ID] = true
		r := p.routes[h.ID]
		if p.info.ShardID.Set {
			return r.WithShard(int32(p.info.ShardID.Value)), true
		}
		return r, true
	}
	for n := 0; n < 2*len(p.hosts); n++ {
		r := policy.hp.Get()
		h, ok := p.byAddress[r.Host()]
		if !ok {
			continue
		}
		if t, ok := emit(h); ok {
			policy.responses[h.ID] = r
			return t, true
		}
	}
	for p.fallback < len(p.hosts) {
		h := p.hosts[p.fallback]
		p.fallback++
		if t, ok := emit(h); ok {
			return t, true
		}
	}
	return gocql.Route{}, false
}
