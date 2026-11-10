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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"fmt"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"

	randv2 "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const hostInfoListMapThreshold = 20

// hostInfoList is an immutable host list snapshot with optional indexed lookup.
type hostInfoList struct {
	hosts     []*implEndpoint.HostInfo
	hostsByID map[cqltype.UUID]*implEndpoint.HostInfo
	hostIDs   []cqltype.UUID
}

func newHostInfoList(hosts []*implEndpoint.HostInfo) *hostInfoList {
	hosts = hosts[:len(hosts):len(hosts)]
	l := &hostInfoList{
		hosts: hosts,
	}

	if len(hosts) >= hostInfoListMapThreshold {
		hostsByID := make(map[cqltype.UUID]*implEndpoint.HostInfo, len(hosts))
		l.hostsByID = hostsByID
		for _, host := range hosts {
			hostID := implEndpoint.InternalHostInfoHostUUID(host)
			if hostID.IsEmpty() {
				continue
			}
			if _, ok := hostsByID[hostID]; !ok {
				hostsByID[hostID] = host
			}
		}
		return l
	}

	hostIDs := make([]cqltype.UUID, len(hosts))
	for i, host := range hosts {
		hostIDs[i] = implEndpoint.InternalHostInfoHostUUID(host)
	}
	l.hostIDs = hostIDs

	return l
}

// allHosts returns hosts in this snapshot. Callers must treat the returned slice as read-only.
func (l *hostInfoList) allHosts() []*implEndpoint.HostInfo {
	if l == nil {
		return nil
	}
	return l.hosts
}

// len returns the number of hosts in this snapshot.
func (l *hostInfoList) InternalLen() int {
	if l == nil {
		return 0
	}
	return len(l.hosts)
}

// hostByID finds a host in this snapshot by host_id.
func (l *hostInfoList) hostByID(hostID cqltype.UUID) *implEndpoint.HostInfo {
	if l == nil || hostID.IsEmpty() {
		return nil
	}
	if l.hostsByID != nil {
		return l.hostsByID[hostID]
	}
	for i, id := range l.hostIDs {
		if id == hostID {
			return l.hosts[i]
		}
	}
	return nil
}

// cowHostList implements a copy-on-write host list.
type cowHostList struct {
	list atomic.Pointer[hostInfoList]
	mu   sync.Mutex
}

func (c *cowHostList) String() string {
	return fmt.Sprintf("%+v", c.get().allHosts())
}

func (c *cowHostList) get() *hostInfoList {
	return c.list.Load()
}

// add will add a host if it not already in the list
func (c *cowHostList) add(host *implEndpoint.HostInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.get().allHosts()

	if n := len(l); n == 0 {
		l = []*implEndpoint.HostInfo{host}
	} else {
		newL := make([]*implEndpoint.HostInfo, n+1)
		for i := 0; i < n; i++ {
			if host.Equal(l[i]) {
				return false
			}
			newL[i] = l[i]
		}
		newL[n] = host
		l = newL
	}

	c.list.Store(newHostInfoList(l))
	return true
}

func (c *cowHostList) addAll(hosts []*implEndpoint.HostInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.get().allHosts()
	newL := make([]*implEndpoint.HostInfo, len(l), len(l)+len(hosts))
	copy(newL, l)

	added := false
	for _, host := range hosts {
		exists := false
		for _, existing := range newL {
			if host.Equal(existing) {
				exists = true
				break
			}
		}
		if !exists {
			newL = append(newL, host)
			added = true
		}
	}

	if !added {
		return false
	}

	c.list.Store(newHostInfoList(newL))
	return true
}

func (c *cowHostList) remove(host *implEndpoint.HostInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.get().allHosts()
	size := len(l)
	if size == 0 {
		return false
	}

	found := false
	newL := make([]*implEndpoint.HostInfo, 0, size)
	for i := 0; i < len(l); i++ {
		if !l[i].Equal(host) {
			newL = append(newL, l[i])
		} else {
			found = true
		}
	}

	if !found {
		return false
	}

	c.list.Store(newHostInfoList(newL))

	return true
}

type HostStateNotifier interface {
	AddHost(host *implEndpoint.HostInfo)
	RemoveHost(host *implEndpoint.HostInfo)
	HostUp(host *implEndpoint.HostInfo)
	HostDown(host *implEndpoint.HostInfo)
}

type KeyspaceUpdateEvent struct {
	Keyspace string
	Change   string
}

type HostTierer interface {
	// HostTier returns an integer specifying how far a host is from the client.
	// Tier must start at 0.
	// The value is used to prioritize closer hosts during host selection.
	// For example this could be:
	// 0 - local rack, 1 - local DC, 2 - remote DC
	// or:
	// 0 - local DC, 1 - remote DC
	HostTier(host *implEndpoint.HostInfo) uint

	// This function returns the maximum possible host tier
	MaxHostTier() uint
}

// HostSelectionPolicy is an interface for selecting
// the most appropriate host to execute a given query.
// HostSelectionPolicy instances cannot be shared between sessions.
type HostSelectionPolicy interface {
	HostStateNotifier
	pool.SetPartitioner
	KeyspaceChanged(KeyspaceUpdateEvent)
	Init(*Core)
	// Reset is opprotunity to reset HostSelectionPolicy if Session initilization failed and we want to
	// call HostSelectionPolicy.Init() again with new Session
	Reset()
	IsLocal(host *implEndpoint.HostInfo) bool
	// Pick returns an iteration function over selected hosts.
	// Multiple attempts of a single query execution won't call the returned NextHost function concurrently,
	// so it's safe to have internal state without additional synchronization as long as every call to Pick returns
	// a different instance of NextHost.
	// IsOperational checks if host policy can properly work with given Session/Cluster/clusterSettings
	IsOperational(*Core) error
}

type selectedHost struct {
	info  *implEndpoint.HostInfo
	token implEndpoint.Token
}

func (host selectedHost) Info() *implEndpoint.HostInfo {
	return host.info
}

func (host selectedHost) Token() implEndpoint.Token {
	return host.token
}

func (host selectedHost) TokenInt64() (implEndpoint.InternalInt64Token, bool) {
	return 0, false
}

func (host selectedHost) Mark(err error) {}

// int64SelectedHost is the fast-path variant of selectedHost: it carries the
// routing token as a raw int64 instead of a boxed Token, so shard-aware conn
// picking consumes it without an interface allocation.
type int64SelectedHost struct {
	info        *implEndpoint.HostInfo
	tokenCasted implEndpoint.InternalInt64Token
}

func (host int64SelectedHost) Info() *implEndpoint.HostInfo {
	return host.info
}

func (host int64SelectedHost) Token() implEndpoint.Token {
	return host.tokenCasted
}

func (host int64SelectedHost) TokenInt64() (implEndpoint.InternalInt64Token, bool) {
	return host.tokenCasted, true
}

func (host int64SelectedHost) Mark(err error) {}

func newSingleHost(info *implEndpoint.HostInfo, maxRetries byte, retryDelay time.Duration) *singleHost {
	return &singleHost{info: info, maxRetries: maxRetries, delay: retryDelay}
}

type singleHost struct {
	info       *implEndpoint.HostInfo
	delay      time.Duration
	retry      byte
	maxRetries byte
}

func (s *singleHost) selectHost() implEndpoint.SelectedHost {
	if s.retry >= s.maxRetries {
		return nil
	}
	if s.retry > 0 && s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.retry++
	return s
}

func (s singleHost) Info() *implEndpoint.HostInfo { return s.info }

func (s singleHost) Token() implEndpoint.Token { return nil }

func (s singleHost) Mark(error) {}

// NextHost is an iteration function over picked hosts
type NextHost func() implEndpoint.SelectedHost

// RoundRobinHostPolicy is a round-robin load balancing policy, where each host
// is tried sequentially for each query.
func RoundRobinHostPolicy() HostSelectionPolicy {
	return &InternalRoundRobinHostPolicy{}
}

type InternalRoundRobinHostPolicy struct {
	hosts           cowHostList
	lastUsedHostIdx uint64
}

func (r *InternalRoundRobinHostPolicy) IsLocal(*implEndpoint.HostInfo) bool { return true }
func (r *InternalRoundRobinHostPolicy) KeyspaceChanged(KeyspaceUpdateEvent) {}
func (r *InternalRoundRobinHostPolicy) SetPartitioner(partitioner string)   {}
func (r *InternalRoundRobinHostPolicy) Init(*Core)                          {}
func (r *InternalRoundRobinHostPolicy) Reset()                              {}
func (r *InternalRoundRobinHostPolicy) IsOperational(*Core) error           { return nil }

func (r *InternalRoundRobinHostPolicy) AddHost(host *implEndpoint.HostInfo) {
	r.hosts.add(host)
}

func (r *InternalRoundRobinHostPolicy) RemoveHost(host *implEndpoint.HostInfo) {
	r.hosts.remove(host)
}

func (r *InternalRoundRobinHostPolicy) HostUp(host *implEndpoint.HostInfo) {
	r.AddHost(host)
}

func (r *InternalRoundRobinHostPolicy) HostDown(host *implEndpoint.HostInfo) {
	r.RemoveHost(host)
}

func ShuffleReplicas() func(*tokenAwareHostPolicy) {
	return func(t *tokenAwareHostPolicy) {
		t.privateStorage.shuffleReplicas = true
	}
}

func DontShuffleReplicas() func(*tokenAwareHostPolicy) {
	return func(t *tokenAwareHostPolicy) {
		t.privateStorage.shuffleReplicas = false
	}
}

// AvoidSlowReplicas enabled avoiding slow replicas
//
// TokenAwareHostPolicy normally does not check how busy replica is, with avoidSlowReplicas enabled it avoids replicas
// if they have equal or more than MAX_IN_FLIGHT_THRESHOLD requests in flight
func AvoidSlowReplicas(max_in_flight_threshold int) func(policy *tokenAwareHostPolicy) {
	return func(t *tokenAwareHostPolicy) {
		t.privateStorage.avoidSlowReplicas = true
		MAX_IN_FLIGHT_THRESHOLD = max_in_flight_threshold
	}
}

// NonLocalReplicasFallback enables fallback to replicas that are not considered local.
//
// TokenAwareHostPolicy used with DCAwareHostPolicy fallback first selects replicas by partition key in local DC, then
// falls back to other nodes in the local DC. Enabling NonLocalReplicasFallback causes TokenAwareHostPolicy
// to first select replicas by partition key in local DC, then replicas by partition key in remote DCs and fall back
// to other nodes in local DC.
func NonLocalReplicasFallback() func(policy *tokenAwareHostPolicy) {
	return func(t *tokenAwareHostPolicy) {
		t.privateStorage.nonLocalReplicasFallback = true
	}
}

// TokenAwareHostPolicy is a token aware host selection policy, where hosts are
// selected based on the partition key, so queries are sent to the host which
// owns the partition. Fallback is used when routing information is not available.
func TokenAwareHostPolicy(fallback HostSelectionPolicy, opts ...func(*tokenAwareHostPolicy)) HostSelectionPolicy {
	p := func() *tokenAwareHostPolicy {
		opaqueStorageValue := new(tokenAwareHostPolicy)
		opaqueStorageValue.privateStorage.fallback = fallback
		opaqueStorageValue.privateStorage.shuffleReplicas = true
		return opaqueStorageValue
	}()

	for _, opt := range opts {
		opt(p)
	}
	return p
}

// clusterMeta holds metadata about cluster topology.
// It is used inside atomic.Pointer and shallow copies are used when replacing it,
// so fields should not be modified in-place. Instead, to modify a field a copy of the field should be made
// and the pointer in clusterMeta updated to point to the new value.
type clusterMeta struct {
	// replicas is map[keyspace]map[token]hosts
	replicas  map[string]tokenRingReplicas
	tokenRing *implEndpoint.InternalTokenRing
}

var MAX_IN_FLIGHT_THRESHOLD int = 10

type tokenAwareHostPolicy struct {
	privateStorage tokenAwareHostPolicyStorage

	// atomic store for *clusterMeta

	// mu protects writes to hosts, partitioner, metadata.
	// reads can be unlocked as long as they are not used for updating state later.

}

func (t *tokenAwareHostPolicy) Init(s *Core) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	if t.privateStorage.getKeyspaceMetadata != nil {
		// Init was already called.
		// See https://github.com/scylladb/gocql/issues/94.
		panic("sharing token aware host selection policy between sessions is not supported")
	}
	t.privateStorage.getKeyspaceMetadata = func(keyspace string) (*metadata.KeyspaceMetadata, error) {
		if keyspace == "" {
			return nil, runtimeerrors.ErrNoKeyspace
		}
		return s.privateStorage.InternalMetadataDescriber.GetKeyspace(keyspace)
	}
	t.privateStorage.getKeyspaceName = func() string { return s.privateStorage.InternalCfg.Keyspace }
	t.privateStorage.logger = s.privateStorage.InternalLogger
}

func (t *tokenAwareHostPolicy) Reset() {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()

	// Sharing token aware host selection policy between sessions is not supported
	// but session initialization can failed for some reasons. So in our application
	// may be we want to create new session again.
	// Reset method should be called in Session.Close method
	t.privateStorage.getKeyspaceMetadata = nil
	t.privateStorage.getKeyspaceName = nil
	t.privateStorage.logger = nil
}

func (t *tokenAwareHostPolicy) IsOperational(session *Core) error {
	return t.privateStorage.fallback.IsOperational(session)
}

func (t *tokenAwareHostPolicy) IsLocal(host *implEndpoint.HostInfo) bool {
	return t.privateStorage.fallback.IsLocal(host)
}

func (t *tokenAwareHostPolicy) KeyspaceChanged(update KeyspaceUpdateEvent) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	meta := t.getMetadataForUpdate()
	t.updateReplicas(meta, update.Keyspace)
	t.privateStorage.metadata.Store(meta)
}

// updateReplicas updates replicas in clusterMeta.
// It must be called with t.mu mutex locked.
// meta must not be nil and it's replicas field will be updated.
func (t *tokenAwareHostPolicy) updateReplicas(meta *clusterMeta, keyspace string) {
	if keyspace == "" || meta == nil || meta.tokenRing == nil {
		return
	}
	newReplicas := make(map[string]tokenRingReplicas, len(meta.replicas))

	ks, err := t.privateStorage.getKeyspaceMetadata(keyspace)
	if err == nil {
		strat := getStrategy(ks, t.privateStorage.logger)
		if strat != nil {
			newReplicas[keyspace] = strat.InternalReplicaMap(meta.tokenRing)
		}
	}

	for ks, replicas := range meta.replicas {
		if ks != keyspace {
			newReplicas[ks] = replicas
		}
	}

	meta.replicas = newReplicas
}

func (t *tokenAwareHostPolicy) SetPartitioner(partitioner string) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()

	if t.privateStorage.partitioner != partitioner {
		t.privateStorage.fallback.SetPartitioner(partitioner)
		t.privateStorage.partitioner = partitioner
		meta := t.getMetadataForUpdate()
		meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.get().allHosts(), t.privateStorage.logger)
		t.updateReplicas(meta, t.privateStorage.getKeyspaceName())
		t.privateStorage.metadata.Store(meta)
	}
}

func (t *tokenAwareHostPolicy) AddHost(host *implEndpoint.HostInfo) {
	t.privateStorage.mu.Lock()
	if t.privateStorage.hosts.add(host) {
		meta := t.getMetadataForUpdate()
		meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.get().allHosts(), t.privateStorage.logger)
		t.updateReplicas(meta, t.privateStorage.getKeyspaceName())
		t.privateStorage.metadata.Store(meta)
	}
	t.privateStorage.mu.Unlock()
	t.privateStorage.fallback.AddHost(host)
}

func (t *tokenAwareHostPolicy) AddHosts(hosts []*implEndpoint.HostInfo) {
	t.privateStorage.mu.Lock()
	t.privateStorage.hosts.addAll(hosts)

	meta := t.getMetadataForUpdate()
	meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.get().allHosts(), t.privateStorage.logger)
	t.updateReplicas(meta, t.privateStorage.getKeyspaceName())
	t.privateStorage.metadata.Store(meta)
	t.privateStorage.mu.Unlock()

	for _, host := range hosts {
		t.privateStorage.fallback.AddHost(host)
	}
}

func (t *tokenAwareHostPolicy) RemoveHost(host *implEndpoint.HostInfo) {
	t.privateStorage.mu.Lock()
	if t.privateStorage.hosts.remove(host) {
		meta := t.getMetadataForUpdate()
		meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.get().allHosts(), t.privateStorage.logger)
		t.updateReplicas(meta, t.privateStorage.getKeyspaceName())
		t.privateStorage.metadata.Store(meta)
	}
	t.privateStorage.mu.Unlock()
	t.privateStorage.fallback.RemoveHost(host)
}

func (t *tokenAwareHostPolicy) HostUp(host *implEndpoint.HostInfo) {
	t.privateStorage.fallback.HostUp(host)
}

func (t *tokenAwareHostPolicy) HostDown(host *implEndpoint.HostInfo) {
	t.privateStorage.fallback.HostDown(host)
}

// getMetadataReadOnly returns current cluster metadata.
// Metadata uses copy on write, so the returned value should be only used for reading.
// To obtain a copy that could be updated, use getMetadataForUpdate instead.
func (t *tokenAwareHostPolicy) getMetadataReadOnly() *clusterMeta {
	return t.privateStorage.metadata.Load()
}

// getMetadataForUpdate returns clusterMeta suitable for updating.
// It is a SHALLOW copy of current metadata in case it was already set or new empty clusterMeta otherwise.
// This function should be called with t.mu mutex locked and the mutex should not be released before
// storing the new metadata.
func (t *tokenAwareHostPolicy) getMetadataForUpdate() *clusterMeta {
	metaReadOnly := t.getMetadataReadOnly()
	meta := new(clusterMeta)
	if metaReadOnly != nil {
		*meta = *metaReadOnly
	}
	return meta
}

// resetTokenRing creates a new tokenRing.
// It must be called with t.mu locked.
func (m *clusterMeta) resetTokenRing(partitioner string, hosts []*implEndpoint.HostInfo, logger logging.StdLogger) {
	if partitioner == "" {
		// partitioner not yet set
		return
	}

	// create a new token ring
	tokenRing, err := implEndpoint.InternalNewTokenRing(partitioner, hosts)
	if err != nil {
		logger.Printf("Unable to update the token ring due to error: %s", err)
		return
	}

	// replace the token ring
	m.tokenRing = tokenRing
}

// hostSet is a small set optimized for tracking hosts returned by the
// token-aware iterator. Uses an inline array for RF <= 9 (3 DCs × RF=3),
// spilling to a map for larger replica sets.
type hostSet struct {
	overflow map[*implEndpoint.HostInfo]struct{}
	arr      [9]*implEndpoint.HostInfo
	n        int
}

func (s *hostSet) add(h *implEndpoint.HostInfo) {
	if s.n < len(s.arr) {
		s.arr[s.n] = h
		s.n++
		return
	}
	if s.overflow == nil {
		s.overflow = make(map[*implEndpoint.HostInfo]struct{})
		for i := range s.n {
			s.overflow[s.arr[i]] = struct{}{}
		}
	}
	s.overflow[h] = struct{}{}
}

func (s *hostSet) contains(h *implEndpoint.HostInfo) bool {
	if s.overflow != nil {
		_, ok := s.overflow[h]
		return ok
	}
	for i := range s.n {
		if s.arr[i] == h {
			return true
		}
	}
	return false
}

// shuffleHostsInPlace shuffles the given slice in-place using math/rand/v2.
func shuffleHostsInPlace(hosts []*implEndpoint.HostInfo) {
	randv2.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
}

// partitionHealthy performs an in-place stable partition of replicas, moving
// healthy (non-busy) hosts to the front while preserving relative order.
func partitionHealthy(replicas []*implEndpoint.HostInfo, s *Core) {
	n := len(replicas)
	if n <= 1 {
		return
	}

	// Snapshot IsBusy to avoid TOCTOU races between counting and placement.
	var busyBuf [9]bool
	var busy []bool
	if n <= len(busyBuf) {
		busy = busyBuf[:n]
	} else {
		busy = make([]bool, n)
	}

	healthyCount := 0
	for i, h := range replicas {
		busy[i] = hostBusy(s, h)
		if !busy[i] {
			healthyCount++
		}
	}

	if healthyCount == 0 || healthyCount == n {
		return // all same category, nothing to do
	}

	var buf [9]*implEndpoint.HostInfo
	var tmp []*implEndpoint.HostInfo
	if n <= len(buf) {
		tmp = buf[:n]
	} else {
		tmp = make([]*implEndpoint.HostInfo, n)
	}
	copy(tmp, replicas)

	hi, ui := 0, healthyCount
	for i, h := range tmp {
		if !busy[i] {
			replicas[hi] = h
			hi++
		} else {
			replicas[ui] = h
			ui++
		}
	}
}

// Fast path: use the raw int64 hash when available, avoiding a Token-boxing
// allocation. Falls back to Hash() otherwise (see int64Hasher in token.go).

// Presized to the known upper bound.

// Zero-copy: replicas must not be mutated below unless needsMutation is true.

// Rare fallback (no tablet/replica-map match): GetHostForToken needs a
// boxed Token, so box only here instead of on every query.

// Cache IsLWT() once: it is read on both the shuffle and avoid-slow-replicas
// paths below, and computing it can take a lock on the query routing info.

// fallback

// filter the token aware selected hosts from the fallback hosts

type dcAwareRR struct {
	local             string
	localHosts        cowHostList
	remoteHosts       cowHostList
	lastUsedHostIdx   uint64
	disableDCFailover bool
}

type dcFailoverDisabledPolicy interface {
	setDCFailoverDisabled()
}

type dcAwarePolicyOption func(p dcFailoverDisabledPolicy)

func HostPolicyOptionDisableDCFailover(p dcFailoverDisabledPolicy) {
	p.setDCFailoverDisabled()
}

// DCAwareRoundRobinPolicy is a host selection policies which will prioritize and
// return hosts which are in the local datacentre before returning hosts in all
// other datercentres
func DCAwareRoundRobinPolicy(localDC string, opts ...dcAwarePolicyOption) HostSelectionPolicy {
	p := &dcAwareRR{local: localDC, disableDCFailover: false}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (d *dcAwareRR) setDCFailoverDisabled() {
	d.disableDCFailover = true
}

// dcFailoverDisabled reports whether this policy was constructed with
// HostPolicyOptionDisableDCFailover. Used by driver_config.go to report
// query.load-balancing.policy.fallback-to-non-preferred-nodes.
func (d *dcAwareRR) dcFailoverDisabled() bool {
	return d.disableDCFailover
}

// localDatacenter reports the datacenter this policy prioritizes. Used by
// driver_config.go to report query.load-balancing.node-preference.
func (d *dcAwareRR) localDatacenter() string {
	return d.local
}

func (d *dcAwareRR) Init(*Core)                          {}
func (d *dcAwareRR) Reset()                              {}
func (d *dcAwareRR) KeyspaceChanged(KeyspaceUpdateEvent) {}
func (d *dcAwareRR) SetPartitioner(p string)             {}

func (d *dcAwareRR) IsOperational(session *Core) error {
	if session.privateStorage.InternalCfg.privateStorage.disableInit || session.privateStorage.InternalCfg.privateStorage.InternalDisableControlConn {
		return nil
	}

	hosts := session.privateStorage.InternalHostSource.InternalGetHostsList()
	for _, host := range hosts {
		if !session.privateStorage.InternalCfg.FilterHost(host) && host.DataCenter() == d.local {
			// Policy can work properly only if there is at least one host from target DC
			// No need to check host status, since it could be down due to the outage
			// We only need to make sure that policy is not misconfigured with wrong DC
			return nil
		}
	}

	return fmt.Errorf("gocql: datacenter %s in the policy was not found in the topology - probable DC aware policy misconfiguration", d.local)
}

func (d *dcAwareRR) IsLocal(host *implEndpoint.HostInfo) bool {
	return host.DataCenter() == d.local
}

func (d *dcAwareRR) AddHost(host *implEndpoint.HostInfo) {
	if d.IsLocal(host) {
		d.localHosts.add(host)
	} else {
		d.remoteHosts.add(host)
	}
}

func (d *dcAwareRR) RemoveHost(host *implEndpoint.HostInfo) {
	if d.IsLocal(host) {
		d.localHosts.remove(host)
	} else {
		d.remoteHosts.remove(host)
	}
}

func (d *dcAwareRR) HostUp(host *implEndpoint.HostInfo)   { d.AddHost(host) }
func (d *dcAwareRR) HostDown(host *implEndpoint.HostInfo) { d.RemoveHost(host) }

// This function is supposed to be called in a fashion
// roundRobbin(offset, hostsPriority1, hostsPriority2, hostsPriority3 ... )
//
// E.g. for DC-naive strategy:
// roundRobbin(offset, allHosts)
//
// For tiered and DC-aware strategy:
// roundRobbin(offset, localHosts, remoteHosts)
func roundRobbin(shift int, hosts ...[]*implEndpoint.HostInfo) NextHost {
	currentLayer := 0
	currentlyObserved := 0

	return func() implEndpoint.SelectedHost {
		// iterate over layers
		for {
			if currentLayer == len(hosts) {
				return nil
			}

			currentLayerSize := len(hosts[currentLayer])

			// iterate over hosts within a layer
			for {
				currentlyObserved++
				if currentlyObserved > currentLayerSize {
					currentLayer++
					currentlyObserved = 0
					break
				}

				h := hosts[currentLayer][(shift+currentlyObserved)%currentLayerSize]

				if h.IsUp() {
					return selectedHost{info: h}
				}

			}
		}
	}
}

// RackAwareRoundRobinPolicy is a host selection policies which will prioritize and
// return hosts which are in the local rack, before hosts in the local datacenter but
// a different rack, before hosts in all other datacenters

type rackAwareRR struct {
	localDC   string
	localRack string
	hosts     []cowHostList
	// lastUsedHostIdx keeps the index of the last used host.
	// It is accessed atomically and needs to be aligned to 64 bits, so we
	// keep it first in the struct. Do not move it or add new struct members
	// before it.
	lastUsedHostIdx   uint64
	disableDCFailover bool
}

func RackAwareRoundRobinPolicy(localDC string, localRack string, opts ...dcAwarePolicyOption) HostSelectionPolicy {
	p := &rackAwareRR{localDC: localDC, localRack: localRack, hosts: make([]cowHostList, 3), disableDCFailover: false}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (d *rackAwareRR) Init(*Core)                          {}
func (d *rackAwareRR) Reset()                              {}
func (d *rackAwareRR) KeyspaceChanged(KeyspaceUpdateEvent) {}
func (d *rackAwareRR) SetPartitioner(p string)             {}

func (d *rackAwareRR) IsOperational(session *Core) error {
	if session.privateStorage.InternalCfg.privateStorage.disableInit || session.privateStorage.InternalCfg.privateStorage.InternalDisableControlConn {
		return nil
	}
	hosts := session.privateStorage.InternalHostSource.InternalGetHostsList()
	for _, host := range hosts {
		if !session.privateStorage.InternalCfg.FilterHost(host) && host.DataCenter() == d.localDC && host.Rack() == d.localRack {
			// Policy can work properly only if there is at least one host from target DC+Rack
			// No need to check host status, since it could be down due to the outage
			// We only need to make sure that policy is not misconfigured with wrong DC+Rack
			return nil
		}
	}
	return fmt.Errorf("gocql: rack %s/%s was not found in the topology - probable Rack aware policy misconfiguration", d.localDC, d.localRack)
}

func (d *rackAwareRR) MaxHostTier() uint {
	return 2
}

func (d *rackAwareRR) setDCFailoverDisabled() {
	d.disableDCFailover = true
}

// dcFailoverDisabled reports whether this policy was constructed with
// HostPolicyOptionDisableDCFailover. Used by driver_config.go to report
// query.load-balancing.policy.fallback-to-non-preferred-nodes.
func (d *rackAwareRR) dcFailoverDisabled() bool {
	return d.disableDCFailover
}

// localDatacenter and localRackName report the datacenter/rack this policy
// prioritizes. Used by driver_config.go to report
// query.load-balancing.node-preference.
func (d *rackAwareRR) localDatacenter() string {
	return d.localDC
}

func (d *rackAwareRR) localRackName() string {
	return d.localRack
}

func (d *rackAwareRR) HostTier(host *implEndpoint.HostInfo) uint {
	if host.DataCenter() == d.localDC {
		if host.Rack() == d.localRack {
			return 0
		} else {
			return 1
		}
	} else {
		return 2
	}
}

func (d *rackAwareRR) IsLocal(host *implEndpoint.HostInfo) bool {
	return d.HostTier(host) == 0
}

func (d *rackAwareRR) AddHost(host *implEndpoint.HostInfo) {
	dist := d.HostTier(host)
	d.hosts[dist].add(host)
}

func (d *rackAwareRR) RemoveHost(host *implEndpoint.HostInfo) {
	dist := d.HostTier(host)
	d.hosts[dist].remove(host)
}

func (d *rackAwareRR) HostUp(host *implEndpoint.HostInfo)   { d.AddHost(host) }
func (d *rackAwareRR) HostDown(host *implEndpoint.HostInfo) { d.RemoveHost(host) }

// ReadyPolicy defines a policy for when a HostSelectionPolicy can be used. After
// each host connects during session initialization, the Ready method will be
// called. If you only need a single Host to be up you can wrap a
// HostSelectionPolicy policy with SingleHostReadyPolicy.
type ReadyPolicy interface {
	Ready() bool
}

// SingleHostReadyPolicy wraps a HostSelectionPolicy and returns Ready after a
// single host has been added via HostUp
func SingleHostReadyPolicy(p HostSelectionPolicy) *singleHostReadyPolicy {
	return &singleHostReadyPolicy{
		HostSelectionPolicy: p,
	}
}

type singleHostReadyPolicy struct {
	HostSelectionPolicy
	ready    bool
	readyMux sync.Mutex
}

func (s *singleHostReadyPolicy) HostUp(host *implEndpoint.HostInfo) {
	s.HostSelectionPolicy.HostUp(host)

	s.readyMux.Lock()
	s.ready = true
	s.readyMux.Unlock()
}

func (s *singleHostReadyPolicy) Ready() bool {
	s.readyMux.Lock()
	ready := s.ready
	s.readyMux.Unlock()
	if !ready {
		return false
	}

	// in case the wrapped policy is also a ReadyPolicy, defer to that
	if rdy, ok := s.HostSelectionPolicy.(ReadyPolicy); ok {
		return rdy.Ready()
	}
	return true
}

func hostBusy(s *Core, h *implEndpoint.HostInfo) bool {
	pool, ok := s.privateStorage.InternalPool.InternalGetPool(h)
	return ok && h != nil && pool.InFlight() >= MAX_IN_FLIGHT_THRESHOLD
}
