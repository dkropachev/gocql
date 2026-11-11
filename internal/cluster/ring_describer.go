// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// Polls system.peers at a specific interval to find new hosts
type InternalRingDescriber struct {
	control        InternalControlConnection
	InternalLogger *slog.Logger
	InternalCfg    *InternalClusterSettings
	// hosts are the set of all hosts in the cassandra ring that we know of.
	// key of map is host_id.
	InternalHosts map[string]*implEndpoint.HostInfo
	// hostIPToUUID maps host native address to host_id.
	hostIPToUUID       map[string]string
	hostsView          atomic.Pointer[ringHostsView] // InternalHostsView cache
	tombstones         map[string]bool
	tombstoneAddresses map[string]string
	prevPartitioner    string
	prevHosts          []*implEndpoint.HostInfo
	diagnostics        []error
	revision           uint64
	eventEpoch         uint64
	refreshSequence    uint64
	mu                 sync.RWMutex
	readMu             sync.Mutex
}

func (r *InternalRingDescriber) setControlConn(c InternalControlConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.control = c
}

// Ask the control node for the local host info
func (r *InternalRingDescriber) getLocalHostInfo(ctx context.Context, conn transport.ConnInterface) (*implEndpoint.HostInfo, error) {
	hosts, err := readV2Topology(ctx, conn, transport.InternalQrySystemLocal, r.InternalCfg.Port)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve local host info: %w", err)
	}
	if len(hosts) != 1 || implEndpoint.InternalHostInfoHostUUID(hosts[0]).IsEmpty() || implEndpoint.InternalHostInfoInvalidConnectAddr(hosts[0]) {
		return nil, cqltype.TypeValidation("local topology", "expected one valid local identity")
	}
	return hosts[0], nil
}

func (r *InternalRingDescriber) getClusterPeerInfo(ctx context.Context, c transport.ConnInterface) ([]*implEndpoint.HostInfo, error) {
	stmt := transport.InternalQrySystemPeersCassandra
	if c.InternalGetIsSchemaV2() {
		stmt = transport.InternalQrySystemPeersV2
	} else if c.InternalIsScyllaConn() {
		stmt = transport.InternalQrySystemPeers
	}
	hosts, err := readV2Topology(ctx, c, stmt, r.InternalCfg.Port)
	if err != nil {
		return nil, err
	}
	valid := hosts[:0]
	for _, h := range hosts {
		if isValidPeer(h) {
			valid = append(valid, h)
			continue
		}
		cause := cqltype.TypeValidation("peer topology", "incomplete peer %s", h.HostID())
		r.mu.Lock()
		known := r.InternalHosts[h.HostID()] != nil
		for _, old := range r.prevHosts {
			if implEndpoint.InternalHostInfoHostUUID(old) == implEndpoint.InternalHostInfoHostUUID(h) || implEndpoint.InternalHostInfoNodeToNodeAddress(old).Equal(implEndpoint.InternalHostInfoNodeToNodeAddress(h)) {
				known = true
				break
			}
		}
		if len(r.diagnostics) < 64 {
			r.diagnostics = append(r.diagnostics, cause)
		}
		r.mu.Unlock()
		if known {
			return nil, cause
		}

	}
	return valid, nil
}

func getPeersFromQuerySystemPeers(querySystemPeerRows []map[string]any, defaultPort int, logger *slog.Logger) ([]*implEndpoint.HostInfo, error) {
	logger = logging.Or(logger)
	var peers []*implEndpoint.HostInfo

	for _, row := range querySystemPeerRows {
		// extract all available info about the peer
		host, err := hostInfoFromMap(row, defaultPort)
		if err != nil {
			return nil, err
		} else if !isValidPeer(host) {
			// If it's not a valid peer
			logger.Warn("ignoring invalid peer, likely a gossip or snitch issue", "peer", host.String())
			continue
		} else if isZeroToken(host) {
			continue
		}

		peers = append(peers, host)
	}

	return peers, nil
}

// Return true if the host is a valid peer
func isValidPeer(host *implEndpoint.HostInfo) bool {
	return !(len(host.RPCAddress()) == 0 || implEndpoint.HostInfoData(host).InternalHostId.IsEmpty() || implEndpoint.HostInfoData(host).InternalDataCenter == "" || implEndpoint.HostInfoData(host).InternalRack == "")
}

func isZeroToken(host *implEndpoint.HostInfo) bool {
	return len(implEndpoint.HostInfoData(host).InternalTokens) == 0
}

// GetHostsFromSystem returns a list of hosts found via queries to system.local and system.peers
func (r *InternalRingDescriber) GetHostsFromSystem() ([]*implEndpoint.HostInfo, string, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	r.mu.Lock()
	r.diagnostics = nil
	r.mu.Unlock()
	r.mu.RLock()
	control := r.control
	epoch := r.eventEpoch
	previous := r.prevHosts
	partitionerBefore := r.prevPartitioner
	r.mu.RUnlock()

	if control == nil {
		return previous, partitionerBefore, metadata.InternalErrNoControl
	}
	ch := control.InternalGetConn()
	if ch == nil || ch.InternalConn == nil {
		return previous, partitionerBefore, metadata.InternalErrNoControl
	}

	var (
		localHost *implEndpoint.HostInfo
		peerHosts []*implEndpoint.HostInfo
		wg        sync.WaitGroup
		errMu     sync.Mutex
		firstErr  error
	)

	ctx, cancel := context.WithCancel(metadata.InternalV2SchemaContext(control.InternalGetSession().NewSchemaSource()))
	defer cancel()

	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		var localErr error
		localHost, localErr = r.getLocalHostInfo(ctx, ch.InternalConn)
		recordErr(localErr)
	}()
	go func() {
		defer wg.Done()
		var peerErr error
		peerHosts, peerErr = r.getClusterPeerInfo(ctx, ch.InternalConn)
		recordErr(peerErr)
	}()
	wg.Wait()

	if firstErr != nil {
		return previous, partitionerBefore, firstErr
	}

	hosts := []*implEndpoint.HostInfo{localHost}
	hosts = append(hosts, peerHosts...)
	seen := make(map[cqltype.UUID]bool, len(hosts))
	for _, h := range hosts {
		if seen[implEndpoint.InternalHostInfoHostUUID(h)] {
			return previous, partitionerBefore, cqltype.TypeValidation("topology", "duplicate host identity %s", h.HostID())
		}
		seen[implEndpoint.InternalHostInfoHostUUID(h)] = true
	}

	var partitioner string
	if len(hosts) > 0 {
		partitioner = hosts[0].Partitioner()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.eventEpoch != epoch {
		return previous, partitionerBefore, cqltype.TypeValidation("topology generation", "topology changed during read")
	}
	filtered := hosts[:0]
	for _, host := range hosts {
		if !r.tombstones[host.HostID()] {
			filtered = append(filtered, host)
		}
	}
	hosts = filtered
	r.prevHosts = hosts
	r.prevPartitioner = partitioner

	return hosts, partitioner, nil
}

func (r *InternalRingDescriber) getHostByIP(ip string) (*implEndpoint.HostInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hi, ok := r.hostIPToUUID[ip]
	return r.InternalHosts[hi], ok
}

func (r *InternalRingDescriber) InternalGetHost(hostID string) *implEndpoint.HostInfo {
	r.mu.RLock()
	host := r.InternalHosts[hostID]
	r.mu.RUnlock()
	return host
}

func (r *InternalRingDescriber) InternalGetHostsList() []*implEndpoint.HostInfo {
	r.mu.RLock()
	hosts := make([]*implEndpoint.HostInfo, 0, len(r.InternalHosts))
	for _, host := range r.InternalHosts {
		hosts = append(hosts, host)
	}
	r.mu.RUnlock()
	return hosts
}

// ringHostsView is an immutable host list for one ring revision.
type ringHostsView struct {
	hosts    []*implEndpoint.HostInfo
	revision uint64
}

// InternalHostsView returns the current hosts as a shared, read-only slice.
// It is rebuilt only when the ring revision changes, so per-request callers
// allocate nothing. Callers must not modify it; InternalGetHostsList returns
// a private copy.
func (r *InternalRingDescriber) InternalHostsView() []*implEndpoint.HostInfo {
	// Every revision change clears the view under the write lock, so a
	// published view is always current.
	if v := r.hostsView.Load(); v != nil {
		return v.hosts
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v := r.hostsView.Load(); v != nil && v.revision == r.revision {
		return v.hosts
	}
	hosts := make([]*implEndpoint.HostInfo, 0, len(r.InternalHosts))
	for _, host := range r.InternalHosts {
		hosts = append(hosts, host)
	}
	r.hostsView.Store(&ringHostsView{revision: r.revision, hosts: hosts})
	return hosts
}

// bumpRevisionLocked records a change to the host set. Caller holds r.mu for
// writing.
func (r *InternalRingDescriber) bumpRevisionLocked() {
	r.revision++
	r.hostsView.Store(nil)
}

func (r *InternalRingDescriber) getHostsMap() map[string]*implEndpoint.HostInfo {
	r.mu.RLock()
	hosts := make(map[string]*implEndpoint.HostInfo, len(r.InternalHosts))
	for k, v := range r.InternalHosts {
		hosts[k] = v
	}
	r.mu.RUnlock()
	return hosts
}

func (r *InternalRingDescriber) InternalAddOrUpdate(host *implEndpoint.HostInfo) *implEndpoint.HostInfo {
	if existingHost, ok := r.addHostIfMissing(host); ok {
		implEndpoint.InternalHostInfoUpdate(existingHost, host)
		host = existingHost
	}
	return host
}

func (r *InternalRingDescriber) addHostIfMissing(host *implEndpoint.HostInfo) (*implEndpoint.HostInfo, bool) {
	if !implEndpoint.InternalValidIpAddr(host.ConnectAddress()) {
		panic(fmt.Sprintf("invalid host: %v", host))
	}
	hostID := host.HostID()

	r.mu.Lock()
	if r.InternalHosts == nil {
		r.InternalHosts = make(map[string]*implEndpoint.HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	existing, ok := r.InternalHosts[hostID]
	if !ok {
		r.bumpRevisionLocked()
		r.InternalHosts[hostID] = host
		r.hostIPToUUID[implEndpoint.InternalHostInfoNodeToNodeAddress(host).String()] = hostID
		existing = host
	}
	r.mu.Unlock()
	return existing, ok
}

func (r *InternalRingDescriber) InternalRemoveHost(hostID string) bool {
	r.mu.Lock()
	if r.InternalHosts == nil {
		r.InternalHosts = make(map[string]*implEndpoint.HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	h, ok := r.InternalHosts[hostID]
	if ok {
		delete(r.hostIPToUUID, implEndpoint.InternalHostInfoNodeToNodeAddress(h).String())
	}
	delete(r.InternalHosts, hostID)
	r.bumpRevisionLocked()
	r.mu.Unlock()
	return ok
}
