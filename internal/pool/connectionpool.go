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

package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/tablets"
	"github.com/scylladb/gocql/v2/internal/transport"

	"github.com/scylladb/gocql/v2/internal/debounce"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	// interface to implement to receive the host information
)

type SetHosts interface {
	SetHosts(hosts []*implEndpoint.HostInfo)
}

// interface to implement to receive the partitioner value
type SetPartitioner interface {
	SetPartitioner(partitioner string)
}

// interface to implement to receive the tablets value
type SetTablets interface {
	SetTablets(tablets tablets.TabletInfoList)
}

type InternalPolicyConnPool struct {
	InternalServices      InternalPoolServices
	InternalHostConnPools map[cqltype.UUID]*InternalHostConnPool
	routes                map[cqltype.UUID]*HostRoute
	InternalKeyspace      string
	InternalPort          int
	InternalNumConns      int
	InternalMu            sync.RWMutex
}

func (p *InternalPolicyConnPool) SetHosts(hosts []*implEndpoint.HostInfo) {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()

	toRemove := make(map[cqltype.UUID]struct{})
	for hostID := range p.InternalHostConnPools {
		toRemove[hostID] = struct{}{}
	}

	pools := make(chan *InternalHostConnPool)
	createCount := 0
	for _, host := range hosts {
		if !host.IsUp() {
			// don't create a connection pool for a down host
			continue
		}
		hostID := implEndpoint.InternalHostInfoHostUUID(host)
		if _, exists := p.InternalHostConnPools[hostID]; exists {
			// still have this host, so don't remove it
			delete(toRemove, hostID)
			continue
		}

		createCount++
		go func(host *implEndpoint.HostInfo) {
			// create a connection pool for the host
			pools <- p.InternalServices.NewHostPool(
				host,
				p.InternalNumConns,
				p.InternalKeyspace,
			)
		}(host)
	}

	// add created pools
	for createCount > 0 {
		pool := <-pools
		createCount--
		if pool.Size() > 0 {
			// add pool only if there a connections available
			p.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(pool.InternalHost)] = pool
			p.routeChangedLocked(implEndpoint.InternalHostInfoHostUUID(pool.InternalHost), pool)
		}
	}

	for addr := range toRemove {
		pool := p.InternalHostConnPools[addr]
		delete(p.InternalHostConnPools, addr)
		p.routeChangedLocked(addr, nil)
		go pool.Close()
	}
}

func (p *InternalPolicyConnPool) InFlight() int {
	p.InternalMu.RLock()
	count := 0
	for _, pool := range p.InternalHostConnPools {
		count += pool.InFlight()
	}
	p.InternalMu.RUnlock()

	return count
}

func (p *InternalPolicyConnPool) Size() int {
	p.InternalMu.RLock()
	count := 0
	for _, pool := range p.InternalHostConnPools {
		count += pool.Size()
	}
	p.InternalMu.RUnlock()

	return count
}

func (p *InternalPolicyConnPool) InternalGetPool(host *implEndpoint.HostInfo) (pool *InternalHostConnPool, ok bool) {
	hostID := implEndpoint.InternalHostInfoHostUUID(host)
	p.InternalMu.RLock()
	pool, ok = p.InternalHostConnPools[hostID]
	p.InternalMu.RUnlock()
	return
}

// Rare path (SetHostID/GetHostPoolByID); getPool is the hot path and skips this parse.
func (p *InternalPolicyConnPool) InternalGetPoolByHostID(hostID string) (pool *InternalHostConnPool, ok bool) {
	id, err := cqltype.ParseUUID(hostID)
	if err != nil {
		return nil, false
	}
	p.InternalMu.RLock()
	pool, ok = p.InternalHostConnPools[id]
	p.InternalMu.RUnlock()
	return
}

// iteratePool runs iter for each host pool, stopping early if it returns false.
//
// The unlock is deferred because iter is caller-supplied: a panic, or a
// runtime.Goexit from something like t.Fatal, would otherwise skip a bare
// RUnlock and leave the read lock held forever, wedging every later addHost,
// removeHost and Close on the pool. The other bare RUnlocks in this file only
// guard reads of internal state and cannot panic.
func (p *InternalPolicyConnPool) InternalIteratePool(iter func(info HostPoolInfo) bool) {
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()

	for _, pool := range p.InternalHostConnPools {
		if !iter(pool) {
			break
		}
	}
}

func (p *InternalPolicyConnPool) Close() {
	p.InternalMu.Lock()
	pools := make([]*InternalHostConnPool, 0, len(p.InternalHostConnPools))
	for id, pool := range p.InternalHostConnPools {
		pools = append(pools, pool)
		delete(p.InternalHostConnPools, id)
		p.routeChangedLocked(id, nil)
	}
	p.InternalMu.Unlock()
	for _, pool := range pools {
		pool.Close()
	}
}

func (p *InternalPolicyConnPool) InternalEnsureHost(host *implEndpoint.HostInfo) *InternalHostConnPool {
	hostID := implEndpoint.InternalHostInfoHostUUID(host)
	p.InternalMu.Lock()
	pool, ok := p.InternalHostConnPools[hostID]
	if !ok {
		pool = p.InternalServices.NewHostPool(
			host,
			p.InternalNumConns,
			p.InternalKeyspace,
		)

		p.InternalHostConnPools[hostID] = pool
		p.routeChangedLocked(hostID, pool)
	}
	p.InternalMu.Unlock()

	return pool
}
func (p *InternalPolicyConnPool) InternalAddHost(host *implEndpoint.HostInfo) {
	p.InternalEnsureHost(host).InternalFill_debounce()
}

func (p *InternalPolicyConnPool) InternalRemoveHost(hostID cqltype.UUID) {
	p.InternalMu.Lock()
	pool, ok := p.InternalHostConnPools[hostID]
	if !ok {
		p.InternalMu.Unlock()
		return
	}

	delete(p.InternalHostConnPools, hostID)
	p.routeChangedLocked(hostID, nil)
	p.InternalMu.Unlock()

	go pool.Close()
}

// hostConnPool is a connection pool for a single host.
// Connection selection is based on a provided ConnSelectionPolicy
func (pool *InternalHostConnPool) logger() *slog.Logger { return logging.Or(pool.InternalLogger) }

type InternalHostConnPool struct {
	InternalServices   InternalPoolServices
	InternalConnPicker ConnPicker
	typed              atomic.Pointer[InternalTypedConnPicker]
	InternalLogger     *slog.Logger
	InternalRepairCtx  context.Context
	InternalHost       *implEndpoint.HostInfo
	InternalDebouncer  *debounce.SimpleDebouncer
	reservationChanged chan struct{}
	// failedShards holds the shards whose failed dials the conviction
	// policy saw and that have not connected since; guarded by InternalMu.
	failedShards         map[uint32]struct{}
	InternalRepairCancel context.CancelFunc
	InternalKeyspace     string
	InternalSize         int
	generation           uint64
	reconnectAttempt     int
	legacy               atomic.Bool // the installed picker is not typed
	// protection for connPicker, closed, filling
	InternalMu                            sync.RWMutex
	InternalClosed                        bool
	InternalFilling                       bool
	InternalState                         poolState
	InternalStarted                       bool
	InternalRepairScheduled, retryStopped bool
	// shardConvicted reports that the conviction policy convicted the host
	// on a shard failure; fillingStopped marks the host down.
	shardConvicted bool
}

func (pool *InternalHostConnPool) String() string {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()
	size, _ := pool.InternalConnPicker.Size()
	return fmt.Sprintf("[filling=%v closed=%v conns=%v size=%v host=%v]",
		pool.InternalFilling, pool.InternalClosed, size, pool.InternalSize, pool.InternalHost)
}

func (services InternalPoolServices) NewHostPool(host *implEndpoint.HostInfo, size int, keyspace string) *InternalHostConnPool {
	baseCtx := services.Context
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	repairCtx, repairCancel := context.WithCancel(baseCtx)
	pool := &InternalHostConnPool{
		InternalRepairCtx: repairCtx, InternalRepairCancel: repairCancel,
		InternalServices:   services,
		InternalHost:       host,
		InternalSize:       size,
		InternalKeyspace:   keyspace,
		InternalConnPicker: InternalNopConnPicker{},
		InternalFilling:    false,
		InternalClosed:     false,
		InternalLogger:     services.Logger,
		InternalDebouncer:  debounce.NewSimpleDebouncer(),
	}

	// the pool is not filled or connected
	return pool
}

// Pick a connection from this connection pool for the given query.
func (pool *InternalHostConnPool) Pick(token implEndpoint.Token) *transport.Conn {
	pool.InternalMu.RLock()
	pickable, repair := pool.InternalPickable()
	var conn *transport.Conn
	if pickable {
		conn = pool.InternalConnPicker.Pick(token)
	}
	pool.InternalMu.RUnlock()
	if repair {
		pool.InternalScheduleTypedRepair()
	}
	return conn
}

// PickInt64 picks a connection for the given raw int64 routing token, avoiding
// the Token interface boxing. Picks fall back to the boxed path when the
// connPicker does not support raw-int64 picking.
func (pool *InternalHostConnPool) PickInt64(token int64) *transport.Conn {
	pool.InternalMu.RLock()
	pickable, repair := pool.InternalPickable()
	var conn *transport.Conn
	if pickable {
		if p, ok := pool.InternalConnPicker.(InternalInt64ConnPicker); ok {
			conn = p.PickInt64(token)
		} else {
			conn = pool.InternalConnPicker.Pick(implEndpoint.InternalInt64Token(token))
		}
	}
	pool.InternalMu.RUnlock()
	if repair {
		pool.InternalScheduleTypedRepair()
	}
	return conn
}

// PickConn routes a picked host to the raw-int64 fast path when it exposes one,
// and to the boxed Token path otherwise.
func (pool *InternalHostConnPool) PickConn(host implEndpoint.SelectedHost) *transport.Conn {
	if th, ok := host.(implEndpoint.InternalInt64TokenSelectedHost); ok {
		if token, has := th.TokenInt64(); has {
			return pool.PickInt64(int64(token))
		}
	}
	return pool.Pick(host.Token())
}

// pickable reports whether the pool is open and has (or is filling)
// connections, and whether a typed pool misses connections, in which case
// the caller requests repair with InternalScheduleTypedRepair after
// releasing pool.mu. Must be called with pool.mu held.
func (pool *InternalHostConnPool) InternalPickable() (pickable, repair bool) {
	if pool.InternalClosed {
		return false, false
	}

	size, missing := pool.InternalConnPicker.Size()
	if missing > 0 && !pool.retryStopped {
		// Legacy callers retain their existing debounce path. Typed runtime repair
		// always uses ReconnectPolicy.
		if _, typed := pool.typedConfig(); typed {
			repair = true
		} else {
			go pool.InternalFill_debounce()
		}

		if size == 0 {
			return false, repair
		}
	}

	return true, repair
}

// Size returns the number of connections currently active in the pool
func (pool *InternalHostConnPool) Size() int {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()

	size, _ := pool.InternalConnPicker.Size()
	return size
}

// Size returns the number of connections currently active in the pool
func (pool *InternalHostConnPool) InFlight() int {
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()

	size := pool.InternalConnPicker.InFlight()
	return size
}

// Close the connection pool
func (pool *InternalHostConnPool) Close() {
	pool.InternalMu.Lock()
	if pool.InternalClosed {
		pool.InternalMu.Unlock()
		return
	}
	pool.InternalClosed = true
	pool.InternalState = InternalPoolClosed
	pool.generation++
	pool.signalReservationLocked()
	if pool.InternalRepairCancel != nil {
		pool.InternalRepairCancel()
	}
	pool.InternalMu.Unlock()

	pool.InternalConnPicker.Close()
}

// Fill the connection pool
func (pool *InternalHostConnPool) InternalFill() {
	if _, typed := pool.typedConfig(); !typed {
		pool.fillNow()
		return
	}
	pool.InternalMu.Lock()
	initial := !pool.InternalStarted && !pool.InternalClosed
	if initial {
		pool.InternalStarted = true
	}
	pool.InternalMu.Unlock()
	if initial {
		pool.fillNow()
	} else {
		pool.InternalScheduleTypedRepair()
	}
}
func (pool *InternalHostConnPool) fillNow() {
	pool.InternalMu.RLock()
	// avoid filling a closed pool, or concurrent filling
	if pool.InternalClosed || pool.InternalFilling {
		pool.InternalMu.RUnlock()
		return
	}

	// determine the filling work to be done
	startCount, fillCount := pool.InternalConnPicker.Size()

	// avoid filling a full (or overfull) pool
	if fillCount <= 0 {
		pool.InternalMu.RUnlock()
		return
	}

	// switch from read to write lock
	pool.InternalMu.RUnlock()
	pool.InternalMu.Lock()

	startCount, fillCount = pool.InternalConnPicker.Size()
	if pool.InternalClosed || pool.InternalFilling || fillCount <= 0 {
		// looks like another goroutine already beat this
		// goroutine to the filling
		pool.InternalMu.Unlock()
		return
	}

	// ok fill the pool
	pool.InternalFilling = true
	if startCount == 0 {
		pool.InternalState = poolInitializing
	}

	// allow others to access the pool while filling
	pool.InternalMu.Unlock()
	// only this goroutine should make calls to fill/empty the pool at this
	// point until after this routine or its subordinates calls
	// fillingStopped

	// fill only the first connection synchronously
	if startCount == 0 {
		err := pool.InternalConnect()
		pool.logConnectErr(err)

		if err != nil {
			// probably unreachable host
			pool.fillingStopped(err)
			return
		}
		// Reset before the rest of the fill starts, so that the policy
		// sees it ahead of any shard failure that fill reports.
		pool.resetConviction()
		// notify the session that this node is connected
		pool.InternalServices.Worker(func() { pool.InternalServices.Connected(pool) })

		// filled one, let's reload it to see if it has changed
		pool.InternalMu.RLock()
		_, fillCount = pool.InternalConnPicker.Size()
		pool.InternalMu.RUnlock()
	}

	// fill the rest of the pool asynchronously
	go func() {
		err := pool.InternalConnectMany(fillCount)

		// mark the end of filling
		pool.fillingStopped(err)

		if err == nil && startCount > 0 {
			// notify the session that this node is connected again
			pool.InternalServices.Worker(func() { pool.InternalServices.Connected(pool) })
		}
	}()
}

func (pool *InternalHostConnPool) InternalFill_debounce() {
	pool.InternalDebouncer.Debounce(pool.InternalFill)
}

func (pool *InternalHostConnPool) logConnectErr(err error) {
	if opErr, ok := errors.AsType[*net.OpError](err); ok && (opErr.Op == "dial" || opErr.Op == "read") {
		// connection refused
		// these are typical during a node outage so avoid log spam.
		pool.logger().Debug("unable to dial host", logging.KeyHost, pool.InternalHost.ConnectAddress().String(), logging.Err(err))
	} else if err != nil {
		// unexpected error
		pool.logger().Warn("failed to connect to host", logging.KeyHost, pool.InternalHost.ConnectAddress().String(), logging.Err(err))
	}
}

// transition back to a not-filling state.
func (pool *InternalHostConnPool) fillingStopped(err error) {
	if err != nil {
		pool.logger().Debug("pool filling stopped", logging.KeyHost, pool.InternalHost.ConnectAddress().String(), logging.Err(err))
		// wait for some time to avoid back-to-back filling
		// this provides some time between failed attempts
		// to fill the pool for the host to recover
		if _, typed := pool.typedConfig(); !typed {
			time.Sleep(time.Duration(rand.Int31n(100)+31) * time.Millisecond)
		}
	}

	pool.InternalMu.Lock()
	pool.InternalFilling = false
	count, missing := pool.InternalConnPicker.Size()
	if !pool.InternalClosed {
		if count > 0 {
			pool.InternalState = InternalPoolReady
			if err == nil && missing == 0 {
				pool.reconnectAttempt = 0
				pool.retryStopped = false
			}
		} else if err != nil {
			pool.InternalState = poolBroken
		}
	}
	host := pool.InternalHost
	port := pool.InternalHost.Port()
	// A closed pool no longer speaks for its host, which may be served by
	// a new pool by now.
	convict := pool.shardConvicted && !pool.InternalClosed
	pool.shardConvicted = false
	pool.InternalMu.Unlock()
	if missing > 0 {
		pool.InternalScheduleTypedRepair()
	}

	// if we errored and the size is now zero, make sure the host is marked as down
	// see https://github.com/apache/cassandra-gocql-driver/issues/1614
	pool.logger().Debug("pool size after filling stopped", logging.KeyHost, host.ConnectAddress().String(), "conns", count)
	if err != nil && count == 0 {
		// An empty failed pool supplies its detached physical host context. Avoid
		// looking the pool up through its parent while initial SetHosts may be
		// waiting for this fill to finish under the parent's membership lock.
		snapshot, _ := implEndpoint.SnapshotHost(host)
		if pool.InternalServices.Conviction.AddFailure(connectivity.ConvictionEvent{Err: err, Host: snapshot, Source: connectivity.ConvictionPool}) {
			convict = true
		}
	}
	if convict {
		pool.InternalServices.Down(host.ConnectAddress(), port)
	}
}

// resetConviction resets the conviction policy for the host once the pool
// opens its first connection. It runs on the filling goroutine before the
// rest of the fill starts.
func (pool *InternalHostConnPool) resetConviction() {
	pool.InternalMu.Lock()
	pool.failedShards = nil
	pool.InternalMu.Unlock()
	if policy := pool.InternalServices.Conviction; policy != nil {
		snapshot, _ := implEndpoint.SnapshotHost(pool.InternalHost)
		policy.Reset(connectivity.ConvictionEvent{Host: snapshot, Source: connectivity.ConvictionPool})
	}
}

// shardFailed reports a failed connection to shard, chosen by the picker out
// of shards, to the conviction policy, once per connection attempt after the
// reconnection retries. shards is zero when the picker did not choose the
// shard. A convicting verdict marks the host down when filling
// stops.
func (pool *InternalHostConnPool) shardFailed(err error, shard, shards int) {
	policy := pool.InternalServices.Conviction
	if policy == nil || shards <= 0 || pool.InternalRepairCtx.Err() != nil {
		return
	}
	snapshot, _ := implEndpoint.SnapshotHost(pool.InternalHost)
	convict := policy.AddFailure(connectivity.ConvictionEvent{Err: err, Host: snapshot, Shard: cqltype.Some(uint32(shard)), Source: connectivity.ConvictionPool})
	pool.InternalMu.Lock()
	if pool.failedShards == nil {
		pool.failedShards = make(map[uint32]struct{})
	}
	pool.failedShards[uint32(shard)] = struct{}{}
	pool.shardConvicted = pool.shardConvicted || convict
	pool.InternalMu.Unlock()
}

// shardConnected resets the conviction policy for shard when its failure was
// reported.
func (pool *InternalHostConnPool) shardConnected(shard, shards int) {
	policy := pool.InternalServices.Conviction
	if policy == nil || shards <= 0 {
		return
	}
	pool.InternalMu.Lock()
	_, failed := pool.failedShards[uint32(shard)]
	delete(pool.failedShards, uint32(shard))
	pool.InternalMu.Unlock()
	if failed {
		snapshot, _ := implEndpoint.SnapshotHost(pool.InternalHost)
		policy.Reset(connectivity.ConvictionEvent{Host: snapshot, Shard: cqltype.Some(uint32(shard)), Source: connectivity.ConvictionPool})
	}
}

// connectMany creates new connections concurrent.
func (pool *InternalHostConnPool) InternalConnectMany(count int) error {
	if count <= 0 {
		return nil
	}
	workers := min(count, 32)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var connectErr error
	next := 0
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if connectErr != nil || next >= count {
					mu.Unlock()
					return
				}
				next++
				mu.Unlock()
				if pool.InternalRepairCtx.Err() != nil {
					return
				}
				err := pool.InternalConnect()
				pool.logConnectErr(err)
				if err != nil {
					mu.Lock()
					if connectErr == nil {
						connectErr = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return connectErr
}

// create a new connection to the host and add it to the pool
func (pool *InternalHostConnPool) InternalConnect() (err error) {
	pool.InternalMu.Lock()
	picker := pool.InternalConnPicker
	shardID, nrShards := picker.NextShard()
	generation := pool.generation
	var pickerGeneration uint64
	if typed, ok := picker.(*InternalTypedConnPicker); ok {
		pickerGeneration = typed.InternalOpeningGeneration()
	}
	pool.InternalMu.Unlock()
	if typed, ok := picker.(*InternalTypedConnPicker); ok {
		defer typed.InternalDialFinished(shardID, nrShards)
	}

	// TODO: provide a more robust connection retry mechanism, we should also
	// be able to detect hosts that come up by trying to connect to downed ones.
	// try to connect
	var conn *transport.Conn
	reconnectionPolicy := pool.InternalServices.Reconnection
	for i := 0; i < reconnectionPolicy.GetMaxRetries(); i++ {
		conn, err = pool.InternalServices.Dial(pool.InternalRepairCtx, pool.InternalHost, pool, shardID, nrShards)
		if err == nil {
			break
		}
		if opErr, ok := errors.AsType[*net.OpError](err); ok {
			// if the error is not a temporary error (ex: network unreachable) don't
			//  retry
			if !opErr.Temporary() {
				break
			}
		}
		pool.logger().Debug("connection failed; reconnecting", logging.KeyHost, pool.InternalHost.ConnectAddress().String(),
			logging.Err(err), "attempt", i, "policy", fmt.Sprintf("%T", reconnectionPolicy))
		time.Sleep(reconnectionPolicy.GetInterval(i))
	}

	if err != nil {
		pool.shardFailed(err, shardID, nrShards)
		return err
	}
	// Validate rejects a non-positive limit, but a custom policy can still
	// return one here, which leaves conn nil without an error.
	if conn == nil {
		return fmt.Errorf("gocql: no connection attempted, %T.GetMaxRetries is not positive",
			reconnectionPolicy)
	}

	openingCtx, openingCancel := conn.InternalOpeningContext(pool.InternalRepairCtx)
	defer openingCancel()
	stopAndJoin := conn.InternalSetupCancellation(openingCtx)
	defer stopAndJoin()
	if pool.InternalKeyspace != "" {
		if err = conn.InternalUseKeyspaceContext(openingCtx, pool.InternalKeyspace); err != nil {
			conn.Close()
			pool.shardFailed(err, shardID, nrShards)
			return err
		}
	}
	if err = conn.InternalValidateOpeningTarget(openingCtx); err != nil {
		conn.Close()
		pool.shardFailed(err, shardID, nrShards)
		return err
	}
	// A setup cancellation callback cannot survive publication and later close a
	// connection after it has begun serving unrelated requests.
	stopAndJoin()
	if err = openingCtx.Err(); err != nil {
		conn.Close()
		return err
	}

	// add the Conn to the pool
	pool.InternalMu.Lock()
	if pool.InternalClosed || generation != pool.generation || openingCtx.Err() != nil {
		pool.InternalMu.Unlock()
		conn.Close()
		return nil
	}

	typed := conn.InternalCfg != nil && conn.InternalCfg.TypedConfig != nil
	if typed {
		implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Lock()
		if conn.InternalTargetEpoch != implEndpoint.HostInfoData(pool.InternalHost).InternalTransportGeneration || conn.InternalOpeningSharderEpoch != implEndpoint.HostInfoData(pool.InternalHost).InternalSharderGeneration && (implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards != implEndpoint.ScyllaHostFeaturesData(&implEndpoint.HostInfoData(pool.InternalHost).InternalScyllaFeatures).InternalNrShards || implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalMsbIgnore != implEndpoint.ScyllaHostFeaturesData(&implEndpoint.HostInfoData(pool.InternalHost).InternalScyllaFeatures).InternalMsbIgnore) {
			implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
			pool.InternalMu.Unlock()
			conn.Close()
			return cqltype.TypeValidation("pool generation", "opening attempt belongs to superseded endpoint/route")
		}
	}
	// lazily initialize the connPicker when we know the required type
	if err := pool.InternalInitConnPicker(conn); err != nil {
		if typed {
			implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
		}
		pool.InternalMu.Unlock()
		conn.Close()
		return err
	}
	put := func() error {
		if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
			_, oldTyped := picker.(*InternalTypedConnPicker)
			if !oldTyped {
				return p.InternalPutInitial(conn)
			}
			return p.InternalPut(conn, pickerGeneration, true)
		}
		return pool.InternalConnPicker.Put(conn)
	}
	if err := put(); err != nil {
		if typed {
			implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
		}
		pool.InternalMu.Unlock()
		if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
			closeConns(p.takeRetired()...)
		}
		conn.Close()
		pool.logger().Debug("connection not added to pool", logging.KeyHost, pool.InternalHost.ConnectAddress().String(), logging.Err(err))
		return nil
	}
	if typed {
		if implEndpoint.HostInfoData(pool.InternalHost).InternalScyllaFeatures != conn.InternalScyllaSupported.ScyllaHostFeatures {
			implEndpoint.InternalHostInfoSetScyllaFeaturesLocked(pool.InternalHost, conn.InternalScyllaSupported.ScyllaHostFeatures)
		}
		implEndpoint.HostInfoData(pool.InternalHost).InternalMu.Unlock()
	}
	pool.InternalState = InternalPoolReady
	pool.signalReservationLocked()
	pool.InternalMu.Unlock()
	if p, ok := pool.InternalConnPicker.(*InternalTypedConnPicker); ok {
		closeConns(p.takeRetired()...)
	}
	conn.InternalFinalizeConnection()
	pool.shardConnected(shardID, nrShards)

	return nil
}

func (pool *InternalHostConnPool) InternalInitConnPicker(conn *transport.Conn) error {
	if _, ok := pool.InternalConnPicker.(InternalNopConnPicker); !ok {
		return nil
	}
	if cfg, ok := pool.typedConfig(); ok {
		picker, err := InternalNewTypedConnPicker(conn, cfg.Pool)
		if err != nil {
			return err
		}
		pool.InternalConnPicker = picker
		pool.typed.Store(picker)
		return nil
	}
	if conn.InternalIsScyllaConn() && implEndpoint.ScyllaHostFeaturesValueData(conn.InternalGetScyllaSupported().ScyllaHostFeatures).InternalNrShards != 0 {
		pool.InternalConnPicker = newScyllaConnPicker(conn, pool.InternalLogger)
		return nil
	}
	pool.InternalConnPicker = InternalNewDefaultConnPicker(pool.InternalSize)
	return nil
}

// handle any error from a Conn
func (pool *InternalHostConnPool) HandleError(conn *transport.Conn, err error, closed bool) {
	if !closed {
		return
	}
	pool.InternalMu.Lock()
	if pool.InternalClosed {
		pool.InternalMu.Unlock()
		return
	}
	pool.InternalConnPicker.Remove(conn)
	pool.signalReservationLocked()
	if n, _ := pool.InternalConnPicker.Size(); n == 0 {
		pool.InternalState = poolBroken
	}
	pool.InternalMu.Unlock()
	pool.logger().Debug("pool connection error", logging.KeyHost, conn.InternalAddr, logging.Err(err))
	if _, typed := pool.typedConfig(); typed {
		pool.InternalScheduleTypedRepair()
	} else {
		go pool.InternalFill_debounce()
	}
}

func (pool *InternalHostConnPool) GetConnectionCount() int {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalConnPicker.GetConnectionCount()
}

func (pool *InternalHostConnPool) GetExcessConnectionCount() int {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalConnPicker.GetExcessConnectionCount()
}

func (pool *InternalHostConnPool) GetShardCount() int {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalConnPicker.GetShardCount()
}

func (pool *InternalHostConnPool) Host() implEndpoint.HostInformation {
	return pool.InternalHost
}

func (pool *InternalHostConnPool) IsClosed() bool {
	pool.InternalMu.Lock()
	defer pool.InternalMu.Unlock()
	return pool.InternalClosed
}
