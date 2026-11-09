// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"math"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
)

func InternalSessionTypedGeneration(s *Core) uint64 {
	return InternalSessionTypedSchemaGeneration(s)
}

// InternalSessionSelectTypedConn returns a connection for a request: from the
// first of plan's routes that has one, or, without a plan, of the request's
// pinned host, or of any host.
func InternalSessionSelectTypedConn(s *Core, ctx context.Context, routing RoutingInfo, plan QueryPlan) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Closed() {
		return nil, ErrClosed
	}
	if s.privateStorage.InternalPool == nil || s.privateStorage.InternalHostSource == nil {
		return nil, ErrNoConnections
	}
	if plan != nil {
		for {
			r, ok := plan.Next()
			if !ok {
				return nil, ErrNoConnections
			}
			if conn := InternalSessionPickRoute(s, r, routing); conn != nil {
				return conn, nil
			}
		}
	}
	if routing.HostID.Set {
		shard := implRouting.AnyShard
		if routing.ShardID.Set {
			shard = int32(min(routing.ShardID.Value, math.MaxInt32))
		}
		if conn := InternalSessionPickRoute(s, implRouting.InternalNewRoute(s.privateStorage.InternalPool.InternalRoute(routing.HostID.Value), shard), routing); conn != nil {
			return conn, nil
		}
		return nil, ErrNoConnections
	}
	hosts := s.privateStorage.InternalHostSource.InternalHostsView()
	if len(hosts) == 0 {
		return nil, ErrNoConnections
	}
	start := int(s.privateStorage.nextTypedHost.Add(1) % uint64(len(hosts)))
	for i := 0; i < len(hosts); i++ {
		r := implRouting.InternalNewRoute(s.privateStorage.InternalPool.InternalRoute(implEndpoint.InternalHostInfoHostUUID(hosts[(start+i)%len(hosts)])), implRouting.AnyShard)
		if conn := InternalSessionPickRoute(s, r, routing); conn != nil {
			return conn, nil
		}
	}
	return nil, ErrNoConnections
}

// InternalSessionPickRoute returns a connection of r's host for r's shard,
// or nil. A route that belongs to no session resolves its host by ID.
func InternalSessionPickRoute(s *Core, r Route, routing RoutingInfo) *Conn {
	host := r.InternalHost()
	if host == nil || routing.HostID.Set && host.ID() != routing.HostID.Value {
		return nil
	}
	pool := host.Pool()
	if pool == nil && !host.Bound() {
		pool = s.privateStorage.InternalPool.InternalRoute(host.ID()).Pool()
	}
	if pool == nil || pool.InternalHost == nil || InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, pool.InternalHost) {
		return nil
	}
	if conn, ok := pool.PickRoute(r.Shard); ok {
		return conn
	}
	if !pool.InternalHost.IsUp() {
		return nil
	}
	// A pool without a typed picker picks the legacy way, strictly when
	// the route names a shard.
	target := r.Target()
	target.StrictShard = target.ShardID.Set
	return InternalSessionPickTypedPool(s, pool, target, routing)
}

func InternalSessionObserveSafely(s *Core, fn func()) {
	defer func() {
		if v := recover(); v != nil && s.privateStorage.InternalLogger != nil {
			func() {
				defer func() { _ = recover() }()
				s.privateStorage.InternalLogger.Printf("gocql: observer panic: %v", v)
			}()
		}
	}()
	fn()
}

// pickTypedPool carries native shard targets through connection selection without
// manufacturing an old query object or discarding table-specific tablet routing.
func InternalSessionPickTypedPool(s *Core, pool *hostConnPool, target Target, routing RoutingInfo) *Conn {
	pool.InternalMu.RLock()
	pickable, repair := pool.InternalPickable()
	var conn *Conn
	if pickable {
		conn = pickTypedPoolLocked(s, pool, target, routing)
	}
	pool.InternalMu.RUnlock()
	if repair {
		pool.InternalScheduleTypedRepair()
	}
	return conn
}

// pickTypedPoolLocked picks from a pickable pool. Must be called with pool.mu held.
func pickTypedPoolLocked(s *Core, pool *hostConnPool, target Target, routing RoutingInfo) *Conn {
	if picker, ok := pool.InternalConnPicker.(*typedConnPicker); ok {
		return picker.InternalPickTarget(target, routing)
	}
	picker, ok := pool.InternalConnPicker.(*scyllaConnPicker)
	if !ok {
		if routing.Token.Set && !target.ShardID.Set && !routing.ShardID.Set {
			if p, ok := pool.InternalConnPicker.(int64ConnPicker); ok {
				return p.PickInt64(routing.Token.Value)
			}
		}
		conn := pool.InternalConnPicker.Pick(nil)
		shard := target.ShardID
		if routing.ShardID.Set {
			shard = routing.ShardID
		}
		if conn != nil && shard.Set && (target.StrictShard || routing.ShardID.Set) && uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard) != shard.Value {
			return nil
		}
		return conn
	}
	shard := target.ShardID
	if !shard.Set && routing.Token.Set && routing.Keyspace != "" && routing.Table != "" {
		if targets, err := s.Metadata().Replicas(routing.Keyspace, routing.Table, routing.Token.Value); err == nil {
			for _, candidate := range targets {
				if candidate.HostID == implEndpoint.InternalHostInfoHostUUID(pool.InternalHost) && candidate.ShardID.Set {
					shard = candidate.ShardID
					break
				}
			}
		}
	}
	picker.InternalMu.RLock()
	defer picker.InternalMu.RUnlock()
	if len(picker.InternalConns) == 0 {
		return nil
	}
	index := -1
	if shard.Set {
		if uint64(shard.Value) < uint64(len(picker.InternalConns)) {
			index = int(shard.Value)
		} else if target.StrictShard || routing.ShardID.Set || routing.Conditional {
			return nil
		}
	} else if routing.Token.Set {
		index = picker.InternalShardOf(int64Token(routing.Token.Value))
	}
	if index >= 0 && index < len(picker.InternalConns) && picker.InternalConns[index] != nil {
		conn := picker.InternalConns[index]
		if shard.Set || routing.Conditional {
			return conn
		}
		return picker.InternalMaybeReplaceWithLessBusyConnection(conn)
	}
	if index >= 0 && (target.StrictShard || routing.ShardID.Set || routing.Conditional) {
		return nil
	}
	return picker.InternalLeastBusyConn()
}

func InternalSessionApplyTypedFeedbackStamped(s *Core, response *responsePage, routing RoutingInfo, stamp tabletFeedbackStamp, features framerConfig) {
	if response == nil || routing.Keyspace == "" || routing.Table == "" || s.privateStorage.InternalMetadataDescriber == nil {
		return
	}
	key := "tablets-routing-v1"
	if features.InternalTabletsRoutingV2 {
		key = "tablets-routing-v2"
	} else if !features.InternalTabletsRoutingV1 {
		return
	}
	for _, entry := range response.Payload() {
		if entry.Key != key || !entry.Value.Valid {
			continue
		}
		hint, err := parseTypedTabletPayload(entry.Value.Value, routing.Keyspace, routing.Table, features.InternalTabletsRoutingV2)
		if err != nil {
			if s.privateStorage.InternalLogger != nil {
				s.privateStorage.InternalLogger.Printf("gocql: tablet hint rejected: %v", err)
			}
			return
		}
		InternalSessionQueueTabletFeedback(s, hint, stamp)
		return
	}
}
