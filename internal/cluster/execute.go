// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"math"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
	pool1 "github.com/scylladb/gocql/v2/internal/pool"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// TypedGeneration returns the session's prepared-schema generation.
func (s *Core) TypedGeneration() uint64 {
	return sessionTypedSchemaGeneration(s)
}

// SelectTypedConn returns a connection for a request: from the
// first of plan's routes that has one, or, without a plan, of the request's
// pinned host, or of any host.
func (s *Core) SelectTypedConn(ctx context.Context, routing model.RoutingInfo, plan implRouting.QueryPlan) (*transport.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Closed() {
		return nil, runtimeerrors.ErrClosed
	}
	if s.privateStorage.InternalPool == nil || s.privateStorage.InternalHostSource == nil {
		return nil, runtimeerrors.ErrNoConnections
	}
	if plan != nil {
		for {
			r, ok := plan.Next()
			if !ok {
				return nil, runtimeerrors.ErrNoConnections
			}
			if conn := sessionPickRoute(s, r, routing); conn != nil {
				return conn, nil
			}
		}
	}
	if routing.HostID.Set {
		shard := implRouting.AnyShard
		if routing.ShardID.Set {
			shard = int32(min(routing.ShardID.Value, math.MaxInt32))
		}
		if conn := sessionPickRoute(s, implRouting.InternalNewRoute(s.privateStorage.InternalPool.InternalRoute(routing.HostID.Value), shard), routing); conn != nil {
			return conn, nil
		}
		return nil, runtimeerrors.ErrNoConnections
	}
	hosts := s.privateStorage.InternalHostSource.InternalHostsView()
	if len(hosts) == 0 {
		return nil, runtimeerrors.ErrNoConnections
	}
	start := int(s.privateStorage.nextTypedHost.Add(1) % uint64(len(hosts)))
	for i := 0; i < len(hosts); i++ {
		r := implRouting.InternalNewRoute(s.privateStorage.InternalPool.InternalRoute(implEndpoint.InternalHostInfoHostUUID(hosts[(start+i)%len(hosts)])), implRouting.AnyShard)
		if conn := sessionPickRoute(s, r, routing); conn != nil {
			return conn, nil
		}
	}
	return nil, runtimeerrors.ErrNoConnections
}

// sessionPickRoute returns a connection of r's host for r's shard,
// or nil. A route that belongs to no session resolves its host by ID.
func sessionPickRoute(s *Core, r implRouting.Route, routing model.RoutingInfo) *transport.Conn {
	host := r.InternalHost()
	if host == nil || routing.HostID.Set && host.ID() != routing.HostID.Value {
		return nil
	}
	pool := host.Pool()
	if pool == nil && !host.Bound() {
		pool = s.privateStorage.InternalPool.InternalRoute(host.ID()).Pool()
	}
	if pool == nil || pool.InternalHost == nil || (&s.privateStorage.InternalCfg).FilterHost(pool.InternalHost) {
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
	return s.PickTypedPool(pool, target, routing)
}

func (s *Core) ObserveSafely(fn func()) {
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
func (s *Core) PickTypedPool(pool *pool1.InternalHostConnPool, target model.Target, routing model.RoutingInfo) *transport.Conn {
	pool.InternalMu.RLock()
	pickable, repair := pool.InternalPickable()
	var conn *transport.Conn
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
func pickTypedPoolLocked(s *Core, pool *pool1.InternalHostConnPool, target model.Target, routing model.RoutingInfo) *transport.Conn {
	if picker, ok := pool.InternalConnPicker.(*pool1.InternalTypedConnPicker); ok {
		return picker.InternalPickTarget(target, routing)
	}
	picker, ok := pool.InternalConnPicker.(*pool1.InternalScyllaConnPicker)
	if !ok {
		if routing.Token.Set && !target.ShardID.Set && !routing.ShardID.Set {
			if p, ok := pool.InternalConnPicker.(pool1.InternalInt64ConnPicker); ok {
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
		index = picker.InternalShardOf(implEndpoint.InternalInt64Token(routing.Token.Value))
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

func (s *Core) ApplyTypedFeedbackStamped(response *transport.InternalResponsePage, routing model.RoutingInfo, stamp implRouting.InternalTabletFeedbackStamp, features transport.InternalFramerConfig) {
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
		hint, err := transport.InternalParseTypedTabletPayload(entry.Value.Value, routing.Keyspace, routing.Table, features.InternalTabletsRoutingV2)
		if err != nil {
			if s.privateStorage.InternalLogger != nil {
				s.privateStorage.InternalLogger.Printf("gocql: tablet hint rejected: %v", err)
			}
			return
		}
		s.QueueTabletFeedback(hint, stamp)
		return
	}
}
