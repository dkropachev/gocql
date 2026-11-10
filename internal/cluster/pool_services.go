// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implNet "net"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
)

func InternalNewPoolServices(s *Core) pool.InternalPoolServices {
	cfg, _ := s.v2PoolConfig()
	return pool.InternalPoolServices{Context: s.privateStorage.InternalCtx, Logger: s.privateStorage.InternalLogger, Config: cfg, Conviction: s.privateStorage.InternalCfg.ConvictionPolicy, Reconnection: s.privateStorage.InternalCfg.ReconnectionPolicy, Worker: func(f func()) bool { return InternalSessionPeripheral(s).InternalStartWorker(f) }, Dial: s.connectShard, Connected: func(receiver *Core) func(a0 *pool.InternalHostConnPool) {
		return func(a0 *pool.InternalHostConnPool) {
			InternalSessionHandlePoolConnected(receiver, a0)
		}
	}(s), Down: func(receiver *Core) func(a0 implNet.
		IP, a1 int) {
		return func(a0 implNet.IP, a1 int) {
			InternalSessionHandleNodeDown(receiver, a0, a1)
		}
	}(s)}
}
func InternalNewHostConnPool(s *Core, h *endpoint.HostInfo, size int, keyspace string) *pool.InternalHostConnPool {
	return pool.InternalNewHostPool(InternalNewPoolServices(s), h, size, keyspace)
}
