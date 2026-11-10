// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	implNet "net"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
)

func (s *Core) NewPoolServices() pool.InternalPoolServices {
	cfg, _ := s.v2PoolConfig()
	return pool.InternalPoolServices{Context: s.privateStorage.InternalCtx, Logger: s.privateStorage.InternalLogger, Config: cfg, Conviction: s.privateStorage.InternalCfg.ConvictionPolicy, Reconnection: s.privateStorage.InternalCfg.ReconnectionPolicy, Worker: func(f func()) bool { return s.Peripheral().InternalStartWorker(f) }, Dial: s.connectShard, Connected: func(receiver *Core) func(a0 *pool.InternalHostConnPool) {
		return func(a0 *pool.InternalHostConnPool) {
			receiver.HandlePoolConnected(a0)
		}
	}(s), Down: func(receiver *Core) func(a0 implNet.
		IP, a1 int) {
		return func(a0 implNet.IP, a1 int) {
			receiver.HandleNodeDown(a0, a1)
		}
	}(s)}
}
func (s *Core) NewHostConnPool(h *endpoint.HostInfo, size int, keyspace string) *pool.InternalHostConnPool {
	return pool.InternalNewHostPool(s.NewPoolServices(), h, size, keyspace)
}
