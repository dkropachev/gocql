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
	"net"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func connConfig(cfg *InternalClusterSettings) (*transport.ConnConfig, error) {
	hostDialer := cfg.HostDialer

	if hostDialer == nil {
		dialer := cfg.Dialer
		if dialer == nil {
			d := net.Dialer{
				Timeout: cfg.ConnectTimeout,
			}
			if cfg.SocketKeepalive > 0 {
				d.KeepAlive = cfg.SocketKeepalive
			}
			dialer = &endpoint.ScyllaShardAwareDialer{Dialer: d}
		}

		hostDialer = &transport.InternalScyllaDialer{
			InternalDialer:    dialer,
			InternalLogger:    cfg.logger(),
			InternalTlsConfig: clusterSettingsGetActualTLSConfig(cfg),
		}
	}

	return &transport.ConnConfig{
		ProtoVersion:      cfg.ProtoVersion,
		CQLVersion:        cfg.CQLVersion,
		WriteTimeout:      cfg.WriteTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		ConnectTimeout:    cfg.ConnectTimeout,
		Dialer:            cfg.Dialer,
		HostDialer:        hostDialer,
		Compressor:        cfg.Compressor,
		Authenticator:     cfg.Authenticator,
		AuthProvider:      cfg.AuthProvider,
		Keepalive:         cfg.SocketKeepalive,
		Logger:            cfg.logger(),
		InternalTlsConfig: clusterSettingsGetActualTLSConfig(cfg),
	}, nil
}

func newPolicyConnPool(session *Core) *pool.InternalPolicyConnPool {
	// create the pool
	pool := &pool.InternalPolicyConnPool{
		InternalServices:      InternalNewPoolServices(session),
		InternalPort:          session.privateStorage.InternalCfg.Port,
		InternalNumConns:      session.privateStorage.InternalCfg.NumConns,
		InternalKeyspace:      session.privateStorage.InternalCfg.Keyspace,
		InternalHostConnPools: map[cqltype.UUID]*pool.InternalHostConnPool{},
	}

	return pool
}
