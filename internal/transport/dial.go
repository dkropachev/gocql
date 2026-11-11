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

package transport

import (
	"bufio"
	"context"
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/streams"

	"crypto/tls"
	"net"
	"net/netip"
	"strings"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// Opening captures session-owned settings before socket admission.
type Opening struct {
	Context                       context.Context
	Hooks                         InternalConnectionHooks
	Reporter                      InternalStartupReporter
	FrameObserver                 FrameHeaderObserver
	StreamObserver                StreamObserver
	Identity                      InternalStartupIdentity
	MetadataTimeout, CoalesceWait time.Duration
	MaxRequests                   int
	PoolPolicy                    InternalConnectionPoolPolicy
}

func newStreamIDs(limit int) *streams.IDGenerator {
	if limit > 0 {
		return streams.NewLimited(limit)
	}
	return streams.New()
}
func Dial(ctx context.Context, host *implEndpoint.HostInfo, cfg *ConnConfig, errorHandler ConnErrorHandler,
	shardID, nrShards int, opening Opening) (*Conn, error) {
	lifetimeCtx := ctx
	if value, ok := ctx.Value(InternalConnectionLifetimeKey{}).(context.Context); ok {
		lifetimeCtx = value
	}
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	if cfg.ProtoVersion < 0 || cfg.ProtoVersion > 127 {
		return nil, fmt.Errorf("invalid protocol version %d: must be between 0 and 127", cfg.ProtoVersion)
	}

	shardDialer, ok := cfg.HostDialer.(implEndpoint.ShardDialer)
	var (
		dialedHost *implEndpoint.DialedHost
		err        error
	)

	isShardAware := false
	if ok && nrShards > 0 {
		isShardAware = true
		dialedHost, err = shardDialer.DialShard(ctx, host, shardID, nrShards)
	} else {
		dialedHost, err = cfg.HostDialer.DialHost(ctx, host)
	}

	if err != nil {
		return nil, err
	}

	if cfg.TypedConfig != nil {
		isShardAware = implEndpoint.DialedHostData(dialedHost).InternalShardTarget.Set
	}
	openingDeadline, _ := ctx.Deadline()
	connectionCtx, cancel := context.WithCancel(lifetimeCtx)
	c := &Conn{
		InternalR: &InternalConnReader{
			InternalConn: dialedHost.Conn,
			InternalR:    bufio.NewReaderSize(dialedHost.Conn, 8<<10),
			transferable: true,
		},
		InternalCfg:                 cfg,
		InternalCalls:               InternalNewCallTable(),
		InternalVersion:             uint8(cfg.ProtoVersion),
		InternalIsShardAware:        isShardAware,
		InternalRequestedShard:      implEndpoint.DialedHostData(dialedHost).InternalShardTarget,
		InternalTargetEpoch:         implEndpoint.DialedHostData(dialedHost).InternalTargetEpoch,
		InternalOpeningSharderEpoch: implEndpoint.DialedHostData(dialedHost).InternalSharderEpoch,
		InternalOpeningDeadline:     openingDeadline,
		InternalValidateTarget:      implEndpoint.DialedHostData(dialedHost).InternalValidateTarget,
		InternalAddr:                dialedHost.Conn.RemoteAddr().String(),
		InternalErrorHandler:        errorHandler,
		InternalCompressor:          cfg.Compressor,
		InternalHooks:               opening.Hooks,
		startupIdentity:             opening.Identity,
		startupReporter:             opening.Reporter,
		InternalMetadataTimeout:     opening.MetadataTimeout,
		coalesceWait:                opening.CoalesceWait,
		InternalPoolPolicy:          opening.PoolPolicy,
		InternalStreams:             newStreamIDs(opening.MaxRequests),
		InternalHost:                host,
		InternalIsSchemaV2:          true, // Try using "system.peers_v2" until proven otherwise
		InternalFrameObserver:       opening.FrameObserver,
		InternalW: &InternalDeadlineContextWriter{
			InternalW:         dialedHost.Conn,
			InternalSemaphore: make(chan struct{}, 1),
			InternalQuit:      make(chan struct{}),
		},
		InternalCtx:            connectionCtx,
		InternalCancel:         cancel,
		InternalLogger:         cfg.logger().With(logging.KeyHost, dialedHost.Conn.RemoteAddr().String()),
		InternalStreamObserver: opening.StreamObserver,
	}
	c.InternalSetSystemRequestTimeout(cfg.ConnectTimeout)
	c.InternalCloseOnCancel()

	if err := c.init(ctx, dialedHost); err != nil {
		cancel()
		c.Close()
		return nil, err
	}

	return c, nil
}

func InternalTlsConfigForAddr(tlsConfig *tls.Config, addr string) *tls.Config {

	if !tlsConfig.InsecureSkipVerify && tlsConfig.ServerName == "" {
		hostname, _, err := net.SplitHostPort(addr)
		if err != nil {
			hostname = strings.Trim(addr, "[]")
		}
		if ip, err := netip.ParseAddr(hostname); err == nil {
			hostname = ip.WithZone("").String()
		}

		tlsConfig = tlsConfig.Clone()
		tlsConfig.ServerName = hostname
	}
	return tlsConfig
}

func WrapTLS(ctx context.Context, conn net.Conn, addr string, tlsConfig *tls.Config) (*implEndpoint.DialedHost, error) {
	if tlsConfig != nil {
		tlsConfig := InternalTlsConfigForAddr(tlsConfig, addr)
		tconn := tls.Client(conn, tlsConfig)
		if err := tconn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		conn = tconn
	}

	return &implEndpoint.DialedHost{
		Conn:            conn,
		DisableCoalesce: tlsConfig != nil,
	}, nil
}
