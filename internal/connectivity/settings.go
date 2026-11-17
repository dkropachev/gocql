// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package connectivity contains physical connection/pool contracts without
// execution policies, routing implementations or schema loaders.
package connectivity

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/metadata"
)

type Option[T any] = cql.Option[T]
type Nullable[T any] = cql.Nullable[T]
type Bytes = cql.Bytes
type HostSnapshot = metadata.HostSnapshot
type PoolOptions struct {
	ConnectionsPerHost, ConnectionsPerShard int
	MaxExcessPerShard                       int
	MaxRequestsPerConnection                int
	MaxOrphanedStreams                      int
	AdvancedShardAwareness                  AdvancedShardAwarenessOptions
}

// AdvancedShardAwarenessOptions picks each connection's shard by dialing the
// node's shard-aware port from a local source port in
// [SourcePortStart, SourcePortEnd].
type AdvancedShardAwarenessOptions struct {
	Enabled                        bool
	SourcePortStart, SourcePortEnd uint16
}

type ProtocolLimits struct {
	MaxFrameBytes, MaxDecompressedBytes int64
	MaxMetadataDepth                    int
	MaxPagingStates                     int
	MaxPagingStateBytes                 int64
}

// MetadataIO contains only the budgets required for encoded metadata I/O.
type MetadataIO struct {
	InitialDiscovery bool
	RequestTimeout   time.Duration
	ServerTimeout    Option[time.Duration]
}

type ClientRoutesOptions struct {
	HostnameOverrides map[string]string
	ConnectionIDs     []string
}

type Config struct {
	LocalAddress                                                                     netip.Addr
	Dialer                                                                           endpoint.Dialer
	DNSResolver                                                                      endpoint.Resolver
	Reconnect                                                                        endpoint.ReconnectPolicy
	DialHost                                                                         func(context.Context, endpoint.DialTarget) (net.Conn, error)
	TLS                                                                              *tls.Config
	TLSSource                                                                        endpoint.TLSSource
	ClientRoutes                                                                     *ClientRoutesOptions
	ContactPoints                                                                    []string
	Pool                                                                             PoolOptions
	Metadata                                                                         MetadataIO
	ReceiveBuffer, SendBuffer                                                        Option[int]
	Linger                                                                           Option[time.Duration]
	Protocol                                                                         int
	ConnectTimeout, DNSTimeout, KeepaliveInterval, KeepaliveTimeout, SocketKeepalive time.Duration
	NoDelay                                                                          bool
}
