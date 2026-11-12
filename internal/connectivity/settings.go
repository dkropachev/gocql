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

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
)

type Option[T any] = cqltype.Option[T]
type Nullable[T any] = cqltype.Nullable[T]
type Bytes = cqltype.Bytes
type HostSnapshot = model.HostSnapshot
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

type ReconnectPolicy interface {
	Delay(attempt int) (time.Duration, bool)
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

// TLSSource supplies the TLS configuration of each new connection.
type TLSSource interface {
	// TLSConfig returns the configuration for the next connection. It is
	// called on every dial, from many goroutines; the result is copied and
	// must not be nil.
	TLSConfig() *tls.Config
}

type ClientRoutesOptions struct {
	HostnameOverrides map[string]string
	ConnectionIDs     []string
}

type AuthProvider interface {
	Start(context.Context, HostSnapshot, string) (AuthExchange, Nullable[[]byte], error)
}

type AuthExchange interface {
	Challenge(context.Context, Nullable[Bytes]) (Nullable[[]byte], error)
	Success(context.Context, Nullable[Bytes]) error
	Close()
}

// ConvictionPolicy receives detached, read-only host metadata. Implementations
// may retain the snapshot and must support concurrent callbacks.
type ConvictionPolicy interface {
	// Implementations should return `true` if the host should be convicted, `false` otherwise.
	AddFailure(err error, host HostSnapshot) bool
	// Implementations should clear out any convictions or state regarding the host.
	Reset(host HostSnapshot)
}
type Config struct {
	LocalAddress                                                                     netip.Addr
	Dialer                                                                           implEndpoint.Dialer
	DNSResolver                                                                      implEndpoint.Resolver
	Reconnect                                                                        ReconnectPolicy
	DialHost                                                                         func(context.Context, implEndpoint.DialTarget) (net.Conn, error)
	TLS                                                                              *tls.Config
	TLSSource                                                                        TLSSource
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
