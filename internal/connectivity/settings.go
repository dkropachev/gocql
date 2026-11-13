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

// ConvictionSource names what failed to connect.
type ConvictionSource uint8

const (
	// ConvictionPool is a host pool connection. An event with Shard set is
	// one shard's connection; an event without it means the pool lost its
	// last connection to the host.
	ConvictionPool ConvictionSource = iota
	// ConvictionControl is the control connection's dial to the host.
	ConvictionControl
)

// ConvictionEvent describes a connection failure to a host, or to one of its
// shards, or the recovery a Reset reports.
type ConvictionEvent struct {
	// Err is the connection error; nil in Reset.
	Err error
	// Host is detached, read-only host metadata that may be retained.
	Host HostSnapshot
	// Shard is set when the event concerns the connection to one shard:
	// a failed dial to it, or, in Reset, a connection to it opened again.
	Shard  Option[uint32]
	Source ConvictionSource
}

// ConvictionPolicy decides whether connection failures mark a host down.
// Callbacks run concurrently and outside driver locks.
//
// A pool that dials shards explicitly (advanced shard awareness) reports each
// failed shard dial with Shard set, before the event without Shard that
// follows when the host has no connections left. A pool that does not choose
// shards reports only the latter.
type ConvictionPolicy interface {
	// AddFailure reports a failure and returns whether to mark the host
	// down. A true verdict on a shard event marks the whole host down once
	// the pool stops filling; there is no per-shard down state. A request for
	// a shard without a connection already uses another shard of the host,
	// unless it names a shard or is conditional.
	AddFailure(ConvictionEvent) bool
	// Reset reports a recovery: with Shard set, a connection to a shard that
	// had failed was opened again; without it, the host's pool connected
	// again. Implementations should clear the matching state.
	Reset(ConvictionEvent)
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
