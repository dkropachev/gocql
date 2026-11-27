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

// DefaultMetadataServerTimeout is the USING TIMEOUT of metadata queries on
// ScyllaDB when config.WithMetadataServerTimeout is not chosen, for the
// client timeout client: one second less, so that the server gives up first
// and answers with its own error, or half of a client timeout of 2s or less.
// An unbounded client timeout keeps 30s.
func DefaultMetadataServerTimeout(client time.Duration) time.Duration {
	switch {
	case client <= 0:
		return 30 * time.Second
	case client <= 2*time.Second:
		return (client / 2).Truncate(time.Millisecond)
	default:
		return (client - time.Second).Truncate(time.Millisecond)
	}
}

// MetadataIO contains only the budgets required for encoded metadata I/O.
type MetadataIO struct {
	InitialDiscovery bool
	RequestTimeout   time.Duration
	ServerTimeout    Option[time.Duration]
}

// SocketOptions records which socket options the user chose with a setter.
// The driver applies its defaults for the others best-effort: a connection
// that does not expose the control keeps the OS's behaviour.
type SocketOptions uint8

const (
	// SocketNoDelay is set when WithNagle chose TCP_NODELAY.
	SocketNoDelay SocketOptions = 1 << iota
	// SocketKeepalive is set when WithTCPKeepalive chose TCP keepalive.
	SocketKeepalive
)

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
	TLSIdentity                                                                      func(endpoint.DialTarget) endpoint.TLSIdentity
	ClientRoutes                                                                     *ClientRoutesOptions
	ContactPoints                                                                    []string
	Pool                                                                             PoolOptions
	Metadata                                                                         MetadataIO
	ReceiveBuffer, SendBuffer                                                        int
	Linger                                                                           Option[time.Duration]
	Protocol                                                                         int
	ConnectTimeout, DNSTimeout, KeepaliveInterval, KeepaliveTimeout, SocketKeepalive time.Duration
	NoDelay                                                                          bool
	// TLSFallback dials in plaintext a node that does not answer TLS and
	// never has.
	TLSFallback bool
	// TLSPort, when not zero, replaces each node's CQL port for TLS dials.
	TLSPort uint16
	// ChosenSocketOptions are the socket options a setter chose; a connection
	// that cannot apply one of them fails to open.
	ChosenSocketOptions SocketOptions
}
