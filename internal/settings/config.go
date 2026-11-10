// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"context"
	"crypto/tls"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"

	// DialHost opens a raw socket using immutable host metadata; TLS is applied afterward.
	logging "github.com/scylladb/gocql/v2/internal/logging"
)

type Config struct {
	LocalAddress      netip.Addr
	Dialer            implEndpoint.Dialer
	DNSResolver       implEndpoint.DNSResolver
	Auth              AuthProvider
	Reconnect         ReconnectPolicy
	Conviction        ConvictionPolicy
	AddressTranslator implEndpoint.AddressTranslator
	Observer          Observer
	LifecycleObserver LifecycleObserver
	Logger            logging.StdLogger
	DialHost          func(context.Context, implEndpoint.DialTarget) (net.Conn, error)
	TLS               *tls.Config
	// TLSSource, when set, supplies TLS for every dial; TLS then holds its
	// config at Build, for validation.
	TLSSource                                         TLSSource
	HostFilter                                        func(model.HostSnapshot) bool
	ClientRoutes                                      *ClientRoutesOptions
	DefaultProfile                                    *Profile
	Partitioners                                      map[string]routing.TokenPartitioner
	TimestampGenerator                                func() int64
	Identity                                          IdentityOptions
	Keyspace, CQLVersion                              string
	ContactPoints                                     []string
	Metadata                                          MetadataOptions
	Pool                                              PoolOptions
	Limits                                            ProtocolLimits
	ReceiveBuffer, SendBuffer                         cqltype.Option[int]
	Linger                                            cqltype.Option[time.Duration]
	ReadTimeout                                       time.Duration
	Protocol                                          int
	ConnectTimeout, DNSTimeout                        time.Duration
	WriteTimeout, KeepaliveInterval, KeepaliveTimeout time.Duration
	SocketKeepalive                                   time.Duration
	CoalesceWait                                      time.Duration
	PreparedCacheSize                                 int
	Port                                              uint16
	NoDelay                                           bool
	Compression                                       cqltype.Compression
	ReportConfiguration                               bool
}

type PoolOptions = connectivity.PoolOptions
type ReconnectPolicy = connectivity.ReconnectPolicy
type ProtocolLimits = connectivity.ProtocolLimits
type MetadataOptions struct {
	Keyspaces                                                     []string
	ServerTimeout                                                 cqltype.Option[time.Duration]
	PeriodicRefresh                                               time.Duration
	RequestTimeout                                                time.Duration
	AgreementTimeout, AgreementPoll                               time.Duration
	Mode                                                          MetadataMode
	InitialDiscovery                                              bool
	AutomaticAgreement, RefreshAfterAgreement                     bool
	TopologyEvents, StatusEvents, SchemaEvents, ClientRouteEvents bool
}
type MetadataMode uint8

const (
	MetadataDisabled MetadataMode = iota
	MetadataMinimal
	MetadataFull
)

type IdentityOptions struct{ DriverName, DriverVersion, ApplicationName, ApplicationVersion, ClientID string }
type ClientRoutesOptions = connectivity.ClientRoutesOptions
type TLSSource = connectivity.TLSSource

func DefaultConfig(contactPoints ...string) Config {
	profile := InternalDefaultProfile()
	profile.Routing, _ = routing.NewRoutingPolicy(routing.RoutingOptions{TokenAware: true, ShuffleReplicas: true})
	return Config{ContactPoints: append([]string(nil), contactPoints...), Port: 9042, CQLVersion: "3.0.0", ConnectTimeout: 5 * time.Second, DNSTimeout: 5 * time.Second, KeepaliveInterval: 30 * time.Second, KeepaliveTimeout: 30 * time.Second, NoDelay: true, Pool: PoolOptions{ConnectionsPerShard: 1, MaxExcessPerShard: 2, MaxRequestsPerConnection: 32768, MaxOrphanedStreams: 1024, ShardAwarePort: true, SourcePortStart: 49152, SourcePortEnd: 65535}, Metadata: MetadataOptions{Mode: MetadataFull, InitialDiscovery: true, PeriodicRefresh: 60 * time.Second, RequestTimeout: 31 * time.Second, AgreementTimeout: 60 * time.Second, AgreementPoll: 200 * time.Millisecond, AutomaticAgreement: true, RefreshAfterAgreement: true, TopologyEvents: true, StatusEvents: true, SchemaEvents: true, ClientRouteEvents: true}, Limits: ProtocolLimits{MaxFrameBytes: 256 << 20, MaxDecompressedBytes: 256 << 20, MaxMetadataDepth: 64, MaxPagingStates: 65536, MaxPagingStateBytes: 16 << 20}, PreparedCacheSize: 1000, Partitioners: map[string]routing.TokenPartitioner{"org.apache.cassandra.dht.Murmur3Partitioner": routing.Murmur3TokenPartitioner, "Murmur3Partitioner": routing.Murmur3TokenPartitioner, "com.scylladb.dht.CDCPartitioner": routing.CDCTokenPartitioner, "CDCPartitioner": routing.CDCTokenPartitioner}, DefaultProfile: &profile, Identity: IdentityOptions{DriverName: InternalDefaultDriverName, DriverVersion: InternalDefaultDriverVersion}, Reconnect: defaultReconnectPolicy{}, ReportConfiguration: true, TimestampGenerator: InternalNewTimestampGenerator()}
}
func InternalNewTimestampGenerator() func() int64 {
	var last atomic.Int64
	return func() int64 {
		for {
			prev := last.Load()
			now := time.Now().UnixMicro()
			if now <= prev {
				now = prev + 1
			}
			if last.CompareAndSwap(prev, now) {
				return now
			}
		}
	}
}
func InternalValidateConfig(config Config) error {
	if _, err := InternalUnixContact(config); err != nil {
		return err
	}
	if len(config.ContactPoints) == 0 {
		return cqltype.TypeValidation("contact points", "at least one contact point is required")
	}
	if config.Protocol != 0 && config.Protocol != 3 && config.Protocol != 4 && config.Protocol != 5 {
		return cqltype.TypeValidation("protocol", "supported versions are 3, 4 and 5")
	}
	if config.Port == 0 {
		return cqltype.TypeValidation("port", "must not be zero")
	}
	if config.CQLVersion == "" {
		return cqltype.TypeValidation("CQL version", "must not be empty")
	}
	if config.ReadTimeout < 0 || config.ConnectTimeout < 0 || config.DNSTimeout < 0 || config.WriteTimeout < 0 || config.KeepaliveInterval < 0 || config.KeepaliveTimeout < 0 || config.SocketKeepalive < 0 || config.CoalesceWait < 0 {
		return cqltype.TypeValidation("timeouts", "must not be negative")
	}
	if config.KeepaliveInterval > 0 && config.KeepaliveTimeout == 0 {
		return cqltype.TypeValidation("heartbeat", "timeout must be positive when heartbeats are enabled")
	}
	if config.Protocol == 5 && config.Compression == cqltype.SnappyCompression {
		return &UnsupportedSettingError{Setting: "compression", Reason: "native protocol5 supports raw or LZ4 segments"}
	}
	if config.Compression > cqltype.SnappyCompression {
		return cqltype.TypeValidation("compression", "unknown algorithm")
	}
	if config.DefaultProfile == nil {
		return cqltype.TypeValidation("profile", "default profile is required; use DefaultConfig")
	}
	if err := InternalValidateProfile(*config.DefaultProfile); err != nil {
		return err
	}
	if config.Limits.MaxFrameBytes <= 0 || config.Limits.MaxDecompressedBytes <= 0 || config.Limits.MaxMetadataDepth <= 0 {
		return cqltype.TypeValidation("protocol limits", "must be positive")
	}
	if config.Limits.MaxPagingStates <= 0 || config.Limits.MaxPagingStateBytes <= 0 {
		return cqltype.TypeValidation("paging limits", "must be positive")
	}
	if config.Limits.MaxMetadataDepth > 128 || config.Limits.MaxFrameBytes > 1<<31-1 || config.Limits.MaxDecompressedBytes > 1<<31-1 {
		return cqltype.TypeValidation("protocol limits", "limits exceed supported protocol range")
	}
	if config.Linger.Set && (config.Linger.Value < 0 || config.Linger.Value%time.Second != 0 || config.Linger.Value/time.Second > math.MaxInt32) {
		return cqltype.TypeValidation("linger", "must be nonnegative whole seconds within int32 range")
	}
	if config.ReceiveBuffer.Set && config.ReceiveBuffer.Value < 0 || config.SendBuffer.Set && config.SendBuffer.Value < 0 {
		return cqltype.TypeValidation("socket buffers", "must not be negative")
	}
	if config.Metadata.AgreementTimeout <= 0 || config.Metadata.AgreementPoll <= 0 {
		return cqltype.TypeValidation("schema agreement", "timeout and poll must be positive")
	}
	if config.Metadata.ServerTimeout.Set && config.Metadata.ServerTimeout.Value > 0 && config.Metadata.ServerTimeout.Value%time.Millisecond != 0 {
		return cqltype.TypeValidation("metadata server timeout", "must use whole milliseconds")
	}
	if config.Metadata.Mode > MetadataFull {
		return cqltype.TypeValidation("metadata mode", "unknown mode")
	}
	if config.Metadata.PeriodicRefresh < 0 || config.Metadata.RequestTimeout < 0 || config.Metadata.AgreementTimeout < 0 || config.Metadata.AgreementPoll < 0 || config.Metadata.ServerTimeout.Set && config.Metadata.ServerTimeout.Value < 0 {
		return cqltype.TypeValidation("metadata timeouts", "must not be negative")
	}
	if routes := config.ClientRoutes; routes != nil {
		if len(routes.ConnectionIDs) == 0 {
			return cqltype.TypeValidation("client routes", "connection IDs are required")
		}
		ids := make(map[string]bool, len(routes.ConnectionIDs))
		for _, id := range routes.ConnectionIDs {
			if id == "" || ids[id] {
				return cqltype.TypeValidation("client routes", "empty or duplicate connection ID")
			}
			ids[id] = true
		}
		for id, host := range routes.HostnameOverrides {
			if !ids[id] || host == "" {
				return cqltype.TypeValidation("client routes", "override requires configured connection ID and hostname")
			}
		}
		if config.AddressTranslator != nil || config.Pool.ShardAwarePort {
			return cqltype.TypeValidation("client routes", "user address translator and advanced shard awareness are incompatible")
		}
		if err := ValidateClientRoutesTLS(routes, config.TLS); err != nil {
			return err
		}
	}
	keys := make(map[string]bool, len(config.Metadata.Keyspaces))
	for _, keyspace := range config.Metadata.Keyspaces {
		if keyspace == "" || keys[keyspace] {
			return cqltype.TypeValidation("metadata keyspaces", "empty or duplicate keyspace")
		}
		keys[keyspace] = true
	}
	for name, partitioner := range config.Partitioners {
		if name == "" || partitioner == nil {
			return cqltype.TypeValidation("partitioners", "empty name or nil partitioner")
		}
	}
	if config.PreparedCacheSize < 0 {
		return cqltype.TypeValidation("prepared cache", "must not be negative")
	}
	if config.Pool.ConnectionsPerHost < 0 || config.Pool.ConnectionsPerShard < 0 || config.Pool.MaxExcessPerShard < 0 || config.Pool.MaxRequestsPerConnection <= 0 || config.Pool.MaxRequestsPerConnection > 32768 || config.Pool.MaxOrphanedStreams < 0 {
		return cqltype.TypeValidation("pool", "invalid pool limits")
	}
	if config.Pool.ConnectionsPerHost == 0 && config.Pool.ConnectionsPerShard == 0 {
		return cqltype.TypeValidation("pool", "host or shard connection count must be positive")
	}
	if config.Pool.ShardAwarePort && (config.Pool.SourcePortStart < 1024 || config.Pool.SourcePortStart > config.Pool.SourcePortEnd) {
		return cqltype.TypeValidation("source ports", "invalid range")
	}
	return nil
}
func InternalSnapshotConfig(config Config) Config {
	if config.DefaultProfile != nil {
		profile := *config.DefaultProfile
		config.DefaultProfile = &profile
	}
	config.ContactPoints = append([]string(nil), config.ContactPoints...)
	config.Metadata.Keyspaces = append([]string(nil), config.Metadata.Keyspaces...)
	if config.TLS != nil {
		config.TLS = InternalCloneV2TLSConfig(config.TLS)
	}
	if config.ClientRoutes != nil {
		routes := *config.ClientRoutes
		routes.ConnectionIDs = append([]string(nil), routes.ConnectionIDs...)
		routes.HostnameOverrides = maps.Clone(routes.HostnameOverrides)
		config.ClientRoutes = &routes
	}
	if config.Partitioners != nil {
		registry := make(map[string]routing.TokenPartitioner, len(config.Partitioners))
		for key, value := range config.Partitioners {
			registry[key] = value
		}
		config.Partitioners = registry
	}
	return config
}

type defaultReconnectPolicy struct{}

func (defaultReconnectPolicy) Delay(attempt int) (time.Duration, bool) {
	delay := time.Second << min(max(attempt, 0), 5)
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2))), true
}
