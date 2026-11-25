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
	"strconv"
	"strings"
	"time"

	"log/slog"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	routing2 "github.com/scylladb/gocql/v2/routing"
	// DialHost opens a raw socket using immutable host metadata; TLS is applied afterward.
)

type Config struct {
	LocalAddress      netip.Addr
	Dialer            endpoint.Dialer
	DNSResolver       endpoint.Resolver
	Auth              auth.Provider
	Reconnect         ReconnectPolicy
	Conviction        endpoint.ConvictionPolicy
	AddressTranslator endpoint.Translator
	Observer          observe.Observer
	Logger            *slog.Logger
	DialHost          func(context.Context, endpoint.DialTarget) (net.Conn, error)
	TLS               *tls.Config
	// TLSSource, when set, supplies TLS for every dial; TLS then holds its
	// config at Build, for validation.
	TLSSource      TLSSource
	HostFilter     func(metadata.HostSnapshot) bool
	ClientRoutes   *ClientRoutesOptions
	DefaultProfile *Profile
	Partitioners   map[string]routing2.TokenPartitioner
	// Timestamps is the source of client timestamps. Its zero value sends
	// none, so the server assigns them; DefaultConfig sets DriverTimestamps.
	Timestamps           ClientTimestamps
	Identity             IdentityOptions
	Keyspace, CQLVersion string
	// LocalDatacenter and LocalRack are the session's locality, which every
	// built-in routing policy without a datacenter of its own prefers.
	LocalDatacenter, LocalRack string
	ContactPoints              []string
	Metadata                   MetadataOptions
	Pool                       PoolOptions
	Limits                     ProtocolLimits
	// ReceiveBuffer and SendBuffer are SO_RCVBUF and SO_SNDBUF in bytes;
	// zero keeps the OS's.
	ReceiveBuffer, SendBuffer int
	Linger                    cql.Option[time.Duration]
	ReadTimeout               time.Duration
	// ReconnectFirst is Reconnect's Delay(1), which ProbeReconnect takes
	// once so the reconnection loop does not ask again.
	ReconnectFirst ReconnectProbe
	// Protocol is the native protocol version, 4 or 5, and Server the
	// database the session expects. Connect sets both from its dialect.
	Protocol                                          int
	ConnectTimeout, DNSTimeout                        time.Duration
	WriteTimeout, KeepaliveInterval, KeepaliveTimeout time.Duration
	SocketKeepalive                                   time.Duration
	PreparedCacheSize                                 int
	Port                                              uint16
	NoDelay                                           bool
	// ChosenSocketOptions are the socket options a setter chose. The others
	// are applied best-effort.
	ChosenSocketOptions connectivity.SocketOptions
	Compression         cql.Compression
	ReportConfiguration bool
	Server              Server
	// ErrorStatementBytes is how much of a statement's CQL text request
	// errors carry; zero leaves it out.
	ErrorStatementBytes int
}

type PoolOptions = connectivity.PoolOptions
type AdvancedShardAwarenessOptions = connectivity.AdvancedShardAwarenessOptions
type ReconnectPolicy = endpoint.ReconnectPolicy
type ProtocolLimits = connectivity.ProtocolLimits
type MetadataOptions struct {
	Keyspaces                                                     []string
	ServerTimeout                                                 cql.Option[time.Duration]
	PeriodicRefresh                                               time.Duration
	RequestTimeout                                                time.Duration
	AgreementTimeout, AgreementPoll                               time.Duration
	Mode                                                          metadata.Mode
	InitialDiscovery                                              bool
	AutomaticAgreement, RefreshAfterAgreement                     bool
	TopologyEvents, StatusEvents, SchemaEvents, ClientRouteEvents bool
}

type IdentityOptions struct{ DriverName, DriverVersion, ApplicationName, ApplicationVersion, ClientID string }
type ClientRoutesOptions = connectivity.ClientRoutesOptions
type SocketOptions = connectivity.SocketOptions

const (
	SocketNoDelay   = connectivity.SocketNoDelay
	SocketKeepalive = connectivity.SocketKeepalive
)

type TLSSource = endpoint.TLSSource

func DefaultConfig(contactPoints ...string) Config {
	profile := InternalDefaultProfile()
	profile.Routing = DefaultRouting()
	return Config{ContactPoints: append([]string(nil), contactPoints...), Port: 9042, Protocol: 4, CQLVersion: "3.0.0", ConnectTimeout: 5 * time.Second, DNSTimeout: 5 * time.Second, KeepaliveInterval: 30 * time.Second, KeepaliveTimeout: 30 * time.Second, NoDelay: true, Pool: PoolOptions{ConnectionsPerShard: 1, MaxExcessPerShard: 2, MaxRequestsPerConnection: 32768, MaxOrphanedStreams: 1024, AdvancedShardAwareness: AdvancedShardAwarenessOptions{Enabled: true, SourcePortStart: 49152, SourcePortEnd: 65535}}, Metadata: MetadataOptions{Mode: metadata.ModeFull, InitialDiscovery: true, PeriodicRefresh: 60 * time.Second, RequestTimeout: 31 * time.Second, AgreementTimeout: 60 * time.Second, AgreementPoll: 200 * time.Millisecond, AutomaticAgreement: true, RefreshAfterAgreement: true, TopologyEvents: true, StatusEvents: true, SchemaEvents: true, ClientRouteEvents: true}, Limits: ProtocolLimits{MaxFrameBytes: 256 << 20, MaxDecompressedBytes: 256 << 20, MaxMetadataDepth: 64, MaxPagingStates: 65536, MaxPagingStateBytes: 16 << 20}, PreparedCacheSize: 1000, Partitioners: map[string]routing2.TokenPartitioner{"org.apache.cassandra.dht.Murmur3Partitioner": routing2.Murmur3TokenPartitioner, "Murmur3Partitioner": routing2.Murmur3TokenPartitioner, "com.scylladb.dht.CDCPartitioner": routing2.CDCTokenPartitioner, "CDCPartitioner": routing2.CDCTokenPartitioner}, DefaultProfile: &profile, Identity: IdentityOptions{DriverName: InternalDefaultDriverName, DriverVersion: InternalDefaultDriverVersion}, Reconnect: defaultReconnectPolicy{}, ReportConfiguration: true, Timestamps: DriverTimestamps()}
}

// TimestampMode is where client timestamps come from.
type TimestampMode uint8

const (
	// ServerTimestampMode sends no client timestamp; the server assigns one.
	ServerTimestampMode TimestampMode = iota
	// DriverTimestampMode takes each timestamp from the time the session
	// admitted the request, kept strictly increasing per session.
	DriverTimestampMode
	// GeneratedTimestampMode calls a user generator for each timestamp.
	GeneratedTimestampMode
)

// ClientTimestamps selects the client timestamp source. Its fields are set
// only by its constructors, so a generator and its mode always agree.
type ClientTimestamps struct {
	next func() int64
	mode TimestampMode
}

// DriverTimestamps is the default source: microseconds since the epoch at
// admission, strictly increasing per session.
func DriverTimestamps() ClientTimestamps { return ClientTimestamps{mode: DriverTimestampMode} }

// GeneratedTimestamps calls next for each timestamp; a nil next sends none.
func GeneratedTimestamps(next func() int64) ClientTimestamps {
	if next == nil {
		return ClientTimestamps{}
	}
	return ClientTimestamps{next: next, mode: GeneratedTimestampMode}
}

// Mode reports the source.
func (c ClientTimestamps) Mode() TimestampMode { return c.mode }

// Generator returns the user generator of GeneratedTimestampMode, else nil.
func (c ClientTimestamps) Generator() func() int64 { return c.next }

// Validate checks the final settings. A cql.ValidationError names, in its
// Field, the config.ConfigBuilder setter that chose the invalid value, and
// says the limit it broke.
func (config Config) Validate() error {
	if _, err := NewObserverHooks(config.Observer); err != nil {
		return err
	}
	path, err := config.UnixContact()
	if err != nil {
		return err
	}
	if len(config.ContactPoints) == 0 {
		return cqltype.TypeValidation("WithContactPoints", "at least one contact point is required")
	}
	if config.Protocol != 4 && config.Protocol != 5 {
		return cqltype.TypeValidation("New", "supported protocol versions are 4 and 5")
	}
	if config.Server == ServerScylla && config.Protocol == 5 {
		return &UnsupportedSettingError{Setting: "protocol", Reason: "ScyllaDB speaks native protocol 4"}
	}
	if config.ClientRoutes != nil {
		if err := ScyllaOnly(&config, "WithClientRoutes"); err != nil {
			return err
		}
	}
	if config.Metadata.ServerTimeout.Set && config.Metadata.ServerTimeout.Value > 0 {
		if err := ScyllaOnly(&config, "WithMetadataServerTimeout"); err != nil {
			return err
		}
	}
	if config.LocalRack != "" && config.LocalDatacenter == "" {
		return cqltype.TypeValidation("WithLocalDatacenter", "a rack requires a datacenter")
	}
	if config.Port == 0 {
		return cqltype.TypeValidation("WithPort", "must be 1 to 65535")
	}
	if path == "" {
		if err := config.validateContactPorts(); err != nil {
			return err
		}
	}
	if config.CQLVersion == "" {
		return cqltype.TypeValidation("WithCQLVersion", "must not be empty")
	}
	for _, d := range []struct {
		setter string
		value  time.Duration
	}{
		{"WithReadTimeout", config.ReadTimeout},
		{"WithConnectTimeout", config.ConnectTimeout},
		{"WithDNSTimeout", config.DNSTimeout},
		{"WithWriteTimeout", config.WriteTimeout},
		{"WithHeartbeat", config.KeepaliveInterval},
		{"WithHeartbeat", config.KeepaliveTimeout},
		{"WithTCPKeepalive", config.SocketKeepalive},
		{"WithMetadataRefresh", config.Metadata.PeriodicRefresh},
		{"WithMetadataTimeout", config.Metadata.RequestTimeout},
	} {
		if d.value < 0 {
			return cqltype.TypeValidation(d.setter, "must not be negative, got %v", d.value)
		}
	}
	for _, d := range []struct {
		setter string
		value  time.Duration
	}{
		{"WithConnectTimeout", config.ConnectTimeout},
		{"WithDNSTimeout", config.DNSTimeout},
		{"WithMetadataTimeout", config.Metadata.RequestTimeout},
	} {
		if d.value == 0 {
			return cqltype.TypeValidation(d.setter, "must be positive, got 0")
		}
	}
	if config.KeepaliveInterval > 0 && config.KeepaliveTimeout == 0 {
		return cqltype.TypeValidation("WithHeartbeat", "timeout must be positive when the interval is")
	}
	if config.Protocol == 5 && config.Compression == cql.SnappyCompression {
		return &UnsupportedSettingError{Setting: "WithCompression", Reason: "native protocol 5 supports raw or LZ4 segments"}
	}
	if config.Compression > cql.SnappyCompression {
		return cqltype.TypeValidation("WithCompression", "unknown algorithm %d", config.Compression)
	}
	if config.DefaultProfile == nil {
		return cqltype.TypeValidation("WithDefaultProfile", "default profile is required; use DefaultConfig")
	}
	if err := (*config.DefaultProfile).ValidateProfile(); err != nil {
		return err
	}
	const maxFrame = 1<<31 - 1
	for _, l := range []struct {
		setter    string
		value     int64
		max       int64
		unitBytes bool
	}{
		{"WithMaxFrameSize", config.Limits.MaxFrameBytes, maxFrame, true},
		{"WithMaxDecompressedSize", config.Limits.MaxDecompressedBytes, maxFrame, true},
		{"WithMaxMetadataDepth", int64(config.Limits.MaxMetadataDepth), 128, false},
		{"WithMaxPagingStates", int64(config.Limits.MaxPagingStates), math.MaxInt, false},
		{"WithMaxPagingStateSize", config.Limits.MaxPagingStateBytes, math.MaxInt64, true},
	} {
		if l.value <= 0 || l.value > l.max {
			if l.max >= math.MaxInt {
				return cqltype.TypeValidation(l.setter, "must be positive, got %d", l.value)
			}
			unit := ""
			if l.unitBytes {
				unit = " bytes"
			}
			return cqltype.TypeValidation(l.setter, "must be 1 to %d%s, got %d", l.max, unit, l.value)
		}
	}
	if config.Linger.Set && (config.Linger.Value < 0 || config.Linger.Value%time.Second != 0 || config.Linger.Value/time.Second > math.MaxInt32) {
		return cqltype.TypeValidation("WithLinger", "must be whole seconds from 0 to %d, got %v", math.MaxInt32, config.Linger.Value)
	}
	if config.ReceiveBuffer < 0 {
		return cqltype.TypeValidation("WithReceiveBuffer", "must not be negative, got %d", config.ReceiveBuffer)
	}
	if config.SendBuffer < 0 {
		return cqltype.TypeValidation("WithSendBuffer", "must not be negative, got %d", config.SendBuffer)
	}
	if config.Metadata.AgreementTimeout <= 0 || config.Metadata.AgreementPoll <= 0 {
		return cqltype.TypeValidation("WithSchemaAgreement", "poll and timeout must be positive")
	}
	if timeout := config.Metadata.ServerTimeout; timeout.Set && (timeout.Value < 0 || timeout.Value%time.Millisecond != 0) {
		return cqltype.TypeValidation("WithMetadataServerTimeout", "must be whole milliseconds, not negative, got %v", timeout.Value)
	}
	if timeout := config.Metadata.ServerTimeout; timeout.Set && timeout.Value > 0 && timeout.Value >= config.Metadata.RequestTimeout {
		return cqltype.TypeValidation("WithMetadataServerTimeout", "must be below WithMetadataTimeout (%v), so the server gives up first, got %v", config.Metadata.RequestTimeout, timeout.Value)
	}
	if config.Metadata.Mode > metadata.ModeFull {
		return cqltype.TypeValidation("WithMetadataMode", "unknown mode %d", config.Metadata.Mode)
	}
	if config.Metadata.Mode == metadata.ModeNone && len(config.Metadata.Keyspaces) > 0 {
		return cqltype.TypeValidation("WithMetadataKeyspaces", "has no effect under metadata.ModeNone, which loads no schema")
	}
	if routes := config.ClientRoutes; routes != nil {
		if len(routes.ConnectionIDs) == 0 {
			return cqltype.TypeValidation("WithClientRoutes", "connection IDs are required")
		}
		ids := make(map[string]bool, len(routes.ConnectionIDs))
		for _, id := range routes.ConnectionIDs {
			if id == "" {
				return cqltype.TypeValidation("WithClientRoutes", "empty connection ID")
			}
			if ids[id] {
				return cqltype.TypeValidation("WithClientRoutes", "connection ID %q repeated", id)
			}
			ids[id] = true
		}
		for id, host := range routes.HostnameOverrides {
			if !ids[id] || host == "" {
				return cqltype.TypeValidation("WithClientRoutes", "override requires configured connection ID and hostname")
			}
		}
		if config.AddressTranslator != nil {
			return cqltype.TypeValidation("WithClientRoutes", "translates addresses itself; drop WithAddressTranslator")
		}
	}
	keys := make(map[string]bool, len(config.Metadata.Keyspaces))
	for _, keyspace := range config.Metadata.Keyspaces {
		if keyspace == "" || keys[keyspace] {
			return cqltype.TypeValidation("WithMetadataKeyspaces", "empty or repeated keyspace %q", keyspace)
		}
		keys[keyspace] = true
	}
	for name, partitioner := range config.Partitioners {
		if name == "" || partitioner == nil {
			return cqltype.TypeValidation("WithPartitioner", "empty name or nil partitioner")
		}
	}
	if config.PreparedCacheSize < 0 {
		return cqltype.TypeValidation("WithPreparedCacheSize", "must not be negative, got %d", config.PreparedCacheSize)
	}
	if config.ErrorStatementBytes < 0 {
		return cqltype.TypeValidation("WithErrorStatementText", "must not be negative, got %d", config.ErrorStatementBytes)
	}
	pool := config.Pool
	switch {
	case pool.ConnectionsPerHost < 0:
		return cqltype.TypeValidation("WithConnectionsPerHost", "must be positive, got %d", pool.ConnectionsPerHost)
	case pool.ConnectionsPerShard < 0:
		return cqltype.TypeValidation("WithConnectionsPerShard", "must be positive, got %d", pool.ConnectionsPerShard)
	case pool.ConnectionsPerHost == 0 && pool.ConnectionsPerShard == 0:
		return cqltype.TypeValidation("WithConnectionsPerShard", "WithConnectionsPerShard or WithConnectionsPerHost must be positive")
	case pool.MaxExcessPerShard < 0:
		return cqltype.TypeValidation("WithMaxExcessPerShard", "must not be negative, got %d", pool.MaxExcessPerShard)
	case pool.MaxRequestsPerConnection <= 0 || pool.MaxRequestsPerConnection > 32768:
		return cqltype.TypeValidation("WithMaxRequestsPerConnection", "must be 1 to 32768, got %d", pool.MaxRequestsPerConnection)
	case pool.MaxOrphanedStreams < 0:
		return cqltype.TypeValidation("WithMaxOrphanedStreams", "must not be negative, got %d", pool.MaxOrphanedStreams)
	}
	if sa := pool.AdvancedShardAwareness; sa.Enabled && (sa.SourcePortStart < 1024 || sa.SourcePortStart > sa.SourcePortEnd) {
		return cqltype.TypeValidation("WithShardAwareSourcePorts", "%d-%d is not a range within 1024-65535", sa.SourcePortStart, sa.SourcePortEnd)
	}
	return nil
}

// validateContactPorts parses the TCP contact points, host[:port], and
// rejects contact points that all name one port other than Port while the
// driver dials discovered nodes on Port.
func (config Config) validateContactPorts() error {
	common, allNamed := 0, true
	for _, point := range config.ContactPoints {
		host, portText, err := net.SplitHostPort(point)
		if err != nil {
			// No port: a host name or address, an IPv6 address unbracketed.
			if _, ipErr := netip.ParseAddr(point); ipErr != nil && (point == "" || strings.ContainsAny(point, ":[]")) {
				return cqltype.TypeValidation("WithContactPoints", "%q is not host or host:port", point)
			}
			allNamed = false
			continue
		}
		port, err := strconv.Atoi(portText)
		if host == "" || err != nil || port < 1 || port > 65535 {
			return cqltype.TypeValidation("WithContactPoints", "%q is not host or host:port with a port from 1 to 65535", point)
		}
		if common == 0 {
			common = port
		} else if common != port {
			common = -1
		}
	}
	if allNamed && common > 0 && common != int(config.Port) && config.Metadata.InitialDiscovery && config.ClientRoutes == nil {
		return cqltype.TypeValidation("WithPort", "is %d, but every contact point names port %d; discovered nodes are dialed on WithPort, so set WithPort(%d) or name no port", config.Port, common, common)
	}
	return nil
}

// CommonContactPort returns the port every TCP contact point names, and
// false when one names none, two name different ports, or one does not
// parse, which Validate reports.
func CommonContactPort(contactPoints []string) (uint16, bool) {
	var common uint16
	for _, point := range contactPoints {
		_, portText, err := net.SplitHostPort(point)
		if err != nil {
			return 0, false
		}
		port, err := strconv.ParseUint(portText, 10, 16)
		if err != nil || port == 0 || common != 0 && uint16(port) != common {
			return 0, false
		}
		common = uint16(port)
	}
	return common, common != 0
}

func (config Config) Snapshot() Config {
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
		registry := make(map[string]routing2.TokenPartitioner, len(config.Partitioners))
		for key, value := range config.Partitioners {
			registry[key] = value
		}
		config.Partitioners = registry
	}
	return config
}

type defaultReconnectPolicy struct{}

// DefaultReconnect returns the reconnect policy DefaultConfig installs.
func DefaultReconnect() ReconnectPolicy { return defaultReconnectPolicy{} }

// DefaultRouting returns a new routing policy of the default profile.
func DefaultRouting() routing.RoutingPolicy {
	p, _ := routing.NewRoutingPolicy(routing2.Options{})
	return p
}

// ReconnectProbe is a reconnect policy's answer for the first attempt.
type ReconnectProbe struct {
	Delay time.Duration
	Again bool
	// Set reports that ProbeReconnect filled it.
	Set bool
}

// ProbeReconnect asks Reconnect for its first delay, once: a later call
// keeps the answer. A negative delay is a validation error.
func (config *Config) ProbeReconnect() error {
	if config.Reconnect == nil || config.ReconnectFirst.Set {
		return nil
	}
	delay, again := config.Reconnect.Delay(1)
	if delay < 0 {
		return cqltype.TypeValidation("WithReconnect", "Delay(1) returned a negative delay, %v", delay)
	}
	config.ReconnectFirst = ReconnectProbe{Delay: delay, Again: again, Set: true}
	return nil
}

// Default reconnect delays: the first attempt waits up to
// DefaultReconnectBase and the delay doubles up to DefaultReconnectMax, each
// jittered down by up to half.
const (
	DefaultReconnectBase = time.Second
	DefaultReconnectMax  = 32 * time.Second
)

// IsDefaultReconnect reports whether p is the policy DefaultConfig installs.
func IsDefaultReconnect(p ReconnectPolicy) bool {
	_, ok := p.(defaultReconnectPolicy)
	return ok
}

func (defaultReconnectPolicy) Delay(attempt int) (time.Duration, bool) {
	delay := DefaultReconnectBase << min(max(attempt-1, 0), 5) // up to DefaultReconnectMax
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2))), true
}
