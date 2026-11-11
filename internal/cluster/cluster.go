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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// PoolConfig configures the connection pool used by the driver, it defaults to
// using a round-robin host selection policy and a round-robin connection selection
// policy for each host.
type PoolConfig struct {
	// HostSelectionPolicy sets the policy for selecting which host to use for a
	// given query (default: RoundRobinHostPolicy())
	// It is not supported to use a single HostSelectionPolicy in multiple sessions
	// (even if you close the old session before using in a new session).
	HostSelectionPolicy HostSelectionPolicy
}

func (p PoolConfig) buildPool(session *Core) *pool.InternalPolicyConnPool {
	return newPolicyConnPool(session)
}

// clusterSettings is a struct to configure the default cluster implementation
// of gocql. It has a variety of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that require a
// different setup must implement their own
type InternalClusterSettings struct {
	// BatchObserver will set the provided batch observer on all queries created from this session.
	// Use it to collect metrics / stats from batch queries by providing an implementation of BatchObserver.
	// Dialer will be used to establish all connections created for this Cluster.
	// If not provided, a default dialer configured with ConnectTimeout will be used.
	// Dialer is ignored if HostDialer is provided.
	Dialer endpoint.InternalLegacyDialer
	// ApplicationInfo reports application information to the server by inserting it into options of the STARTUP frame
	ApplicationInfo endpoint.ApplicationInfo
	// DNSResolver Resolves DNS names to IP addresses
	DNSResolver endpoint.DNSResolver
	// Logger for this clusterSettings.
	// If not specified, defaults to the gocql.defaultLogger.
	Logger *slog.Logger
	// HostDialer will be used to establish all connections for this Cluster.
	// Unlike Dialer, HostDialer is responsible for setting up the entire connection, including the TLS session.
	// To support shard-aware port, HostDialer should implement ShardDialer.
	// If not provided, Dialer will be used instead.
	HostDialer endpoint.HostDialer
	// StreamObserver will be notified of stream state changes.
	// This can be used to track in-flight protocol requests and responses.
	StreamObserver transport.StreamObserver
	// FrameHeaderObserver will set the provided frame header observer on all frames' headers created from this session.
	// Use it to collect metrics / stats from frames by providing an implementation of FrameHeaderObserver.
	FrameHeaderObserver transport.FrameHeaderObserver
	// ConnectObserver will set the provided connect observer on all queries
	// created from this session.
	ConnectObserver ConnectObserver
	// QueryObserver will set the provided query observer on all queries created from this session.
	// Use it to collect metrics / stats from queries by providing an implementation of QueryObserver.
	// AddressTranslator will translate addresses found on peer discovery and/or
	// node change events.
	AddressTranslator endpoint.AddressTranslator
	// HostFilter will filter all incoming events for host, any which don't pass
	// the filter will be ignored. If set will take precedence over any options set
	// via Discovery
	HostFilter HostFilter
	// Compression algorithm.
	// Default: nil
	Compressor protocol.Compressor
	// Default: nil
	Authenticator transport.Authenticator
	// PoolConfig configures the underlying connection pool, allowing the
	// configuration of host selection and connection selection policies.
	PoolConfig PoolConfig
	// Default retry policy to use for queries.
	// Default: SimpleRetryPolicy{NumRetries: 3}.
	// ConvictionPolicy decides whether to mark host as down based on the error and host info.
	// Default: SimpleConvictionPolicy
	ConvictionPolicy settings.ConvictionPolicy
	// Default reconnection policy to use for reconnecting before trying to mark host as down.
	ReconnectionPolicy pool.ReconnectionPolicy
	// A reconnection policy to use for reconnecting when connecting to the cluster first time.
	InitialReconnectionPolicy pool.ReconnectionPolicy
	// SslOpts configures TLS use when HostDialer is not set.
	// SslOpts is ignored if HostDialer is set.
	SslOpts *transport.SslOptions
	// An Authenticator factory. Can be used to create alternative authenticators.
	// Default: nil
	AuthProvider       func(h *endpoint.HostInfo) (transport.Authenticator, error)
	ClientRoutesConfig *ClientRoutesConfig
	// The version of the driver that is going to be reported to the server.
	// Defaulted to current library version
	DriverVersion string
	// The name of the driver that is going to be reported to the server.
	// Default: "ScyllaDB GoLang Driver"
	DriverName string
	// Initial keyspace. Optional.
	Keyspace string
	// CQL version (default: 3.0.0)
	CQLVersion string
	// Addresses for the initial connections. It is recommended to use the value set in
	// the Cassandra config for broadcast_address or listen_address, an IP address not
	// a domain name. This is because events from Cassandra will use the configured IP
	// address, which is used to index connected hosts. If the domain name specified
	// resolves to more than 1 IP address then the driver may connect multiple times to
	// the same host, and will not mark the node being down or up from events.
	//
	// Each entry may optionally include a port in host:port format. When cfg.Port
	// is 0 (not explicitly set) and ClientRoutesConfig is nil, the driver learns
	// the port from these entries (see cfg.Port for details). When cfg.Port is
	// set to a non-zero value, embedded ports in host strings are ignored for
	// the purpose of peer discovery.
	Hosts          []string
	privateStorage clusterSettingsStorage
	// The time to wait for frames before flushing the frames connection to Cassandra.
	// Can help reduce syscall overhead by making less calls to write. Set to 0 to
	// disable.
	//
	// (default: 200 microseconds)
	WriteCoalesceWaitTime time.Duration
	// WriteTimeout limits the time the driver waits to write a request to a network connection.
	// WriteTimeout should be lower than or equal to Timeout.
	// WriteTimeout defaults to the value of Timeout.
	WriteTimeout time.Duration
	// The keepalive period to use, enabled if > 0 (default: 15 seconds)
	// SocketKeepalive is used to set up the default dialer and is ignored if Dialer or HostDialer is provided.
	SocketKeepalive time.Duration
	// If not zero, gocql attempt to reconnect known DOWN nodes in every ReconnectInterval.
	ReconnectInterval time.Duration
	// The maximum amount of time to wait for schema agreement in a cluster after
	// receiving a schema change frame. (default: 60s)
	MaxWaitSchemaAgreement time.Duration
	// ProtoVersion sets the version of the native protocol to use, this will
	// enable features in the driver for specific protocol versions, generally this
	// should be set to a supported version (3,4,5) for the cluster being connected to.
	//
	// If it is 0 or unset (the default) then the driver will attempt to discover the
	// highest supported protocol up to version 4. Protocol version 5 must currently
	// be selected explicitly. In clusters with nodes of different versions the protocol
	// selected is not defined (ie, it can be any of the supported in the cluster).
	ProtoVersion int
	// Maximum number of inflight requests allowed per connection.
	// Default: 32768 for CQL v3 and newer
	// Default: 128 for older CQL versions
	MaxRequestsPerConn int
	// Timeout defines the maximum time to wait for a single server response.
	// The default is 11 seconds, which is slightly higher than the default
	// server-side timeout for most query types.
	//
	// When a session creates a Query or Batch, it inherits this timeout as
	// the request timeout.
	//
	// Important notes:
	// 1. This value should be greater than the server timeout for all queries
	//    you execute. Otherwise, you risk creating retry storms: the server
	//    may still be processing the request while the client times out and retries.
	// 2. This timeout does not apply during initial connection setup.
	//    For that, see ConnectTimeout.
	Timeout time.Duration
	// The timeout for the requests to the schema tables. (default: 60s)
	//
	// Zero means the driver does not bound these queries itself: it arms no
	// client-side deadline, and against ScyllaDB it sends them without the
	// " USING TIMEOUT ...ms" override, so how long they may run is left to the
	// server's own configuration - its request timeouts, or the service level in
	// force. Nothing else on the driver side steps in: controlConn.runQuery, which
	// every schema-table read goes through, passes a context with no deadline, and
	// the connection's read deadline is disarmed while waiting for a response to
	// begin. Schema agreement is the exception, bounding its own queries by
	// MaxWaitSchemaAgreement. Set a positive value to keep these queries under a
	// deadline of the client's choosing.
	MetadataSchemaRequestTimeout time.Duration
	// ConnectTimeout limits the time spent during connection setup.
	// During initial connection setup, internal queries, AUTH requests will return an error if the client
	// does not receive a response within the ConnectTimeout period.
	// ConnectTimeout is applied to the connection setup queries independently.
	// ConnectTimeout also limits the duration of dialing a new TCP connection
	// in case there is no Dialer nor HostDialer configured.
	// ConnectTimeout has a default value of 11 seconds.
	ConnectTimeout time.Duration
	// Port used when dialing.
	//
	// When left at 0 (the zero value, i.e. not explicitly set) and
	// ClientRoutesConfig is nil, the driver attempts to learn the port from
	// cfg.Hosts: if every host entry embeds the same port, that port is used;
	// if no entry embeds a port, the default 9042 is used; any inconsistency
	// (some entries have a port, some don't, or they differ) is an error.
	//
	// When set to a non-zero value the driver uses that port for all
	// connections — including peer-discovered nodes — and never learns from
	// cfg.Hosts. Set to 9042 explicitly if you want the default port and
	// also want to prevent any learning from host strings.
	//
	// Default: 0 (auto-detect; effectively 9042 when no port is in Hosts)
	Port int
	// The size of the connection pool for each host.
	// The pool filling runs in separate gourutine during the session initialization phase.
	// gocql will always try to get 1 connection on each host pool
	// during session initialization AND it will attempt
	// to fill each pool afterward asynchronously if NumConns > 1.
	// Notice: There is no guarantee that pool filling will be finished in the initialization phase.
	// Also, it describes a maximum number of connections at the same time.
	// Default: 2
	NumConns int
	// Maximum cache size for prepared statements globally for gocql.
	// Default: 1000
	MaxPreparedStmts int
	// Default page size to use for created sessions.
	// Default: 5000
	PageSize int
	// Maximum cache size for query info about statements for each session.
	// Default: 1000
	MaxRoutingKeyInfo int
	// ReadTimeout limits the time the driver waits for data from the connection.
	// It has only one purpose, identify faulty connection early and drop it.
	// Default: 11 Seconds
	ReadTimeout time.Duration
	// An event bus configuration
	EventBusConfig eventbus.EventBusConfig
	// The gocql driver may hold excess shard connections to reuse them when existing connections are dropped.
	// This configuration variable defines the limit for such excess connections. Once the limit is reached,
	// gocql starts dropping any additional excess connections.
	// The limit is computed as `MaxExcessShardConnectionsRate` * <number_of_shards>.
	MaxExcessShardConnectionsRate float32
	// Consistency for the serial part of queries, values can be either SERIAL or LOCAL_SERIAL.
	// Default: unset
	SerialConsistency cqltype.Consistency
	// Default consistency level.
	// Default: Quorum
	Consistency cqltype.Consistency
	// Configure events the driver will register for
	Events struct {
		// disable registering for status events (node up/down)
		DisableNodeStatusEvents bool
		// disable registering for topology events (node added/removed/moved)
		DisableTopologyEvents bool
		// disable registering for schema events (keyspace/table/function removed/created/updated)
		DisableSchemaEvents bool
	}
	// Default idempotence for queries
	DefaultIdempotence bool
	// Sends a client side timestamp for all requests which overrides the timestamp at which it arrives at the server.
	// Default: true, only enabled for protocol 3 and above.
	DefaultTimestamp bool
	// DisableSkipMetadata will override the internal result metadata cache so that the driver does not
	// send skip_metadata for queries, this means that the result will always contain
	// the metadata to parse the rows and will not reuse the metadata from the prepared
	// statement.
	//
	// This flag exists because prepared-statement result metadata could not be
	// invalidated safely: after an ALTER the server kept answering with the old
	// column set, so reusing the cached metadata could misdecode rows. It defaults
	// to true for that reason.
	//
	// This flag is IGNORED — including when it was set to true explicitly — once the
	// connection exchanges result metadata IDs and the driver holds one for the
	// statement. That is the case on native protocol v5, and on protocol v4 once the
	// SCYLLA_USE_METADATA_ID extension is negotiated. Either way the underlying
	// problem is fixed: the server hands out a result metadata ID at prepare time,
	// the driver returns it with every execute, and the server answers a stale ID
	// with METADATA_CHANGED plus fresh metadata. Skipping is then safe, so the driver
	// skips and this workaround no longer applies.
	//
	// This matches scylladb/scylla-drivers#81, which specifies skipping as the safe
	// default "if SCYLLA_USE_METADATA_ID was negotiated or CQL v5 is used", and the
	// Scylla java-driver, which skips for any non-empty result metadata ID. The
	// Scylla python-driver implements the extension half only.
	//
	// There is deliberately no knob to force metadata for a whole session once an ID
	// is in play (the java-driver's skip-cql4-metadata-resolve-method has no
	// equivalent here). Use Query.NoSkipMetadata for a specific query; ScanCAS and
	// MapScanCAS already do so internally.
	//
	// See https://issues.apache.org/jira/browse/CASSANDRA-10786
	// See https://github.com/scylladb/scylladb/issues/20860
	// See https://github.com/scylladb/scylladb/pull/23292
	//
	// Default: true, and has no effect on a connection that exchanges result
	// metadata IDs.
	DisableSkipMetadata bool
	// DisableShardAwarePort disables advanced shard awareness: the driver will not
	// choose a source port and connect through Scylla's shard-aware port to target
	// a desired shard. Per-shard connection pooling and token-to-shard routing remain
	// enabled through the regular CQL port.
	//
	// It is generally recommended to leave this option turned off because gocql can use
	// the shard-aware port to make the process of establishing more robust.
	// However, if you have a cluster with nodes which expose shard-aware port
	// but the port is unreachable due to network configuration issues, you can use
	// this option to work around the issue. Set it to true only if you neither can fix
	// your network nor disable shard-aware port on your nodes.
	DisableShardAwarePort bool
	// If DisableInitialHostLookup then the driver will not attempt to get host info
	// from the system.peers table, this will mean that the driver will connect to
	// hosts supplied and will not attempt to lookup the hosts information, this will
	// mean that data_center, rack and token information will not be available and as
	// such host filtering and token aware query routing will not be available.
	DisableInitialHostLookup bool
	// internal config for testing
	// If IgnorePeerAddr is true and the address in system.peers does not match
	// the supplied host by either initial hosts or discovered via events then the
	// host will be replaced with the supplied address.
	//
	// For example if an event comes in with host=10.0.0.1 but when looking up that
	// address in system.local or system.peers returns 127.0.0.1, the peer will be
	// set to 10.0.0.1 which is what will be used to connect to.
	IgnorePeerAddr bool
	// DisableDriverConfigReporting turns off the driver's self-description to the
	// cluster during connection setup. Reporting is enabled by default so that
	// operators can inspect the settings of a client while investigating an
	// incident.
	//
	// The control connection sends a DRIVER_CONFIG startup option holding a JSON
	// description of the effective configuration. It ends up in the
	// system.clients.client_options column, alongside SESSION_ID, a per-session
	// unique identifier that every connection of a session reports and that is
	// always sent regardless of this setting, letting all of a session's
	// connections be correlated with each other.
	//
	// When set to true, DRIVER_CONFIG is not sent. SESSION_ID is unaffected.
	//
	// Default: false
	DisableDriverConfigReporting bool
}

// newClusterSettings generates a new config for the default cluster implementation.
//
// The supplied hosts are used to initially connect to the cluster then the rest of
// the ring will be automatically discovered. It is recommended to use the value set in
// the Cassandra config for broadcast_address or listen_address, an IP address not
// a domain name. This is because events from Cassandra will use the configured IP
// address, which is used to index connected hosts. If the domain name specified
// resolves to more than 1 IP address then the driver may connect multiple times to
// the same host, and will not mark the node being down or up from events.
func InternalNewClusterSettings(hosts ...string) *InternalClusterSettings {
	logger := slog.Default()
	cfg := &InternalClusterSettings{
		Hosts:                         hosts,
		CQLVersion:                    "3.0.0",
		Timeout:                       11 * time.Second,
		ConnectTimeout:                60 * time.Second,
		ReadTimeout:                   11 * time.Second,
		WriteTimeout:                  11 * time.Second,
		MaxExcessShardConnectionsRate: 2,
		NumConns:                      2,
		Consistency:                   cqltype.Quorum,
		MaxPreparedStmts:              defaultMaxPreparedStmts,
		MaxRoutingKeyInfo:             1000,
		PageSize:                      5000,
		DefaultTimestamp:              true,
		DriverName:                    settings.InternalDefaultDriverName,
		DriverVersion:                 settings.InternalDefaultDriverVersion,
		MaxWaitSchemaAgreement:        60 * time.Second,
		ReconnectInterval:             60 * time.Second,
		ConvictionPolicy:              &settings.SimpleConvictionPolicy{},
		ReconnectionPolicy:            &pool.ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second},
		InitialReconnectionPolicy:     &pool.NoReconnectionPolicy{},
		SocketKeepalive:               15 * time.Second,
		WriteCoalesceWaitTime:         200 * time.Microsecond,
		MetadataSchemaRequestTimeout:  60 * time.Second,
		DisableSkipMetadata:           true,
		Logger:                        logger,
		DNSResolver:                   endpoint.InternalDefaultDnsResolver,
		EventBusConfig: eventbus.EventBusConfig{
			InputEventsQueueSize: 10240,
		},
	}

	return cfg
}

func (cfg *InternalClusterSettings) logger() *slog.Logger {
	if cfg.Logger == nil {
		return slog.Default()
	}
	return cfg.Logger
}

// open initializes the cluster based on this config and returns a
// session object that can be used to interact with the database.
func (cfg *InternalClusterSettings) Open() (*Core, error) {
	return connectSettings(*cfg)
}

func (cfg *InternalClusterSettings) openAsync() (*Core, error) {
	return connectSettingsAsync(*cfg)
}

func (cfg *InternalClusterSettings) FilterHost(host *endpoint.HostInfo) bool {
	return !(cfg.HostFilter == nil || cfg.HostFilter.Accept(host))
}

func (cfg *InternalClusterSettings) ValidateAndInitSSL() error {
	cfg.privateStorage.actualSslOpts = nil
	if cfg.SslOpts == nil {
		return nil
	}
	actualTLSConfig, err := setupTLSConfig(cfg.SslOpts, cfg.logger())
	if err != nil {
		return fmt.Errorf("failed to initialize ssl configuration: %w", err)
	}
	cfg.privateStorage.actualSslOpts = new(atomic.Pointer[tls.Config])
	cfg.privateStorage.actualSslOpts.Store(actualTLSConfig)
	return nil
}

func clusterSettingsGetActualTLSConfig(cfg *InternalClusterSettings) *tls.Config {
	if cfg.privateStorage.actualSslOpts == nil {
		return nil
	}
	val := cfg.privateStorage.actualSslOpts.Load()
	if val == nil {
		return nil
	}
	return val.Clone()
}

type ClusterOption func(*InternalClusterSettings)

func (cfg *InternalClusterSettings) WithOptions(opts ...ClusterOption) *InternalClusterSettings {
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

type ClientRoutesOption func(*ClientRoutesConfig)

// Deprecated: WithMaxResolverConcurrency no longer has any effect
func WithMaxResolverConcurrency(val int) func(*ClientRoutesConfig) {
	return func(cfg *ClientRoutesConfig) {}
}

// Deprecated: WithResolveHealthyEndpointPeriod no longer has any effect
func WithResolveHealthyEndpointPeriod(val time.Duration) func(*ClientRoutesConfig) {
	return func(cfg *ClientRoutesConfig) {}
}

func WithEndpoints(endpoints ...ClientRoutesEndpoint) func(*ClientRoutesConfig) {
	return func(cfg *ClientRoutesConfig) {
		cfg.Endpoints = endpoints
	}
}

func WithTable(tableName string) func(*ClientRoutesConfig) {
	return func(cfg *ClientRoutesConfig) {
		cfg.TableName = tableName
	}
}

// WithShardAwareness controls whether client routes use advanced shard awareness,
// where the driver chooses a source port to target a desired shard. It is disabled
// by default; per-shard pooling and token-to-shard routing remain enabled.
//
// Enable it only when the client-route endpoint forwards to Scylla's Proxy
// Protocol v2 shard-aware CQL listener and the proxy supplies the original client
// source port. clusterSettings.DisableShardAwarePort takes precedence.
func WithShardAwareness(enabled bool) func(*ClientRoutesConfig) {
	return func(cfg *ClientRoutesConfig) {
		cfg.EnableShardAwareness = enabled
	}
}

// WithClientRoutes returns a cluster option that enables routing through
// ScyllaDB Cloud private endpoints using the system.client_routes table.
//
// Configure at least one connection ID with WithEndpoints. If cfg.Hosts is empty
// when this option is applied, non-empty ConnectionAddr values from the endpoints
// are also used as initial contact points.
//
// Advanced shard awareness is disabled by default for client routes because
// private endpoints commonly use NAT. Use WithShardAwareness to opt in only when
// the endpoint targets Scylla's Proxy Protocol v2 shard-aware CQL listener and
// preserves the original client source port. Basic shard awareness remains enabled.
func WithClientRoutes(opts ...ClientRoutesOption) func(*InternalClusterSettings) {
	pmCfg := ClientRoutesConfig{
		TableName: "system.client_routes",
	}
	for _, opt := range opts {
		opt(&pmCfg)
	}
	return func(cfg *InternalClusterSettings) {
		cfg.ClientRoutesConfig = &pmCfg
		if len(cfg.Hosts) == 0 {
			for _, ep := range pmCfg.Endpoints {
				if ep.ConnectionAddr != "" {
					cfg.Hosts = append(cfg.Hosts, ep.ConnectionAddr)
				}
			}
		}
		// TODO: cfg.ControlConnectionOnlyToInitialNodes
	}
}

func (cfg *InternalClusterSettings) applyClientRoutesConfig() {
	if cfg.ClientRoutesConfig != nil && !cfg.ClientRoutesConfig.EnableShardAwareness {
		cfg.DisableShardAwarePort = true
	}
}

func (cfg *InternalClusterSettings) Validate() error {
	if len(cfg.Hosts) == 0 {
		return ErrNoHosts
	}

	if cfg.Authenticator != nil && cfg.AuthProvider != nil {
		return cqltype.TypeValidation("Authenticator", "cannot be combined with AuthProvider")
	}

	if cfg.InitialReconnectionPolicy == nil {
		return cqltype.TypeValidation("InitialReconnectionPolicy", "is nil")
	}

	if cfg.InitialReconnectionPolicy.GetMaxRetries() <= 0 {
		return cqltype.TypeValidation("InitialReconnectionPolicy", "GetMaxRetries must return a positive number")
	}

	if cfg.ReconnectionPolicy == nil {
		return cqltype.TypeValidation("ReconnectionPolicy", "is nil")
	}

	if cfg.ReconnectionPolicy.GetMaxRetries() <= 0 {
		return cqltype.TypeValidation("ReconnectionPolicy", "GetMaxRetries must return a positive number")
	}

	if cfg.PageSize < 0 {
		return cqltype.TypeValidation("PageSize", "must not be negative")
	}

	if cfg.MaxRoutingKeyInfo < 0 {
		return cqltype.TypeValidation("MaxRoutingKeyInfo", "must not be negative")
	}

	if cfg.MaxPreparedStmts < 0 {
		return cqltype.TypeValidation("MaxPreparedStmts", "must not be negative")
	}

	if cfg.SocketKeepalive < 0 {
		return cqltype.TypeValidation("SocketKeepalive", "must not be negative")
	}

	if cfg.MaxRequestsPerConn < 0 || cfg.MaxRequestsPerConn > 32768 {
		return cqltype.TypeValidation("MaxRequestsPerConn", "must be between 0 and 32768")
	}

	if cfg.NumConns < 0 {
		return cqltype.TypeValidation("NumConns", "must not be negative")
	}

	// Resolve cfg.Port: when the user has not set it explicitly (Port == 0),
	// attempt to learn it from the host strings (unless client routes are in
	// use, which manage their own addressing). Fall back to defaultCQLPort when
	// no port is embedded in the host strings.
	if cfg.Port == 0 {
		// When ClientRoutesConfig is set we skip learning from cfg.Hosts because
		// Private Service Connect (PSC) discovery entry points have different
		// ports by design, so a uniform port derived from the initial contact
		// points would be incorrect for peer-discovered nodes.
		if cfg.ClientRoutesConfig == nil {
			learned, err := learnPortFromHosts(cfg.Hosts)
			if err != nil {
				return err
			}
			if learned != 0 {
				cfg.Port = learned
			} else {
				cfg.Port = defaultCQLPort
			}
		} else {
			cfg.Port = defaultCQLPort
		}
	}

	if cfg.Port <= 0 || cfg.Port > 65535 {
		return cqltype.TypeValidation("Port", "must be between 1 and 65535")
	}

	if cfg.WriteTimeout < 0 {
		return cqltype.TypeValidation("WriteTimeout", "must not be negative")
	}

	if cfg.Timeout < 0 {
		return cqltype.TypeValidation("Timeout", "must not be negative")
	}

	if cfg.ConnectTimeout < 0 {
		return cqltype.TypeValidation("ConnectTimeout", "must not be negative")
	}

	if cfg.ReadTimeout < 0 {
		return cqltype.TypeValidation("ReadTimeout", "must not be negative")
	}

	if cfg.MetadataSchemaRequestTimeout < 0 {
		return cqltype.TypeValidation("MetadataSchemaRequestTimeout", "must not be negative")
	}

	if cfg.WriteCoalesceWaitTime < 0 {
		return cqltype.TypeValidation("WriteCoalesceWaitTime", "must not be negative")
	}

	if cfg.ReconnectInterval < 0 {
		return cqltype.TypeValidation("ReconnectInterval", "must not be negative")
	}

	if cfg.MaxWaitSchemaAgreement < 0 {
		return cqltype.TypeValidation("MaxWaitSchemaAgreement", "must not be negative")
	}

	if cfg.ProtoVersion < 0 || cfg.ProtoVersion > 127 {
		return cqltype.TypeValidation("ProtoVersion", "must be between 0 and 127")
	}

	if cfg.ProtoVersion >= protocol.ProtoVersion5 && cfg.Compressor != nil {
		// The native protocol v5 spec allows only lz4 for segment compression.
		// A v5 server still advertises snappy in its OPTIONS response, but STARTUP
		// with COMPRESSION=snappy on v5 is rejected server-side ("Snappy
		// compression is not supported in protocol V5"). We reject any compressor
		// lacking v5 segment support up front to fail fast with a clear message
		// rather than at the first segment.
		//
		// This is a capability check, not a concrete-type check: a Compressor
		// supports v5 segment framing iff it also implements SegmentCompressor.
		// The built-in SnappyCompressor deliberately does not; LZ4Compressor does.
		// A nil compressor (no compression) is always allowed on v5.
		if _, ok := cfg.Compressor.(protocol.SegmentCompressor); !ok {
			return cqltype.TypeValidation("Compressor", "%q does not support protocol v5 segment framing; use LZ4Compressor or no compression with ProtoVersion >= 5", cfg.Compressor.Name())
		}
	}

	if !cfg.DisableSkipMetadata {
		// The hazard this warns about is confined to connections that exchange no
		// result metadata ID: protocol v4 or lower against a server that does not
		// advertise SCYLLA_USE_METADATA_ID. Everywhere else the server reports
		// metadata changes via METADATA_CHANGED, DisableSkipMetadata is ignored, and
		// skipping is both safe and the default — so say which case is actually risky.
		// The protocol version is negotiated per connection, long after Validate runs,
		// so this cannot be narrowed down here.
		cfg.logger().Warn("skipping result metadata can lead to unpredictable results if columns involved in a prepared query are altered, on connections that exchange no result metadata ID (protocol v4 or lower without the SCYLLA_USE_METADATA_ID extension)")
	}

	if cfg.SerialConsistency > 0 && !cfg.SerialConsistency.IsSerial() {
		return cqltype.TypeValidation("SerialConsistency", "must be SERIAL or LOCAL_SERIAL, got %v", cfg.SerialConsistency)
	}

	if cfg.DNSResolver == nil {
		return cqltype.TypeValidation("DNSResolver", "is nil")
	}

	if cfg.MaxExcessShardConnectionsRate < 0 {
		return cqltype.TypeValidation("MaxExcessShardConnectionsRate", "must not be negative")
	}

	if cfg.ClientRoutesConfig != nil {
		if cfg.AddressTranslator != nil {
			return cqltype.TypeValidation("AddressTranslator", "cannot be combined with ClientRoutesConfig")
		}
		if err := cfg.ClientRoutesConfig.Validate(); err != nil {
			return &cqltype.ValidationError{Field: "ClientRoutesConfig", Err: err}
		}
	}

	return cfg.ValidateAndInitSSL()
}

const defaultCQLPort = 9042

var (
	ErrNoHosts              = runtimeerrors.ErrNoHosts
	ErrNoConnectionsStarted = runtimeerrors.ErrNoConnectionsStarted
	ErrHostQueryFailed      = runtimeerrors.ErrHostQueryFailed
)

// learnPortFromHosts inspects the host entries for embedded ports.
// It returns the common port (> 0) if every entry carries the same explicit
// port, 0 if no entry carries a port, or an error as soon as inconsistency
// is detected: some entries have a port while others don't, or two entries
// carry different port values.
func learnPortFromHosts(hosts []string) (int, error) {
	var port int // 0 = not yet seen
	sawPortless := false
	for _, addr := range hosts {
		_, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			// No port in this entry.
			if port != 0 {
				// Already saw a ported entry – return immediately.
				return 0, errors.New(
					"cfg.Hosts entries must either all include a port or none: " +
						"use cfg.Port to set a uniform port instead")
			}
			sawPortless = true
			continue
		}
		p, err := strconv.Atoi(portStr)
		if err != nil || p <= 0 || p > 65535 {
			return 0, fmt.Errorf("invalid port %q in host entry %q: port must be a number between 1 and 65535", portStr, addr)
		}
		if sawPortless {
			// Already saw a portless entry – return immediately.
			return 0, errors.New(
				"cfg.Hosts entries must either all include a port or none: " +
					"use cfg.Port to set a uniform port instead")
		}
		if port == 0 {
			port = p
		} else if port != p {
			return 0, fmt.Errorf(
				"all cfg.Hosts entries must use the same port, got %d and %d: "+
					"use cfg.Port to set a uniform port instead",
				port, p)
		}
	}
	return port, nil
}

func setupTLSConfig(sslOpts *transport.SslOptions, logger *slog.Logger) (*tls.Config, error) {
	//  Config.InsecureSkipVerify | EnableHostVerification | Result
	//  Config is nil             | true                   | verify host
	//  Config is nil             | false                  | do not verify host
	//  false                     | false                  | verify host
	//  true                      | false                  | do not verify host
	//  false                     | true                   | verify host
	//  true                      | true                   | verify host
	var tlsConfig *tls.Config
	if sslOpts.Config == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: !sslOpts.EnableHostVerification,
			// Ticket max size is 16371 bytes, so it can grow up to 16mb max.
			ClientSessionCache: tls.NewLRUClientSessionCache(1024),
		}
	} else {
		// use clone to avoid race.
		tlsConfig = sslOpts.Config.Clone()
	}

	if tlsConfig.InsecureSkipVerify && sslOpts.EnableHostVerification {
		tlsConfig.InsecureSkipVerify = false
	}

	// ca cert is optional
	if sslOpts.CaPath != "" {
		if tlsConfig.RootCAs == nil {
			tlsConfig.RootCAs = x509.NewCertPool()
		}

		pem, err := os.ReadFile(sslOpts.CaPath)
		if err != nil {
			return nil, fmt.Errorf("unable to open CA certs: %w", err)
		}

		if !tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("failed parsing or CA certs")
		}
	}

	if sslOpts.CertPath != "" || sslOpts.KeyPath != "" {
		mycert, err := tls.LoadX509KeyPair(sslOpts.CertPath, sslOpts.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to load X509 key pair: %w", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, mycert)
	}

	// Emit deprecation warning if the option is used
	if sslOpts.DisableStrictCertificateValidation {
		if logger != nil {
			logger.Warn("DisableStrictCertificateValidation is deprecated and will be removed; configure certificate chains for strict validation")
		}
	}

	// Add strict certificate chain validation unless explicitly disabled
	// This ensures that the entire certificate chain is properly validated,
	// not just that one intermediate certificate is trusted.
	if !tlsConfig.InsecureSkipVerify && !sslOpts.DisableStrictCertificateValidation {
		strictVerify := strictVerifyPeerCertificate(tlsConfig.RootCAs)
		existingVerify := tlsConfig.VerifyPeerCertificate
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if err := strictVerify(rawCerts, verifiedChains); err != nil {
				return err
			}
			if existingVerify != nil {
				return existingVerify(rawCerts, verifiedChains)
			}
			return nil
		}
	}

	return tlsConfig, nil
}

// strictVerifyPeerCertificate returns a VerifyPeerCertificate callback that performs
// certificate chain validation by explicitly calling cert.Verify(). This ensures that
// the certificate chain is properly validated against the configured root CAs and
// intermediate certificates, rather than relying on Go's default TLS behavior.
func strictVerifyPeerCertificate(rootCAs *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		chains := verifiedChains
		if len(chains) == 0 {
			if len(rawCerts) == 0 {
				return errors.New("no certificates provided")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("failed to parse certificate: %w", err)
			}
			intermediates := x509.NewCertPool()
			for i := 1; i < len(rawCerts); i++ {
				intermediateCert, err := x509.ParseCertificate(rawCerts[i])
				if err != nil {
					return fmt.Errorf("failed to parse intermediate certificate: %w", err)
				}
				intermediates.AddCert(intermediateCert)
			}
			opts := x509.VerifyOptions{
				Roots:         rootCAs,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}
			chains, err = cert.Verify(opts)
			if err != nil {
				return fmt.Errorf("certificate verification failed for subject=%q, issuer=%q: %w",
					cert.Subject.String(), cert.Issuer.String(), err)
			}
			if len(chains) == 0 {
				return fmt.Errorf("no valid certificate chains found for subject=%q", cert.Subject.String())
			}
		}
		for _, chain := range chains {
			if len(chain) == 0 {
				continue
			}
			chainValid := true
			for i := 0; i < len(chain)-1; i++ {
				if err := chain[i].CheckSignatureFrom(chain[i+1]); err != nil {
					chainValid = false
					break
				}
			}
			if chainValid {
				rootCert := chain[len(chain)-1]
				if err := rootCert.CheckSignatureFrom(rootCert); err != nil {
					continue
				}
				return nil
			}
		}
		subject := "<unknown>"
		if len(chains) > 0 && len(chains[0]) > 0 {
			subject = chains[0][0].Subject.String()
		}
		return fmt.Errorf("no valid certificate chain terminating at a self-signed root for subject=%q", subject)
	}
}
