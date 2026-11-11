// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/endpoint"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

// Config is a validated, immutable session configuration. Build one with
// NewConfig and ConfigBuilder.Build; the zero Config is rejected by Connect.
// Build snapshots configuration values. Callbacks and provider objects remain
// shared and must support concurrent use. A Config can be reused for many sessions.
type Config struct {
	settings *implSettings.Config
}

// ConfigBuilder assembles a Config. Settings that are not chosen keep the
// driver's behavior; when a setting is chosen again, the last call wins.
// Setters only store values; Build validates the final configuration.
// Zero durations disable optional timeouts and background work. Nil optional
// providers remove the override. A ConfigBuilder is not safe for concurrent use.
type ConfigBuilder struct {
	profile              *ProfileBuilder
	routes               *ClientRoutesBuilder
	ignoredEvents        []EventKind
	settings             implSettings.Config
	onlyHost             cql.Option[cql.UUID]
	discoveryChosen      bool
	shardAwarenessChosen bool
	maxExcessChosen      bool
	perHost              bool
}

// NewConfig starts a configuration that connects to contactPoints. A contact
// point is host[:port], or a single absolute Unix socket path (optionally
// prefixed with "unix:"); for a socket, no host discovery is filled in unless
// chosen.
func NewConfig(contactPoints ...string) *ConfigBuilder {
	return &ConfigBuilder{settings: implSettings.DefaultConfig(contactPoints...), profile: NewProfile()}
}

// Build validates the final settings and returns the Config or the first
// validation error. Later calls on the builder do not affect the returned
// Config.
func (b *ConfigBuilder) Build() (Config, error) {
	settings := b.settings
	profile := b.profile.profile
	settings.DefaultProfile = &profile
	if b.routes != nil {
		routes := b.routes.routes
		settings.ClientRoutes = &routes
	}
	if path, _ := implSettings.InternalUnixSocketPath(settings.ContactPoints); path != "" {
		if !b.discoveryChosen {
			settings.Metadata.InitialDiscovery = false
		}
	}
	if settings.TLSSource != nil {
		settings.TLS = settings.TLSSource.TLSConfig()
		if settings.TLS == nil {
			return Config{}, &cqlerr.ValidationError{Field: "TLS", Err: errors.New("source returned no config")}
		}
	}
	if settings.ClientRoutes != nil && !b.shardAwarenessChosen {
		settings.Pool.ShardAwarePort = false
	}
	if b.perHost {
		if b.shardAwarenessChosen && settings.Pool.ShardAwarePort {
			return Config{}, &cqlerr.ValidationError{Field: "pool size", Err: errors.New("PerHost dials no shard-aware port; drop WithAdvancedShardAwareness or use PerShard")}
		}
		if b.maxExcessChosen {
			return Config{}, &cqlerr.ValidationError{Field: "pool size", Err: errors.New("PerHost keeps no per-shard excess connections; drop WithMaxExcessPerShard or use PerShard")}
		}
		settings.Pool.ShardAwarePort = false
	}
	if b.onlyHost.Set {
		id := b.onlyHost.Value
		if id.IsEmpty() {
			return Config{}, &cqlerr.ValidationError{Field: "only host", Err: errors.New("empty host ID")}
		}
		keep := settings.HostFilter
		settings.HostFilter = func(host routing.HostSnapshot) bool { return host.ID == id && (keep == nil || keep(host)) }
	}
	for _, kind := range b.ignoredEvents {
		switch kind {
		case TopologyEvent:
			settings.Metadata.TopologyEvents = false
		case StatusEvent:
			settings.Metadata.StatusEvents = false
		case SchemaEvent:
			settings.Metadata.SchemaEvents = false
		case ClientRouteEvent:
			settings.Metadata.ClientRouteEvents = false
		default:
			return Config{}, &cqlerr.ValidationError{Field: "ignored events", Err: fmt.Errorf("%d is not a server event", kind)}
		}
	}
	if err := settings.Validate(); err != nil {
		return Config{}, err
	}
	resolved := settings.Snapshot()
	return Config{settings: &resolved}, nil
}

// WithPort sets the port used for contact points that name none. Default 9042.
func (b *ConfigBuilder) WithPort(port uint16) *ConfigBuilder {
	b.settings.Port = port
	return b
}

// WithContactPoints replaces all contact points, using the same address syntax
// as NewConfig. Build requires at least one contact point.
func (b *ConfigBuilder) WithContactPoints(contactPoints ...string) *ConfigBuilder {
	b.settings.ContactPoints = append([]string(nil), contactPoints...)
	return b
}

// WithKeyspace sets the keyspace every connection uses. Default none.
func (b *ConfigBuilder) WithKeyspace(name string) *ConfigBuilder {
	b.settings.Keyspace = name
	return b
}

// WithAuth sets the authenticator. Default none.
func (b *ConfigBuilder) WithAuth(provider auth.Provider) *ConfigBuilder {
	b.settings.Auth = provider
	return b
}

// WithTLS encrypts every connection with the configuration source returns
// when the connection is dialed: StaticTLS for a fixed configuration,
// NewTLSFiles or your own TLSSource for one that changes while the session runs. Build
// validates the configuration source returns at that time. Unless it skips
// verification or names a server, each connection verifies the node's IP
// address. A nil source selects plaintext. Default plaintext.
func (b *ConfigBuilder) WithTLS(source TLSSource) *ConfigBuilder {
	b.settings.TLSSource = source
	return b
}

// WithCompression sets frame compression. Default none.
func (b *ConfigBuilder) WithCompression(compression Compression) *ConfigBuilder {
	b.settings.Compression = compression
	return b
}

// WithCQLVersion sets the CQL version sent in STARTUP. Default "3.0.0".
func (b *ConfigBuilder) WithCQLVersion(version string) *ConfigBuilder {
	b.settings.CQLVersion = version
	return b
}

// WithMaxFrameSize caps a received frame's size in bytes. Default 256 MiB.
func (b *ConfigBuilder) WithMaxFrameSize(bytes int64) *ConfigBuilder {
	b.settings.Limits.MaxFrameBytes = bytes
	return b
}

// WithMaxDecompressedSize caps a frame's decompressed size in bytes. Default 256 MiB.
func (b *ConfigBuilder) WithMaxDecompressedSize(bytes int64) *ConfigBuilder {
	b.settings.Limits.MaxDecompressedBytes = bytes
	return b
}

// WithMaxMetadataDepth caps the nesting depth of result type metadata. Default 64.
func (b *ConfigBuilder) WithMaxMetadataDepth(depth int) *ConfigBuilder {
	b.settings.Limits.MaxMetadataDepth = depth
	return b
}

// WithMaxPagingStates caps the paging states a session keeps. Default 65536.
func (b *ConfigBuilder) WithMaxPagingStates(n int) *ConfigBuilder {
	b.settings.Limits.MaxPagingStates = n
	return b
}

// WithMaxPagingStateSize caps one paging state's size in bytes. Default 16 MiB.
func (b *ConfigBuilder) WithMaxPagingStateSize(bytes int64) *ConfigBuilder {
	b.settings.Limits.MaxPagingStateBytes = bytes
	return b
}

// WithDialer opens raw connections; TLS is applied afterward. Use
// endpoint.DialerFunc to adapt a function. Default net.Dialer.
func (b *ConfigBuilder) WithDialer(dialer endpoint.Dialer) *ConfigBuilder {
	b.settings.Dialer = dialer
	return b
}

// WithResolver resolves contact point and client route host names. Each
// lookup is bounded by the DNS timeout through its context, which the resolver
// must honour. Default net.DefaultResolver.
func (b *ConfigBuilder) WithResolver(resolver endpoint.Resolver) *ConfigBuilder {
	b.settings.DNSResolver = resolver
	return b
}

// WithLocalAddress sets the source address of every connection. Default any.
func (b *ConfigBuilder) WithLocalAddress(address netip.Addr) *ConfigBuilder {
	b.settings.LocalAddress = address
	return b
}

// WithConnectTimeout bounds opening one connection. Default 5s.
func (b *ConfigBuilder) WithConnectTimeout(d time.Duration) *ConfigBuilder {
	b.settings.ConnectTimeout = d
	return b
}

// WithDNSTimeout bounds one name lookup. Default 5s.
func (b *ConfigBuilder) WithDNSTimeout(d time.Duration) *ConfigBuilder {
	b.settings.DNSTimeout = d
	return b
}

// WithNagle turns Nagle's algorithm on or off. Off sets TCP_NODELAY. Default
// off.
func (b *ConfigBuilder) WithNagle(on bool) *ConfigBuilder {
	b.settings.NoDelay = !on
	return b
}

// WithTCPKeepalive enables TCP keepalive probes with period. Default off.
func (b *ConfigBuilder) WithTCPKeepalive(period time.Duration) *ConfigBuilder {
	b.settings.SocketKeepalive = period
	return b
}

// WithSocketBuffers sets SO_RCVBUF and SO_SNDBUF in bytes. Default the OS's.
func (b *ConfigBuilder) WithSocketBuffers(receive, send int) *ConfigBuilder {
	b.settings.ReceiveBuffer = cql.Some(receive)
	b.settings.SendBuffer = cql.Some(send)
	return b
}

// WithLinger sets SO_LINGER in whole seconds; Linger(0) resets connections on
// close. Default the OS's.
func (b *ConfigBuilder) WithLinger(d time.Duration) *ConfigBuilder {
	b.settings.Linger = cql.Some(d)
	return b
}

// WithReadTimeout drops a connection that delivers no bytes for d. Default none.
func (b *ConfigBuilder) WithReadTimeout(d time.Duration) *ConfigBuilder {
	b.settings.ReadTimeout = d
	return b
}

// WithWriteTimeout drops a connection that accepts no bytes for d. Default none.
func (b *ConfigBuilder) WithWriteTimeout(d time.Duration) *ConfigBuilder {
	b.settings.WriteTimeout = d
	return b
}

// WithHeartbeat sends a CQL OPTIONS request on idle connections every interval
// and drops a connection whose heartbeat takes longer than timeout. A zero
// interval sends no heartbeats: WithHeartbeat(0, 0) turns them off. A positive
// interval needs a positive timeout. Default 30s and 30s.
func (b *ConfigBuilder) WithHeartbeat(interval, timeout time.Duration) *ConfigBuilder {
	b.settings.KeepaliveInterval = interval
	b.settings.KeepaliveTimeout = timeout
	return b
}

// WithCoalesceWrites delays writes up to wait to send them together. Any wait
// turns off the inline execution path, so it costs latency on a healthy
// cluster. Default off.
func (b *ConfigBuilder) WithCoalesceWrites(wait time.Duration) *ConfigBuilder {
	b.settings.CoalesceWait = wait
	return b
}

// PoolSize is how many connections the session keeps open to each node. Make
// one with PerShard or PerHost.
type PoolSize struct {
	n       int
	perHost bool
}

// PerShard keeps n connections to every shard of each node, so each request
// goes out on a connection of the shard that owns its data. A node that
// reports no shards, such as Cassandra, counts as one shard. This is the
// default, with n = 1.
func PerShard(n int) PoolSize { return PoolSize{n: n} }

// PerHost keeps n connections to each node regardless of its shard count.
//
// On ScyllaDB it turns shard-aware connection management off: connections are
// dialed through the regular port only, never the shard-aware port (as with
// NoAdvancedShardAwareness), so the node assigns each connection's shard and
// some shards may get none. A request whose shard has no connection goes out
// on another shard's connection and costs the node a cross-shard hop. There
// are no per-shard excess connections, so WithMaxExcessPerShard does not
// apply. Build rejects PerHost together with WithAdvancedShardAwareness or
// WithMaxExcessPerShard.
func PerHost(n int) PoolSize { return PoolSize{n: n, perHost: true} }

// WithPoolSize sets how many connections the session keeps to each node:
// PerShard or PerHost. n must be positive. Default PerShard(1).
func (b *ConfigBuilder) WithPoolSize(size PoolSize) *ConfigBuilder {
	b.perHost = size.perHost
	if size.perHost {
		b.settings.Pool.ConnectionsPerHost, b.settings.Pool.ConnectionsPerShard = size.n, 0
	} else {
		b.settings.Pool.ConnectionsPerShard, b.settings.Pool.ConnectionsPerHost = size.n, 0
	}
	return b
}

// WithMaxExcessPerShard caps connections opened beyond the target per shard.
// It applies to PerShard pools only. Default 2.
func (b *ConfigBuilder) WithMaxExcessPerShard(n int) *ConfigBuilder {
	b.maxExcessChosen = true
	b.settings.Pool.MaxExcessPerShard = n
	return b
}

// WithMaxRequestsPerConnection caps in-flight requests per connection, at most
// 32768. Default 32768.
func (b *ConfigBuilder) WithMaxRequestsPerConnection(n int) *ConfigBuilder {
	b.settings.Pool.MaxRequestsPerConnection = n
	return b
}

// WithMaxOrphanedStreams caps streams abandoned by cancelled requests before a
// connection is recycled. Default 1024.
func (b *ConfigBuilder) WithMaxOrphanedStreams(n int) *ConfigBuilder {
	b.settings.Pool.MaxOrphanedStreams = n
	return b
}

// WithAdvancedShardAwareness picks the shard of each new connection by dialing
// the node's shard-aware port from a local source port chosen from first to
// last. It replaces NoAdvancedShardAwareness. Default on with ports
// 49152-65535, except under WithClientRoutes or PerHost.
func (b *ConfigBuilder) WithAdvancedShardAwareness(first, last uint16) *ConfigBuilder {
	b.shardAwarenessChosen = true
	b.settings.Pool.ShardAwarePort = true
	b.settings.Pool.SourcePortStart, b.settings.Pool.SourcePortEnd = first, last
	return b
}

// NoAdvancedShardAwareness connects through the regular port only and lets
// the node assign the shard; the pool keeps dialing until every shard is
// covered. It replaces WithAdvancedShardAwareness.
func (b *ConfigBuilder) NoAdvancedShardAwareness() *ConfigBuilder {
	b.shardAwarenessChosen = true
	b.settings.Pool.ShardAwarePort = false
	return b
}

// WithDiscovery chooses whether the session discovers and connects to the
// cluster's nodes (true) or connects only to the contact points (false).
// Default true, except for a Unix socket, where it is false unless chosen.
func (b *ConfigBuilder) WithDiscovery(on bool) *ConfigBuilder {
	b.discoveryChosen = true
	b.settings.Metadata.InitialDiscovery = on
	return b
}

// WithHostFilter skips nodes for which keep returns false. Default all nodes.
func (b *ConfigBuilder) WithHostFilter(keep func(routing.HostSnapshot) bool) *ConfigBuilder {
	b.settings.HostFilter = keep
	return b
}

// WithOnlyHost connects only to the node with host ID id, as reported by
// Session.Metadata. It composes with WithHostFilter: a node is used when it
// is id and the filter keeps it. The contact points still serve discovery.
// Build rejects an empty id. Default all nodes.
func (b *ConfigBuilder) WithOnlyHost(id cql.UUID) *ConfigBuilder {
	b.onlyHost = cql.Some(id)
	return b
}

// WithAddressTranslator rewrites node addresses before dialing. It is called
// for the CQL and any shard-aware endpoint of a node with the node's
// endpoint.Peer; an error fails that connection attempt. Default identity.
func (b *ConfigBuilder) WithAddressTranslator(translator endpoint.Translator) *ConfigBuilder {
	b.settings.AddressTranslator = translator
	return b
}

// WithClientRoutes connects through ScyllaDB client routes. Nil removes routes.
func (b *ConfigBuilder) WithClientRoutes(routes *ClientRoutesBuilder) *ConfigBuilder {
	b.routes = routes
	return b
}

// WithReconnect sets the delay between attempts to reopen a node's connections.
// Nil disables reconnection attempts.
// Default jittered backoff from 1s to 32s.
func (b *ConfigBuilder) WithReconnect(policy ReconnectPolicy) *ConfigBuilder {
	b.settings.Reconnect = policy
	return b
}

// WithConviction decides when connection errors mark a node down. Default none.
func (b *ConfigBuilder) WithConviction(policy ConvictionPolicy) *ConfigBuilder {
	b.settings.Conviction = policy
	return b
}

// WithMetadataMode selects how much schema metadata is loaded. Default
// metadata.SchemaFull.
func (b *ConfigBuilder) WithMetadataMode(mode metadata.Mode) *ConfigBuilder {
	b.settings.Metadata.Mode = mode
	return b
}

// WithMetadataKeyspaces limits schema metadata to keyspaces. Default all.
func (b *ConfigBuilder) WithMetadataKeyspaces(keyspaces ...string) *ConfigBuilder {
	b.settings.Metadata.Keyspaces = append([]string(nil), keyspaces...)
	return b
}

// WithPartitioner sets the token partitioner for the partitioner class name,
// next to the built-in Murmur3 and CDC partitioners.
func (b *ConfigBuilder) WithPartitioner(name string, partitioner routing.TokenPartitioner) *ConfigBuilder {
	b.settings.Partitioners[name] = partitioner
	return b
}

// WithMetadataRefresh reloads metadata every interval. Zero turns periodic
// reloads off; events and schema agreement still refresh it. Default 60s.
func (b *ConfigBuilder) WithMetadataRefresh(interval time.Duration) *ConfigBuilder {
	b.settings.Metadata.PeriodicRefresh = interval
	return b
}

// WithMetadataTimeout bounds one metadata request. Default 31s.
func (b *ConfigBuilder) WithMetadataTimeout(d time.Duration) *ConfigBuilder {
	b.settings.Metadata.RequestTimeout = d
	return b
}

// WithMetadataServerTimeout asks the server to bound metadata queries with USING
// TIMEOUT, in whole milliseconds. Default unset.
func (b *ConfigBuilder) WithMetadataServerTimeout(d time.Duration) *ConfigBuilder {
	b.settings.Metadata.ServerTimeout = cql.Some(d)
	return b
}

// WithIgnoreEvents stops listening for the given server events: TopologyEvent,
// StatusEvent, SchemaEvent or ClientRouteEvent. Each call replaces the complete
// ignored-event list; no arguments restores all events. Default none ignored.
func (b *ConfigBuilder) WithIgnoreEvents(kinds ...EventKind) *ConfigBuilder {
	b.ignoredEvents = append([]EventKind(nil), kinds...)
	return b
}

// WithSchemaAgreement waits up to timeout, polling every poll, for nodes to agree
// on the schema. Default 60s and 200ms.
func (b *ConfigBuilder) WithSchemaAgreement(timeout, poll time.Duration) *ConfigBuilder {
	b.settings.Metadata.AgreementTimeout = timeout
	b.settings.Metadata.AgreementPoll = poll
	return b
}

// WithAutomaticSchemaAgreement chooses whether a DDL statement waits for
// schema agreement before it returns. With false, call
// Session.AwaitSchemaAgreement where agreement matters. Default true.
func (b *ConfigBuilder) WithAutomaticSchemaAgreement(on bool) *ConfigBuilder {
	b.settings.Metadata.AutomaticAgreement = on
	return b
}

// WithRefreshAfterAgreement chooses whether metadata is reloaded once schema
// agreement is reached. Default true.
func (b *ConfigBuilder) WithRefreshAfterAgreement(on bool) *ConfigBuilder {
	b.settings.Metadata.RefreshAfterAgreement = on
	return b
}

// WithDefaultProfile replaces the complete execution defaults for statements
// that pass no Profile. The profile is copied now; policy objects stay shared.
// Build validates the final settings and rejects a profile that
// ProfileBuilder.Build did not make; start one with NewProfile.
func (b *ConfigBuilder) WithDefaultProfile(profile Profile) *ConfigBuilder {
	b.profile = &ProfileBuilder{profile: internalProfile(profile)}
	return b
}

// ConfigureDefaultProfile chooses execution defaults for statements that pass no
// Profile. choose receives the builder's default profile, so several calls
// add to the same profile, and edits apply on top of the profile an earlier
// WithDefaultProfile set. A nil choose resets the entire default profile.
func (b *ConfigBuilder) ConfigureDefaultProfile(choose func(*ProfileBuilder)) *ConfigBuilder {
	if choose == nil {
		b.profile = NewProfile()
	} else {
		choose(b.profile)
	}
	return b
}

// WithPreparedCacheSize keeps up to n prepared statements, so preparing the
// same CQL again reuses the cached statement. Zero turns the cache off: every
// Session.Prepare prepares anew. Default 1000.
func (b *ConfigBuilder) WithPreparedCacheSize(n int) *ConfigBuilder {
	b.settings.PreparedCacheSize = n
	return b
}

// WithTimestampGenerator supplies client timestamps in microseconds. It replaces
// WithServerTimestamps. Default a monotonic clock.
func (b *ConfigBuilder) WithTimestampGenerator(next func() int64) *ConfigBuilder {
	b.settings.TimestampGenerator = next
	return b
}

// WithServerTimestamps sends no client timestamp, so the server assigns one. It
// replaces WithTimestampGenerator.
func (b *ConfigBuilder) WithServerTimestamps() *ConfigBuilder {
	b.settings.TimestampGenerator = nil
	return b
}

// WithApplication reports the application's name and version in STARTUP.
func (b *ConfigBuilder) WithApplication(name, version string) *ConfigBuilder {
	b.settings.Identity.ApplicationName = name
	b.settings.Identity.ApplicationVersion = version
	return b
}

// WithClientID reports id in STARTUP.
func (b *ConfigBuilder) WithClientID(id string) *ConfigBuilder {
	b.settings.Identity.ClientID = id
	return b
}

// WithLogger receives the driver's structured log records. Default slog.Default().
// Use slog.New(slog.DiscardHandler) to silence the driver, a Debug-level handler for
// per-connection detail, or NewPrintfLogger for a printf-style logger. Records at Info and
// above are rate-limited per message and host; handler panics are contained.
func (b *ConfigBuilder) WithLogger(logger *slog.Logger) *ConfigBuilder {
	b.settings.Logger = logger
	return b
}

// WithObserver receives every request and attempt. Default none.
func (b *ConfigBuilder) WithObserver(observer Observer) *ConfigBuilder {
	b.settings.Observer = observer
	return b
}

// WithLifecycleObserver receives session and connection lifecycle events.
func (b *ConfigBuilder) WithLifecycleObserver(observer LifecycleObserver) *ConfigBuilder {
	b.settings.LifecycleObserver = observer
	return b
}

// WithConfigReport chooses whether the driver configuration is sent to the
// server. Default true.
func (b *ConfigBuilder) WithConfigReport(on bool) *ConfigBuilder {
	b.settings.ReportConfiguration = on
	return b
}

// String lists the effective settings. Credentials and TLS material are not
// printed.
func (c Config) String() string {
	if c.settings == nil {
		return "gocql.Config{}"
	}
	s := c.settings
	var w strings.Builder
	fmt.Fprintf(&w, "contact points=%v port=%d keyspace=%q compression=%v", s.ContactPoints, s.Port, s.Keyspace, s.Compression)
	fmt.Fprintf(&w, " tls=%t auth=%t", s.TLS != nil, s.Auth != nil)
	fmt.Fprintf(&w, " connections per shard=%d per host=%d advanced shard awareness=%t", s.Pool.ConnectionsPerShard, s.Pool.ConnectionsPerHost, s.Pool.ShardAwarePort)
	fmt.Fprintf(&w, " discovery=%t metadata=%d prepared cache=%d", s.Metadata.InitialDiscovery, s.Metadata.Mode, s.PreparedCacheSize)
	if p := s.DefaultProfile; p != nil {
		fmt.Fprintf(&w, " consistency=%v timeout=%v page size=%d", p.Consistency, p.Timeout, p.PageSize)
	}
	return w.String()
}

// resolved returns the settings to connect with, or an error for the zero
// Config.
func (c Config) resolved() (implSettings.Config, error) {
	if c.settings == nil {
		return implSettings.Config{}, &cqlerr.ValidationError{Field: "Config", Err: errors.New("zero Config; build one with gocql.NewConfig")}
	}
	return *c.settings, nil
}

// ProfileBuilder assembles a Profile. Settings that are not chosen keep the
// driver's defaults: LOCAL_QUORUM, LOCAL_SERIAL, 30s timeout, 5000-row pages,
// one page prefetched, token-aware routing and safe retries. The last call
// for a setting wins.
type ProfileBuilder struct {
	profile implSettings.Profile
}

// NewProfile starts a profile.
func NewProfile() *ProfileBuilder {
	return &ProfileBuilder{profile: *implSettings.DefaultConfig().DefaultProfile}
}

// Build validates the profile, for use as Options.Profile.
func (p *ProfileBuilder) Build() (Profile, error) {
	if err := p.profile.ValidateProfile(); err != nil {
		return Profile{}, err
	}
	return publicProfile(p.profile), nil
}

// WithConsistency sets the consistency level. Default LOCAL_QUORUM.
func (p *ProfileBuilder) WithConsistency(c cql.Consistency) *ProfileBuilder {
	p.profile.Consistency = c
	return p
}

// WithSerialConsistency sets the serial consistency of conditional statements:
// SERIAL or LOCAL_SERIAL. It replaces NoSerialConsistency. Default
// LOCAL_SERIAL.
func (p *ProfileBuilder) WithSerialConsistency(c cql.Consistency) *ProfileBuilder {
	p.profile.SerialConsistency = cql.Some(c)
	return p
}

// NoSerialConsistency sends no serial consistency. It replaces
// WithSerialConsistency.
func (p *ProfileBuilder) NoSerialConsistency() *ProfileBuilder {
	p.profile.SerialConsistency = cql.Option[cql.Consistency]{}
	return p
}

// WithTimeout bounds one execution, including retries. It replaces NoTimeout.
// Default 30s.
func (p *ProfileBuilder) WithTimeout(d time.Duration) *ProfileBuilder {
	p.profile.Timeout = d
	return p
}

// NoTimeout leaves executions bounded only by their context. It replaces
// WithTimeout.
func (p *ProfileBuilder) NoTimeout() *ProfileBuilder {
	p.profile.Timeout = 0
	return p
}

// WithPageTimeout bounds fetching one page. Default none.
func (p *ProfileBuilder) WithPageTimeout(d time.Duration) *ProfileBuilder {
	p.profile.PageTimeout = d
	return p
}

// WithPageSize sets the rows per page. Default 5000.
func (p *ProfileBuilder) WithPageSize(rows int32) *ProfileBuilder {
	p.profile.PageSize = rows
	return p
}

// NoPrefetch fetches the next page only when the current one is consumed.
func (p *ProfileBuilder) NoPrefetch() *ProfileBuilder {
	return p.WithPrefetch(0)
}

// WithPrefetch sets how many continuation pages to prefetch: zero or one.
// It replaces NoPrefetch. Default one.
func (p *ProfileBuilder) WithPrefetch(pages int) *ProfileBuilder {
	p.profile.Prefetch = pages
	return p
}

// WithRetry sets the retry policy; nil disables retries.
// Default retry.Safe{MaxAttempts: 3}.
func (p *ProfileBuilder) WithRetry(policy retry.Policy) *ProfileBuilder {
	p.profile.Retry = policy
	return p
}

// WithRouting sets the routing policy. Nil uses the session pool's host
// selection without a request-specific routing plan. Default token-aware with
// shuffled replicas.
func (p *ProfileBuilder) WithRouting(policy routing.Policy) *ProfileBuilder {
	p.profile.Routing = policy
	return p
}

// WithSpeculation sets the speculative execution policy; nil disables it.
// Default none.
func (p *ProfileBuilder) WithSpeculation(policy SpeculationPolicy) *ProfileBuilder {
	p.profile.Speculation = policy
	return p
}

// ClientRoutesBuilder assembles ScyllaDB client routes settings.
type ClientRoutesBuilder struct {
	routes implSettings.ClientRoutesOptions
}

// NewClientRoutes routes through the given client connection IDs.
func NewClientRoutes(connectionIDs ...string) *ClientRoutesBuilder {
	return &ClientRoutesBuilder{routes: implSettings.ClientRoutesOptions{ConnectionIDs: append([]string(nil), connectionIDs...)}}
}

// WithHostnameOverride dials hostname for connectionID. An empty hostname
// removes that connection's override.
func (r *ClientRoutesBuilder) WithHostnameOverride(connectionID, hostname string) *ClientRoutesBuilder {
	if hostname == "" {
		delete(r.routes.HostnameOverrides, connectionID)
		return r
	}
	if r.routes.HostnameOverrides == nil {
		r.routes.HostnameOverrides = make(map[string]string)
	}
	r.routes.HostnameOverrides[connectionID] = hostname
	return r
}

// WithTLS requires verified TLS on routed connections.
func (r *ClientRoutesBuilder) WithTLS() *ClientRoutesBuilder {
	r.routes.TLS = true
	return r
}

// NoTLS removes the client-route requirement for verified TLS. It replaces
// WithTLS; the session's WithTLS still controls connection encryption.
func (r *ClientRoutesBuilder) NoTLS() *ClientRoutesBuilder {
	r.routes.TLS = false
	return r
}
