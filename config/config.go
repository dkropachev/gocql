// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2/auth"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/event"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/observe"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/routing"
)

// Config is a validated, immutable session configuration. Build one with
// New and ConfigBuilder.Build; gocql.Connect rejects the zero Config with a
// cql.ValidationError.
// Build snapshots configuration values. Callbacks and provider objects remain
// shared and must support concurrent use. A Config can be reused for many sessions.
type Config struct {
	settings *implSettings.Config
}

// ConfigBuilder assembles a Config. Settings that are not chosen keep the
// driver's behavior; when a setting is chosen again, the last call wins.
// Setters only store values; Build validates the final configuration, and a
// cql.ValidationError it returns names the setter to fix in its Field.
// Zero durations disable optional timeouts and background work. A nil
// provider or policy restores the driver's default; the one exception is
// WithTimestampGenerator, where nil sends no client timestamp.
// The timeouts set here bound connections, lookups and metadata requests; a
// request's own time limit is an execution default, ProfileBuilder.WithTimeout
// on the profile WithDefaultProfile sets.
// A ConfigBuilder is not safe for concurrent use. Start one with New: Build
// rejects a zero ConfigBuilder with a cql.ValidationError.
type ConfigBuilder struct {
	profile              *ProfileBuilder
	ignoredEvents        []event.Kind
	routes               []ClientRoute
	settings             implSettings.Config
	onlyHost             cql.Option[cql.UUID]
	discoveryChosen      bool
	shardAwarenessChosen bool
	sourcePortsChosen    bool
	maxExcessChosen      bool
	portChosen           bool
	schemaChangeWait     SchemaChangeWait
	perHost              bool
	dialect              Dialect
	// started reports that New made the builder.
	started bool
}

// New starts a configuration that connects to contactPoints, which speak
// dialect. A contact point is host[:port], or a single absolute Unix socket
// path (optionally prefixed with "unix:"); for a socket, no host discovery is
// filled in unless chosen. Build rejects the zero Dialect.
func New(dialect Dialect, contactPoints ...string) *ConfigBuilder {
	return &ConfigBuilder{settings: implSettings.DefaultConfig(contactPoints...), profile: NewProfile(), dialect: dialect, schemaChangeWait: SchemaChangeWaitRefresh, started: true}
}

// Clone returns an independent copy of the builder, so a shared base can be
// specialized without changing it: settings chosen on the copy do not reach
// b, and those chosen on b later do not reach the copy. Providers, policies
// and callbacks stay shared, as in a Config. The clone of a zero
// ConfigBuilder is zero too, and Build rejects it.
func (b *ConfigBuilder) Clone() *ConfigBuilder {
	c := *b
	c.settings = b.settings.Snapshot()
	if b.profile != nil {
		profile := *b.profile
		c.profile = &profile
	}
	c.ignoredEvents = slices.Clone(b.ignoredEvents)
	c.routes = slices.Clone(b.routes)
	return &c
}

// Build validates the final settings and returns the Config or the first
// validation error. A setting the dialect does not support fails with a
// cqlerr.UnsupportedSettingError. Later calls on the builder do not affect
// the returned Config.
func (b *ConfigBuilder) Build() (Config, error) {
	if !b.started {
		return Config{}, &cql.ValidationError{Field: "config", Err: errors.New("zero ConfigBuilder; start one with config.New")}
	}
	server, protocol, ok := implSettings.DialectSpec(uint8(b.dialect))
	if !ok {
		return Config{}, &cql.ValidationError{Field: "New", Err: fmt.Errorf("dialect %v is not ScyllaV4, CassandraV4 or CassandraV5", b.dialect)}
	}
	settings := b.settings
	settings.Server, settings.Protocol = server, protocol
	if !b.portChosen {
		if port, ok := implSettings.CommonContactPort(settings.ContactPoints); ok {
			settings.Port = port
		}
	}
	if b.profile == nil || !b.profile.started {
		return Config{}, &cql.ValidationError{Field: "WithDefaultProfile", Err: errors.New("zero ProfileBuilder; start one with config.NewProfile")}
	}
	profile := b.profile.profile
	settings.DefaultProfile = &profile
	settings.ClientRoutes = clientRoutesOptions(b.routes)
	if path, _ := implSettings.InternalUnixSocketPath(settings.ContactPoints); path != "" {
		if !b.discoveryChosen {
			settings.Metadata.InitialDiscovery = false
		}
	}
	if settings.Identity.ApplicationVersion != "" && settings.Identity.ApplicationName == "" {
		return Config{}, &cql.ValidationError{Field: "WithApplication", Err: errors.New("a version requires a name")}
	}
	if settings.TLSSource != nil {
		settings.TLS = settings.TLSSource.TLSConfig()
		if settings.TLS == nil {
			return Config{}, &cql.ValidationError{Field: "WithTLSSource", Err: errors.New("source returned no config")}
		}
	}
	if settings.TLS == nil {
		for _, set := range []struct {
			name string
			on   bool
		}{{"WithTLSIdentity", settings.TLSIdentity != nil}, {"WithTLSPort", settings.TLSPort != 0}, {"WithTLSFallback", settings.TLSFallback}} {
			if set.on {
				return Config{}, &cql.ValidationError{Field: set.name, Err: errors.New("requires WithTLS or WithTLSSource")}
			}
		}
	}
	if settings.TLSPort != 0 && settings.ClientRoutes != nil {
		return Config{}, &cql.ValidationError{Field: "WithTLSPort", Err: errors.New("client routes choose their own ports")}
	}
	if settings.ClientRoutes != nil && !b.shardAwarenessChosen {
		settings.Pool.AdvancedShardAwareness.Enabled = false
	}
	if b.perHost {
		if b.shardAwarenessChosen && settings.Pool.AdvancedShardAwareness.Enabled {
			return Config{}, &cql.ValidationError{Field: "WithConnectionsPerHost", Err: errors.New("dials no shard-aware port; drop WithAdvancedShardAwareness(true) or use WithConnectionsPerShard")}
		}
		if b.maxExcessChosen {
			return Config{}, &cql.ValidationError{Field: "WithConnectionsPerHost", Err: errors.New("keeps no per-shard excess connections; drop WithMaxExcessPerShard or use WithConnectionsPerShard")}
		}
		settings.Pool.AdvancedShardAwareness.Enabled = false
	}
	if sa := settings.Pool.AdvancedShardAwareness; b.sourcePortsChosen && (sa.SourcePortStart < 1024 || sa.SourcePortStart > sa.SourcePortEnd) {
		return Config{}, &cql.ValidationError{Field: "WithShardAwareSourcePorts", Err: fmt.Errorf("%d-%d is not a range within 1024-65535", sa.SourcePortStart, sa.SourcePortEnd)}
	}
	if b.sourcePortsChosen {
		if err := implSettings.ScyllaOnly(&settings, "WithShardAwareSourcePorts"); err != nil {
			return Config{}, err
		}
		if !settings.Pool.AdvancedShardAwareness.Enabled {
			reason := "WithAdvancedShardAwareness(false)"
			switch {
			case b.perHost:
				reason = "WithConnectionsPerHost"
			case settings.ClientRoutes != nil && !b.shardAwarenessChosen:
				reason = "WithClientRoutes"
			}
			return Config{}, &cql.ValidationError{Field: "WithShardAwareSourcePorts", Err: fmt.Errorf("has no effect: %s turns advanced shard awareness off", reason)}
		}
	}
	if b.onlyHost.Set {
		id := b.onlyHost.Value
		if id.IsZero() {
			return Config{}, &cql.ValidationError{Field: "WithOnlyHost", Err: errors.New("empty host ID")}
		}
		keep := settings.HostFilter
		settings.HostFilter = func(host metadata.HostSnapshot) bool { return host.HostID == id && (keep == nil || keep(host)) }
	}
	switch b.schemaChangeWait {
	case SchemaChangeWaitNone, SchemaChangeWaitAgreement, SchemaChangeWaitRefresh:
		settings.Metadata.AutomaticAgreement = b.schemaChangeWait >= SchemaChangeWaitAgreement
		settings.Metadata.RefreshAfterAgreement = b.schemaChangeWait == SchemaChangeWaitRefresh
	default:
		return Config{}, &cql.ValidationError{Field: "WithSchemaChangeWait", Err: fmt.Errorf("%v is not SchemaChangeWaitNone, SchemaChangeWaitAgreement or SchemaChangeWaitRefresh", b.schemaChangeWait)}
	}
	for _, kind := range b.ignoredEvents {
		switch kind {
		case event.KindTopology:
			settings.Metadata.TopologyEvents = false
		case event.KindStatus:
			settings.Metadata.StatusEvents = false
		case event.KindSchema:
			settings.Metadata.SchemaEvents = false
		case event.KindClientRoute:
			settings.Metadata.ClientRouteEvents = false
		default:
			return Config{}, &cql.ValidationError{Field: "WithIgnoreEvents", Err: fmt.Errorf("%v is not a server event", kind)}
		}
	}
	if err := settings.Validate(); err != nil {
		return Config{}, err
	}
	if err := settings.ProbeReconnect(); err != nil {
		return Config{}, err
	}
	resolved := settings.Snapshot()
	return Config{settings: &resolved}, nil
}

// WithPort sets the CQL port of contact points that name none and of
// discovered nodes whose metadata reports no native port, as ScyllaDB's
// system.peers does not. Contact points that all name one port other than
// port would have the driver dial the nodes it discovers on the wrong port,
// so Build rejects them unless WithDiscovery(false) or WithClientRoutes is
// set; name no port in them, or set WithPort to theirs. Default the port
// every TCP contact point names, when they all name the same one, and 9042
// otherwise; Config.String shows the port in effect.
func (b *ConfigBuilder) WithPort(port uint16) *ConfigBuilder {
	b.settings.Port = port
	b.portChosen = true
	return b
}

// WithContactPoints replaces all contact points, using the same address syntax
// as New. Build requires at least one and rejects one it cannot parse, such
// as a port that is not a number from 1 to 65535.
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

// WithTLS encrypts every connection with a copy of config, taken now. Unless
// config skips verification or names a server, each connection verifies the
// node's IP address; WithTLSIdentity changes the names per node. A nil config selects plaintext. It replaces
// WithTLSSource. Default plaintext.
func (b *ConfigBuilder) WithTLS(config *tls.Config) *ConfigBuilder {
	if config == nil {
		b.settings.TLSSource = nil
	} else {
		b.settings.TLSSource = staticTLS{config: implSettings.InternalCloneV2TLSConfig(config)}
	}
	return b
}

// WithTLSSource encrypts every connection with the configuration source
// returns when the connection is dialed, for one that changes while the
// session runs: NewTLSFileSource or your own endpoint.TLSSource. Build validates the
// configuration source returns at that time. Verification is as for WithTLS.
// A nil source selects plaintext. It replaces WithTLS.
func (b *ConfigBuilder) WithTLSSource(source endpoint.TLSSource) *ConfigBuilder {
	b.settings.TLSSource = source
	return b
}

// WithTLSIdentity sets the function that chooses each TLS connection's
// identity: the SNI it sends, the name the node's certificate must match, a
// chain-only check, and a check of its own. The driver calls it once per
// dial, with the target being dialed, on the dialing goroutine, so it must
// not block. Target.Host.HostID is zero for a contact point dialed before the
// driver has learned the node's identity: return an identity that needs only
// its address then. The certificates, CAs and cipher settings stay those of
// the session's single TLS configuration. It requires WithTLS or
// WithTLSSource. Nil restores the defaults, which WithTLS describes.
func (b *ConfigBuilder) WithTLSIdentity(identity func(target endpoint.DialTarget) endpoint.TLSIdentity) *ConfigBuilder {
	b.settings.TLSIdentity = identity
	return b
}

// WithTLSPort dials TLS connections on port of every node instead of its CQL
// port: ScyllaDB's and Cassandra's native_transport_port_ssl, which serves
// TLS beside the plaintext port. The address translator translates it as it
// does the CQL port. Shard-aware TLS connections keep the port each node
// advertises. It requires WithTLS or WithTLSSource; client routes choose
// their own ports and reject it. Zero, the default, dials the CQL port.
func (b *ConfigBuilder) WithTLSPort(port uint16) *ConfigBuilder {
	b.settings.TLSPort = port
	return b
}

// WithTLSFallback(true) lets a session reach a cluster while TLS is being
// enabled on it, node by node. Each dial tries TLS first, on WithTLSPort's
// port when set. When the node does not speak TLS there (it answers with
// something other than TLS, closes the connection during the handshake, or
// refuses the TLS port), the dial falls back to plaintext on the CQL port.
// A certificate that fails verification never falls back. The fallback is
// one-way: once a node has answered TLS it is never dialed in plaintext
// again, and once every node of the cluster has, the session stops falling
// back at all. Plaintext connections opened earlier stay until they close.
// An attacker on the network can still downgrade a node that has not yet
// answered TLS, so turn it off once the rollout is done. It requires WithTLS
// or WithTLSSource. Default false: a TLS session never dials in plaintext.
func (b *ConfigBuilder) WithTLSFallback(on bool) *ConfigBuilder {
	b.settings.TLSFallback = on
	return b
}

// WithCompression sets frame compression. Default none.
func (b *ConfigBuilder) WithCompression(compression cql.Compression) *ConfigBuilder {
	b.settings.Compression = compression
	return b
}

// WithCQLVersion sets the CQL version sent in STARTUP. Default "3.0.0".
func (b *ConfigBuilder) WithCQLVersion(version string) *ConfigBuilder {
	b.settings.CQLVersion = version
	return b
}

// WithLimits caps what the session accepts from servers and keeps for paging.
// A zero field keeps the driver's default for that limit; Build rejects a
// negative one, or one above its maximum. Each call replaces the complete
// limits setting. Default the zero Limits.
func (b *ConfigBuilder) WithLimits(limits Limits) *ConfigBuilder {
	defaults := implSettings.DefaultConfig().Limits
	pick := func(set, def int64) int64 {
		if set == 0 {
			return def
		}
		return set
	}
	b.settings.Limits = implSettings.ProtocolLimits{
		MaxFrameBytes:        pick(limits.MaxFrameBytes, defaults.MaxFrameBytes),
		MaxDecompressedBytes: pick(limits.MaxDecompressedBytes, defaults.MaxDecompressedBytes),
		MaxMetadataDepth:     int(pick(int64(limits.MaxMetadataDepth), int64(defaults.MaxMetadataDepth))),
		MaxPagingStates:      int(pick(int64(limits.MaxPagingStates), int64(defaults.MaxPagingStates))),
		MaxPagingStateBytes:  pick(limits.MaxPagingStateBytes, defaults.MaxPagingStateBytes),
	}
	return b
}

// WithDialer opens raw connections; TLS is applied afterward. Use
// endpoint.DialerFunc to adapt a function. A nil dialer restores the default,
// endpoint.DefaultDialer.
//
// The other connection settings still apply to a custom dialer. The dial
// context carries the WithConnectTimeout deadline. The source address chosen
// by WithLocalAddress or advanced shard awareness is in
// endpoint.DialTarget.LocalAddress, and a connection from another source is
// rejected. WithNagle, WithTCPKeepalive, WithReceiveBuffer, WithSendBuffer
// and WithLinger are set on the returned TCP connection afterwards; a wrapper
// that hides those controls fails the connection with a
// cqlerr.UnsupportedSettingError. A custom dialer may wrap
// endpoint.DefaultDialer.
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

// WithConnectTimeout bounds opening one connection; Build rejects zero.
// Default 5s.
func (b *ConfigBuilder) WithConnectTimeout(d time.Duration) *ConfigBuilder {
	b.settings.ConnectTimeout = d
	return b
}

// WithDNSTimeout bounds one name lookup; Build rejects zero. Default 5s.
func (b *ConfigBuilder) WithDNSTimeout(d time.Duration) *ConfigBuilder {
	b.settings.DNSTimeout = d
	return b
}

// WithNagle turns Nagle's algorithm on or off. Off sets TCP_NODELAY. Default
// off.
//
// The default is applied best-effort: a connection from WithDialer that does
// not expose SetNoDelay, directly or through an Unwrap() net.Conn method,
// keeps the OS's setting. Once this setter is called, such a connection
// fails to open with an UnsupportedSettingError.
func (b *ConfigBuilder) WithNagle(on bool) *ConfigBuilder {
	b.settings.NoDelay = !on
	b.settings.ChosenSocketOptions |= implSettings.SocketNoDelay
	return b
}

// WithTCPKeepalive enables TCP keepalive probes with period; zero turns them
// off. Default off.
//
// The default is applied best-effort, as for WithNagle: a connection that
// does not expose SetKeepAlive keeps the OS's setting unless this setter was
// called.
func (b *ConfigBuilder) WithTCPKeepalive(period time.Duration) *ConfigBuilder {
	b.settings.SocketKeepalive = period
	b.settings.ChosenSocketOptions |= implSettings.SocketKeepalive
	return b
}

// WithReceiveBuffer sets SO_RCVBUF in bytes; zero keeps the OS's. Default
// the OS's.
func (b *ConfigBuilder) WithReceiveBuffer(bytes int) *ConfigBuilder {
	b.settings.ReceiveBuffer = bytes
	return b
}

// WithSendBuffer sets SO_SNDBUF in bytes; zero keeps the OS's. Default the
// OS's.
func (b *ConfigBuilder) WithSendBuffer(bytes int) *ConfigBuilder {
	b.settings.SendBuffer = bytes
	return b
}

// WithLinger sets SO_LINGER in whole seconds; WithLinger(0) resets connections on
// close. Default the OS's.
func (b *ConfigBuilder) WithLinger(d time.Duration) *ConfigBuilder {
	b.settings.Linger = cql.Some(d)
	return b
}

// WithReadStallTimeout drops a connection that delivers no bytes for d, so
// the requests waiting on a node that stopped answering fail and can retry
// elsewhere. It bounds a stalled socket, not a request: a request's time
// limit is the profile's ProfileBuilder.WithTimeout. Default none.
func (b *ConfigBuilder) WithReadStallTimeout(d time.Duration) *ConfigBuilder {
	b.settings.ReadTimeout = d
	return b
}

// WithWriteStallTimeout drops a connection that accepts no bytes for d. It
// bounds a stalled socket, not a request: a request's time limit is the
// profile's ProfileBuilder.WithTimeout. Default none.
func (b *ConfigBuilder) WithWriteStallTimeout(d time.Duration) *ConfigBuilder {
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

// WithConnectionsPerShard keeps n connections to every shard of each node, so
// each request goes out on a connection of the shard that owns its data. A
// node that reports no shards, such as Cassandra, counts as one shard. n must
// be positive. It replaces WithConnectionsPerHost. Default 1.
func (b *ConfigBuilder) WithConnectionsPerShard(n int) *ConfigBuilder {
	b.perHost = false
	b.settings.Pool.ConnectionsPerShard, b.settings.Pool.ConnectionsPerHost = n, 0
	return b
}

// WithConnectionsPerHost keeps n connections to each node regardless of its
// shard count. n must be positive. It replaces WithConnectionsPerShard.
//
// On ScyllaDB it turns shard-aware connection management off: connections are
// dialed through the regular port only, never the shard-aware port (as with
// WithAdvancedShardAwareness(false)), so the node assigns each connection's shard and
// some shards may get none. A request whose shard has no connection goes out
// on another shard's connection and costs the node a cross-shard hop. There
// are no per-shard excess connections, so WithMaxExcessPerShard does not
// apply. Build rejects it together with WithAdvancedShardAwareness(true) or
// WithMaxExcessPerShard.
func (b *ConfigBuilder) WithConnectionsPerHost(n int) *ConfigBuilder {
	b.perHost = true
	b.settings.Pool.ConnectionsPerHost, b.settings.Pool.ConnectionsPerShard = n, 0
	return b
}

// WithMaxExcessPerShard caps connections opened beyond the target per shard.
// It applies to WithConnectionsPerShard pools only. A Cassandra node counts
// as one shard, so there it caps the excess per node. Default 2.
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
// connection is recycled; zero recycles it on the first. Default 1024.
func (b *ConfigBuilder) WithMaxOrphanedStreams(n int) *ConfigBuilder {
	b.settings.Pool.MaxOrphanedStreams = n
	return b
}

// WithAdvancedShardAwareness chooses whether each new connection picks its
// shard by dialing the node's shard-aware port from a local source port of
// that shard, taken from WithShardAwareSourcePorts (true), or connects through
// the regular port and lets the node assign the shard, the pool dialing until
// every shard is covered (false). Default true, except under
// WithConnectionsPerHost and WithClientRoutes, where it is false unless
// chosen. Choose true under WithClientRoutes only when every route endpoint
// forwards to ScyllaDB's Proxy Protocol v2 shard-aware listener and passes
// the client's source port, since NAT rewrites the port and loses the shard.
// Cassandra has no shards, so there it has no effect.
func (b *ConfigBuilder) WithAdvancedShardAwareness(on bool) *ConfigBuilder {
	b.shardAwarenessChosen = true
	b.settings.Pool.AdvancedShardAwareness.Enabled = on
	return b
}

// WithShardAwareSourcePorts sets the local source ports, first to last,
// that advanced shard awareness dials from. Build rejects a range that is
// empty or starts below 1024. It does not turn advanced shard awareness on:
// Build rejects it where advanced shard awareness is off, under
// WithConnectionsPerHost, WithAdvancedShardAwareness(false) or
// WithClientRoutes, and with a Cassandra dialect, which has no shards, as a
// cqlerr.UnsupportedSettingError. Default 49152-65535.
func (b *ConfigBuilder) WithShardAwareSourcePorts(first, last uint16) *ConfigBuilder {
	b.sourcePortsChosen = true
	b.settings.Pool.AdvancedShardAwareness.SourcePortStart = first
	b.settings.Pool.AdvancedShardAwareness.SourcePortEnd = last
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

// WithLocalDatacenter sets the session's local datacenter. Every built-in
// routing policy whose routing.Options name no datacenter of their own, the
// default profile's and every other profile's alike, prefers replicas there
// and, unless AllowRemote is set, routes only there. Connect fails when the
// cluster has no eligible host in dc. Set it on a cluster with more than one
// datacenter: the default LOCAL_QUORUM consistency is local to the
// coordinator's datacenter. Default none: every datacenter is local, and
// Connect logs a warning when the cluster has more than one.
func (b *ConfigBuilder) WithLocalDatacenter(dc string) *ConfigBuilder {
	b.settings.LocalDatacenter = dc
	return b
}

// WithLocalRack sets the session's local rack within WithLocalDatacenter,
// which the same built-in policies prefer within the local datacenter.
// Connect fails when the datacenter has no eligible host in rack; Build
// rejects a rack without a datacenter. Default none: every rack of the local
// datacenter is local.
func (b *ConfigBuilder) WithLocalRack(rack string) *ConfigBuilder {
	b.settings.LocalRack = rack
	return b
}

// WithHostFilter skips nodes for which keep returns false. Default all nodes.
func (b *ConfigBuilder) WithHostFilter(keep func(metadata.HostSnapshot) bool) *ConfigBuilder {
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
// endpoint.Peer; an error fails that connection attempt. Build rejects it
// together with WithClientRoutes, which translates addresses itself. Default
// identity.
func (b *ConfigBuilder) WithAddressTranslator(translator endpoint.Translator) *ConfigBuilder {
	b.settings.AddressTranslator = translator
	return b
}

// WithClientRoutes connects through ScyllaDB client routes, one per client
// connection, in order. Each call replaces the complete client routes
// setting; no routes removes them. Build rejects an empty or repeated
// connection ID, and client routes together with WithAddressTranslator. Only
// ScyllaDB has client routes: Build with a Cassandra dialect fails with a
// cqlerr.UnsupportedSettingError. Default none.
func (b *ConfigBuilder) WithClientRoutes(routes ...ClientRoute) *ConfigBuilder {
	b.routes = append([]ClientRoute(nil), routes...)
	return b
}

// WithReconnect sets the delay between attempts to reopen a node's
// connections; see endpoint.ReconnectPolicy for how the driver calls it.
// Build calls Delay(1) once and rejects a negative delay. A nil policy
// restores the default. endpoint.ReconnectFunc adapts a function; to never
// reconnect, pass one that declines:
//
//	endpoint.ReconnectFunc(func(int) (time.Duration, bool) { return 0, false })
//
// Default jittered backoff from 1s to 32s.
func (b *ConfigBuilder) WithReconnect(policy endpoint.ReconnectPolicy) *ConfigBuilder {
	if policy == nil {
		policy = implSettings.DefaultReconnect()
	}
	b.settings.Reconnect = policy
	return b
}

// WithConviction decides when connection errors mark a node down. A nil
// policy restores the default, endpoint.SimpleConvictionPolicy: a node is
// down once opening its connections leaves it with none, or the control
// connection fails to dial it.
func (b *ConfigBuilder) WithConviction(policy endpoint.ConvictionPolicy) *ConfigBuilder {
	b.settings.Conviction = policy
	return b
}

// WithMetadataMode selects how much schema metadata is loaded. Default
// metadata.ModeFull. Loading less costs routing: the metadata.Mode values say
// what each mode keeps.
func (b *ConfigBuilder) WithMetadataMode(mode metadata.Mode) *ConfigBuilder {
	b.settings.Metadata.Mode = mode
	return b
}

// WithMetadataKeyspaces limits schema metadata to keyspaces. Build rejects it
// under metadata.ModeNone, which loads no schema. Connect logs a warning when
// the session keyspace (WithKeyspace) is not among them. Default all.
func (b *ConfigBuilder) WithMetadataKeyspaces(keyspaces ...string) *ConfigBuilder {
	b.settings.Metadata.Keyspaces = append([]string(nil), keyspaces...)
	return b
}

// WithPartitioner sets the token partitioner for the partitioner class name,
// next to the built-in Murmur3 and CDC partitioners; a built-in name is
// replaced. A table's partitioner class picks the partitioner, so one that no
// table names, such as the CDC partitioner on Cassandra, is never used.
// Build rejects an empty name or a nil partitioner. Default the built-in
// partitioners only.
func (b *ConfigBuilder) WithPartitioner(name string, partitioner routing.TokenPartitioner) *ConfigBuilder {
	if b.settings.Partitioners == nil {
		b.settings.Partitioners = make(map[string]routing.TokenPartitioner)
	}
	b.settings.Partitioners[name] = partitioner
	return b
}

// WithMetadataRefresh reloads metadata every interval. Zero turns periodic
// reloads off; events and schema agreement still refresh it. Default 60s.
func (b *ConfigBuilder) WithMetadataRefresh(interval time.Duration) *ConfigBuilder {
	b.settings.Metadata.PeriodicRefresh = interval
	return b
}

// WithMetadataTimeout bounds one metadata request; Build rejects zero.
// Default 31s: one second above the 30s USING TIMEOUT that
// WithMetadataServerTimeout derives from it by default, so that on ScyllaDB
// a slow metadata query ends with the server's own error, which names the
// cause, before the driver gives up on it.
func (b *ConfigBuilder) WithMetadataTimeout(d time.Duration) *ConfigBuilder {
	b.settings.Metadata.RequestTimeout = d
	return b
}

// WithMetadataServerTimeout asks ScyllaDB to bound metadata queries with USING
// TIMEOUT, in whole milliseconds; zero sends none. It must be below
// WithMetadataTimeout, so that the server gives up first and answers with its
// own error; Build rejects it otherwise. Cassandra has no USING TIMEOUT, so
// Build with a Cassandra dialect fails on a positive value with a
// cqlerr.UnsupportedSettingError. Default one second below WithMetadataTimeout
// (half of it at 2s or less) on ScyllaDB, none on Cassandra.
func (b *ConfigBuilder) WithMetadataServerTimeout(d time.Duration) *ConfigBuilder {
	b.settings.Metadata.ServerTimeout = cql.Some(d)
	return b
}

// WithIgnoreEvents stops listening for the given server events:
// event.KindTopology, event.KindStatus, event.KindSchema or
// event.KindClientRoute. Each call replaces the complete ignored-event list;
// no arguments restores all events. Default none ignored.
func (b *ConfigBuilder) WithIgnoreEvents(kinds ...event.Kind) *ConfigBuilder {
	b.ignoredEvents = append([]event.Kind(nil), kinds...)
	return b
}

// WithSchemaAgreement polls every poll, for up to timeout, for nodes to agree
// on the schema. The period comes first, as in WithHeartbeat. Default 200ms
// and 60s.
func (b *ConfigBuilder) WithSchemaAgreement(poll, timeout time.Duration) *ConfigBuilder {
	b.settings.Metadata.AgreementTimeout = timeout
	b.settings.Metadata.AgreementPoll = poll
	return b
}

// WithSchemaChangeWait chooses what a statement that changes the schema
// waits for before it returns: nothing, schema agreement among the nodes
// (WithSchemaAgreement bounds the wait), or agreement and then a reload of
// the changed keyspace's metadata, so Session.Metadata shows the change.
// Build rejects a value it does not define. Default SchemaChangeWaitRefresh.
func (b *ConfigBuilder) WithSchemaChangeWait(wait SchemaChangeWait) *ConfigBuilder {
	b.schemaChangeWait = wait
	return b
}

// WithDefaultProfile replaces the complete execution defaults for statements
// that pass no Profile with the ones profile has chosen so far:
//
//	config.New(config.ScyllaV4, "10.0.0.1").
//		WithDefaultProfile(config.NewProfile().WithConsistency(cql.One))
//
// The builder is copied now, so later calls on it do not reach this
// configuration; policy objects stay shared. Build validates the profile as
// ProfileBuilder.Build does, and the cql.ValidationError it returns names the
// profile's setter, such as "WithPageSize", in its Field; a zero
// ProfileBuilder fails with Field "WithDefaultProfile". To change a few
// fields of a built configuration's profile, derive the builder with
// Config.DefaultProfile and Profile.Builder. A nil profile restores the
// default, the profile NewProfile starts.
func (b *ConfigBuilder) WithDefaultProfile(profile *ProfileBuilder) *ConfigBuilder {
	if profile == nil {
		b.profile = NewProfile()
		return b
	}
	copied := *profile
	b.profile = &copied
	return b
}

// WithPreparedCacheSize keeps up to n prepared statements, so preparing the
// same CQL again reuses the cached statement. Zero turns the cache off: every
// Session.Prepare prepares anew. Default 1000.
func (b *ConfigBuilder) WithPreparedCacheSize(n int) *ConfigBuilder {
	b.settings.PreparedCacheSize = n
	return b
}

// WithTimestampGenerator supplies client timestamps: the driver calls next
// once per stamped request and sends the time it returns, truncated to the
// microsecond, as CQL's write timestamps are. Return strictly increasing
// times when two writes of one session to a cell must keep their order. A
// nil next sends no client timestamp, so the server assigns one: unlike
// every other setter, nil does not restore the default. By default the
// driver stamps each request with the time it was admitted, strictly
// increasing per session. Only ordinary statements, and batches of them, are
// stamped: a conditional one, declared or inferred (cql.UnknownStatement), a
// counter write and a request with execopt.Options.Timestamp set take no
// generated timestamp.
func (b *ConfigBuilder) WithTimestampGenerator(next func() time.Time) *ConfigBuilder {
	if next == nil {
		b.settings.Timestamps = implSettings.GeneratedTimestamps(nil)
		return b
	}
	b.settings.Timestamps = implSettings.GeneratedTimestamps(func() int64 { return next().UnixMicro() })
	return b
}

// WithApplication reports the application's name and version in STARTUP.
// Build rejects a version without a name; empty name and version report
// neither.
func (b *ConfigBuilder) WithApplication(name, version string) *ConfigBuilder {
	b.settings.Identity.ApplicationName = name
	b.settings.Identity.ApplicationVersion = version
	return b
}

// WithClientID reports id in STARTUP. Default none.
func (b *ConfigBuilder) WithClientID(id string) *ConfigBuilder {
	b.settings.Identity.ClientID = id
	return b
}

// WithLogger receives the driver's structured log records. Default slog.Default().
// Use slog.New(slog.DiscardHandler) to silence the driver, or a Debug-level handler for
// per-connection detail. To log through a *log.Logger l at level lvl, pass
//
//	slog.New(slog.NewTextHandler(l.Writer(), &slog.HandlerOptions{Level: lvl}))
//
// Records at Info and above are rate-limited per message and host (see
// WithLogRateLimit); handler panics are contained.
func (b *ConfigBuilder) WithLogger(logger *slog.Logger) *ConfigBuilder {
	b.settings.Logger = logger
	return b
}

// WithLogRateLimit collapses log records of Info and above that repeat the
// same level, message and host within window into the first one; the next
// one after the window carries a suppressed count of those dropped. Debug
// records are never limited. Zero turns the limit off, so every record
// reaches the logger; Build rejects a negative window. Default 10s.
func (b *ConfigBuilder) WithLogRateLimit(window time.Duration) *ConfigBuilder {
	b.settings.LogRateLimit = window
	return b
}

// WithObserver installs hooks that observe every request and attempt of the
// session; observe.Join combines several. The zero Hooks observes nothing.
// Default none.
func (b *ConfigBuilder) WithObserver(hooks observe.Hooks) *ConfigBuilder {
	b.settings.Observer = hooks
	return b
}

// WithConfigReport chooses whether the driver configuration is sent to the
// server, as JSON in the DRIVER_CONFIG STARTUP option of the control
// connection, following docs/driver-config-schema.json. The report carries no
// credentials, TLS material, addresses or host names, and its version field
// changes only on an incompatible change. Default true.
func (b *ConfigBuilder) WithConfigReport(on bool) *ConfigBuilder {
	b.settings.ReportConfiguration = on
	return b
}

// WithErrorStatementText makes request errors carry the statement they ran:
// cqlerr.ExecutionError's Statement and BatchSize, and its message. A
// batch is described by its statement count and its first statement; a
// prepared statement by the CQL text it was prepared from. Text longer than
// maxBytes is cut at a character boundary to at most maxBytes bytes and "..."
// is appended. Bound values are never included, but literals written into the
// CQL text are, which is why it is off by default: error messages end up in
// logs and reports that may not be cleared for application data. Zero turns
// it off; a few hundred bytes identify most statements. Default 0.
func (b *ConfigBuilder) WithErrorStatementText(maxBytes int) *ConfigBuilder {
	b.settings.ErrorStatementBytes = maxBytes
	return b
}

// String lists the effective settings. Credentials and TLS material are not
// printed.
func (c Config) String() string {
	if c.settings == nil {
		return "config.Config{}"
	}
	s := c.settings
	var w strings.Builder
	fmt.Fprintf(&w, "dialect=%v contact points=%v port=%d keyspace=%q compression=%v", Dialect(implSettings.DialectOf(s.Server, s.Protocol)), s.ContactPoints, s.Port, s.Keyspace, s.Compression)
	fmt.Fprintf(&w, " local datacenter=%q local rack=%q", s.LocalDatacenter, s.LocalRack)
	fmt.Fprintf(&w, " tls=%t tls port=%d tls fallback=%t auth=%t", s.TLS != nil, s.TLSPort, s.TLSFallback, s.Auth != nil)
	fmt.Fprintf(&w, " address translator=%t client routes=%d", s.AddressTranslator != nil, clientRouteCount(s.ClientRoutes))
	fmt.Fprintf(&w, " connect timeout=%v dns timeout=%v read stall timeout=%v write stall timeout=%v", s.ConnectTimeout, s.DNSTimeout, s.ReadTimeout, s.WriteTimeout)
	fmt.Fprintf(&w, " heartbeat=%v/%v metadata timeout=%v", s.KeepaliveInterval, s.KeepaliveTimeout, s.Metadata.RequestTimeout)
	fmt.Fprintf(&w, " connections per shard=%d per host=%d advanced shard awareness=%t", s.Pool.ConnectionsPerShard, s.Pool.ConnectionsPerHost, s.Pool.AdvancedShardAwareness.Enabled)
	fmt.Fprintf(&w, " discovery=%t metadata=%v prepared cache=%d", s.Metadata.InitialDiscovery, s.Metadata.Mode, s.PreparedCacheSize)
	if p := s.DefaultProfile; p != nil {
		fmt.Fprintf(&w, " consistency=%v timeout=%v page size=%d", p.Consistency, p.Timeout, p.PageSize)
	}
	return w.String()
}

// DefaultProfile returns the execution defaults of statements that pass no
// Profile: the profile WithDefaultProfile chose, or the driver's. Derive
// another profile from it with Profile.Builder. The zero Config returns the
// zero Profile, which is not built.
func (c Config) DefaultProfile() Profile {
	if c.settings == nil || c.settings.DefaultProfile == nil {
		return Profile{}
	}
	return publicProfile(*c.settings.DefaultProfile)
}

// Observer returns the hooks WithObserver chose, which a session connected
// with c also returns from Session.Observer; the zero Hooks without them or
// for the zero Config.
func (c Config) Observer() observe.Hooks {
	if c.settings == nil {
		return observe.Hooks{}
	}
	return c.settings.Observer
}

// resolved returns the settings to connect with, or an error for the zero
// Config.
func (c Config) resolved() (implSettings.Config, error) {
	if c.settings == nil {
		return implSettings.Config{}, &cql.ValidationError{Field: "Config", Err: errors.New("zero Config; build one with config.New")}
	}
	return *c.settings, nil
}

// ProfileBuilder assembles a Profile. Settings that are not chosen keep the
// driver's defaults: LOCAL_QUORUM, LOCAL_SERIAL, 30s timeout, 5000-row pages,
// one page prefetched, token-aware routing and safe retries. The last call
// for a setting wins. Start one with NewProfile: a zero ProfileBuilder has
// none of those defaults, and Build rejects it with a cql.ValidationError.
type ProfileBuilder struct {
	profile implSettings.Profile
	// started reports that NewProfile or ConfigBuilder made the builder.
	started bool
}

// NewProfile starts a profile from the driver's defaults, which
// ProfileBuilder lists, not from any session's: to change a few settings of
// the profile a session runs with, for execopt.Options.Profile, start from
// gocql.Session.DefaultProfile().Builder() or Config.DefaultProfile().Builder()
// instead.
func NewProfile() *ProfileBuilder {
	return &ProfileBuilder{profile: *implSettings.DefaultConfig().DefaultProfile, started: true}
}

// MustBuild is Build that panics on error, for package-level profiles and
// profiles written from constants.
func (p *ProfileBuilder) MustBuild() Profile {
	profile, err := p.Build()
	if err != nil {
		panic(err)
	}
	return profile
}

// Build validates the profile, for use as Options.Profile.
func (p *ProfileBuilder) Build() (Profile, error) {
	if !p.started {
		return Profile{}, &cql.ValidationError{Field: "profile", Err: errors.New("zero ProfileBuilder; start one with config.NewProfile")}
	}
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

// WithTimeout bounds one execution, including retries and every page Rows
// fetches. A set execopt.Options.Timeout replaces it for one execution, and the
// call's context can end the execution sooner. Zero leaves executions bounded
// only by their context. Default 30s.
func (p *ProfileBuilder) WithTimeout(d time.Duration) *ProfileBuilder {
	p.profile.Timeout = d
	return p
}

// WithPageTimeout bounds fetching one page; a set execopt.Options.PageTimeout
// replaces it for one execution. Default none.
func (p *ProfileBuilder) WithPageTimeout(d time.Duration) *ProfileBuilder {
	p.profile.PageTimeout = d
	return p
}

// WithPageSize sets the rows per page, 1 to cql.Unpaged; cql.Unpaged requests
// the whole result in one response. Build rejects any other value. Default
// 5000.
func (p *ProfileBuilder) WithPageSize(rows int) *ProfileBuilder {
	p.profile.InternalSetPageSize(rows)
	return p
}

// WithPrefetch chooses whether Rows request the next page as soon as a page
// arrives (true), so they hold at most two response pages, or only when the
// current one is consumed (false). Each, All, One, RunPage and streams never
// prefetch. Default true.
func (p *ProfileBuilder) WithPrefetch(on bool) *ProfileBuilder {
	p.profile.Prefetch = prefetchPages(on)
	return p
}

// WithRetry sets the retry policy. A nil policy restores the default;
// retry.Never{} disables retries. Default retry.Safe{MaxAttempts: 3}.
func (p *ProfileBuilder) WithRetry(policy retry.Policy) *ProfileBuilder {
	if policy == nil {
		policy = implSettings.InternalDefaultProfile().Retry
	}
	p.profile.Retry = policy
	return p
}

// WithRouting sets the routing policy. A nil policy restores the default:
// routing.NewPolicy(routing.Options{}), token-aware with shuffled replicas
// and leader-aware for strongly consistent tables, preferring the session's
// WithLocalDatacenter.
func (p *ProfileBuilder) WithRouting(policy routing.Policy) *ProfileBuilder {
	if policy == nil {
		p.profile.Routing = implSettings.DefaultRouting()
		return p
	}
	p.profile.Routing = implRouting.EngineOf(policy)
	return p
}

// WithIdempotent chooses whether every statement the profile runs is safe to
// run more than once, as cql.Statement.Idempotent marks one statement: with
// true, retry.Safe retries and speculation applies to all of them. With
// false, each statement's own Idempotent decides. execopt.Options.Idempotent
// overrides both for one execution. It is v1's ClusterConfig
// DefaultIdempotence. Default false.
func (p *ProfileBuilder) WithIdempotent(on bool) *ProfileBuilder {
	p.profile.Idempotent = on
	return p
}

// WithCQLTraceSampler sets a function the driver asks, once per request as
// it is admitted, whether to record a server-side CQL trace session of the
// request's attempts, so that a sample of production traffic can be traced
// without setting execopt.Options.CQLTrace on chosen calls. It receives the
// request's routing.Info: its keyspace, table, token, consistency and kind.
// A request whose options set CQLTrace is traced without asking. The
// function is shared by every request of the profile and must be safe for
// concurrent use and fast: it runs on every request. Tracing writes to
// system_traces on the cluster, so sample sparingly, such as one request in
// a thousand. Default nil, which traces only what CQLTrace asks for.
//
//	var n atomic.Uint64
//	profile := session.DefaultProfile().Builder().
//		WithCQLTraceSampler(func(routing.Info) bool { return n.Add(1)%1000 == 0 }).
//		MustBuild()
func (p *ProfileBuilder) WithCQLTraceSampler(sample func(routing.Info) bool) *ProfileBuilder {
	p.profile.CQLTraceSampler = sample
	return p
}

// WithSpeculation sets the speculative execution policy; nil disables it.
// Default none. Only an idempotent execution of ordinary statements
// speculates; see retry.SpeculationPolicy.
func (p *ProfileBuilder) WithSpeculation(policy retry.SpeculationPolicy) *ProfileBuilder {
	p.profile.Speculation = policy
	return p
}
