// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/netip"
	"slices"
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
// A Config shares nothing its builder or caller can change, so it is safe to
// reuse for many sessions and from many goroutines.
type Config struct {
	settings *implSettings.Config
}

// ProtocolVersion selects the native protocol version.
type ProtocolVersion uint8

const (
	// ProtocolAuto negotiates the highest version both sides support.
	ProtocolAuto ProtocolVersion = 0
	Protocol3    ProtocolVersion = 3
	Protocol4    ProtocolVersion = 4
	Protocol5    ProtocolVersion = 5
)

// choices records which settings the caller chose, so Build infers only the
// ones left to the driver, and collects invalid calls for Build to report.
type choices struct {
	chosen map[string]bool
	errs   []error
}

func (c *choices) choose(key string) {
	if c.chosen == nil {
		c.chosen = make(map[string]bool)
	}
	c.chosen[key] = true
}

func (c *choices) has(key string) bool { return c.chosen[key] }

func (c *choices) fail(call, reason string) {
	c.errs = append(c.errs, &cqlerr.ValidationError{Field: call, Err: errors.New(reason)})
}

// ConfigBuilder assembles a Config. Settings that are not chosen keep the
// driver's behavior; when a setting is chosen again, the last call wins.
// Features that are on by default are turned off by No* methods; features
// that are off by default have no off switch. Methods never fail: invalid
// arguments are reported by Build and leave the setting unchanged. A
// ConfigBuilder is not safe for concurrent use.
type ConfigBuilder struct {
	profile *ProfileBuilder
	routes  *ClientRoutesBuilder
	choices
	settings implSettings.Config
}

// NewConfig starts a configuration that connects to contactPoints. A contact
// point is host[:port], or a single absolute Unix socket path (optionally
// prefixed with "unix:"); for a socket, protocol 3 and no host discovery are
// filled in unless chosen.
func NewConfig(contactPoints ...string) *ConfigBuilder {
	b := &ConfigBuilder{settings: implSettings.DefaultConfig(contactPoints...), profile: NewProfile()}
	if len(contactPoints) == 0 {
		b.fail("NewConfig()", "at least one contact point is required")
	}
	return b
}

// Build validates every setting and returns the Config, or an error joining
// every problem found. Later calls on the builder do not affect the returned
// Config.
func (b *ConfigBuilder) Build() (Config, error) {
	errs := append([]error(nil), b.errs...)
	settings := b.settings
	profile, err := b.profile.build("DefaultProfile")
	if err != nil {
		errs = append(errs, err)
	}
	settings.DefaultProfile = &profile
	if b.routes != nil {
		routes, err := b.routes.build()
		if err != nil {
			errs = append(errs, err)
		}
		settings.ClientRoutes = &routes
	}
	if path, _ := implSettings.InternalUnixSocketPath(settings.ContactPoints); path != "" {
		if !b.has("protocol") {
			settings.Protocol = int(Protocol3)
		}
		if !b.has("discovery") {
			settings.Metadata.InitialDiscovery = false
		}
	}
	if settings.TLSFiles != nil {
		if err := loadTLSFiles(&settings); err != nil {
			errs = append(errs, err)
		}
	}
	if settings.ClientRoutes != nil && !b.has("advanced shard awareness") {
		settings.Pool.ShardAwarePort = false
	}
	if len(errs) == 0 {
		if err := implSettings.InternalValidateConfig(settings); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	resolved := implSettings.InternalSnapshotConfig(settings)
	return Config{settings: &resolved}, nil
}

// loadTLSFiles makes the files' content settings.TLS, keeping the config it
// replaces as the base later reloads apply the files to.
func loadTLSFiles(settings *implSettings.Config) error {
	base := settings.TLS
	if err := implSettings.CheckTLSFilesBase(base, *settings.TLSFiles); err != nil {
		return err
	}
	content, err := implSettings.ReadTLSFiles(*settings.TLSFiles)
	if err != nil {
		return err
	}
	config, err := implSettings.BuildTLSFromFiles(base, content)
	if err != nil {
		return err
	}
	settings.TLSFilesBase = base
	settings.TLS = config
	return nil
}

// set applies a choice of key when valid; otherwise it records reason as
// call's error and leaves the setting unchanged.
func (b *ConfigBuilder) set(key, call string, valid bool, reason string, apply func()) *ConfigBuilder {
	if !valid {
		b.fail(call, reason)
		return b
	}
	b.choose(key)
	apply()
	return b
}

// Port sets the port used for contact points that name none. Default 9042.
func (b *ConfigBuilder) Port(port uint16) *ConfigBuilder {
	return b.set("port", fmt.Sprintf("Port(%d)", port), port != 0, "must not be zero", func() { b.settings.Port = port })
}

// Keyspace sets the keyspace every connection uses. Default none.
func (b *ConfigBuilder) Keyspace(name string) *ConfigBuilder {
	return b.set("keyspace", fmt.Sprintf("Keyspace(%q)", name), name != "", "must not be empty", func() { b.settings.Keyspace = name })
}

// Auth sets the authenticator. Default none.
func (b *ConfigBuilder) Auth(provider auth.Provider) *ConfigBuilder {
	return b.set("auth", "Auth(...)", provider != nil, "provider is required", func() { b.settings.Auth = provider })
}

// TLS encrypts every connection with config, which is copied. Unless config
// skips verification or names a server, each connection verifies the node's
// IP address. Default plaintext.
func (b *ConfigBuilder) TLS(config *tls.Config) *ConfigBuilder {
	return b.set("tls", "TLS(...)", config != nil, "config is required", func() { b.settings.TLS = config.Clone() })
}

// TLSFiles names PEM files for ConfigBuilder.TLSFiles.
type TLSFiles = implSettings.TLSFiles

// TLSFiles loads the client certificate and key, and the CA certificates that
// replace RootCAs, from files, and has the session reload them whenever their
// content changes. Other TLS settings come from TLS, or are TLS 1.2+ defaults
// when TLS is not called. Build fails if the files cannot be loaded; a reload
// that fails keeps the previous files. Open connections keep the certificate
// they were dialed with; see Session.UpdateTLS.
func (b *ConfigBuilder) TLSFiles(files TLSFiles) *ConfigBuilder {
	reason := files.Check()
	return b.set("tls files", "TLSFiles(...)", reason == "", reason, func() { b.settings.TLSFiles = &files })
}

// ProtocolVersion pins the native protocol version. Default ProtocolAuto, or
// Protocol3 for a Unix socket.
func (b *ConfigBuilder) ProtocolVersion(version ProtocolVersion) *ConfigBuilder {
	valid := version == ProtocolAuto || version == Protocol3 || version == Protocol4 || version == Protocol5
	return b.set("protocol", fmt.Sprintf("ProtocolVersion(%d)", version), valid, "supported versions are 3, 4 and 5", func() { b.settings.Protocol = int(version) })
}

// Compression sets frame compression. Default none.
func (b *ConfigBuilder) Compression(compression Compression) *ConfigBuilder {
	return b.set("compression", fmt.Sprintf("Compression(%d)", compression), compression <= SnappyCompression, "unknown algorithm", func() { b.settings.Compression = compression })
}

// CQLVersion sets the CQL version sent in STARTUP. Default "3.0.0".
func (b *ConfigBuilder) CQLVersion(version string) *ConfigBuilder {
	return b.set("cql version", fmt.Sprintf("CQLVersion(%q)", version), version != "", "must not be empty", func() { b.settings.CQLVersion = version })
}

// MaxFrameSize caps a received frame's size in bytes. Default 256 MiB.
func (b *ConfigBuilder) MaxFrameSize(bytes int64) *ConfigBuilder {
	return b.set("max frame size", fmt.Sprintf("MaxFrameSize(%d)", bytes), bytes > 0, "must be positive", func() { b.settings.Limits.MaxFrameBytes = bytes })
}

// MaxDecompressedSize caps a frame's decompressed size in bytes. Default 256 MiB.
func (b *ConfigBuilder) MaxDecompressedSize(bytes int64) *ConfigBuilder {
	return b.set("max decompressed size", fmt.Sprintf("MaxDecompressedSize(%d)", bytes), bytes > 0, "must be positive", func() { b.settings.Limits.MaxDecompressedBytes = bytes })
}

// MaxMetadataDepth caps the nesting depth of result type metadata. Default 64.
func (b *ConfigBuilder) MaxMetadataDepth(depth int) *ConfigBuilder {
	return b.set("max metadata depth", fmt.Sprintf("MaxMetadataDepth(%d)", depth), depth > 0, "must be positive", func() { b.settings.Limits.MaxMetadataDepth = depth })
}

// MaxPagingStates caps the paging states a session keeps. Default 65536.
func (b *ConfigBuilder) MaxPagingStates(n int) *ConfigBuilder {
	return b.set("max paging states", fmt.Sprintf("MaxPagingStates(%d)", n), n > 0, "must be positive", func() { b.settings.Limits.MaxPagingStates = n })
}

// MaxPagingStateSize caps one paging state's size in bytes. Default 16 MiB.
func (b *ConfigBuilder) MaxPagingStateSize(bytes int64) *ConfigBuilder {
	return b.set("max paging state size", fmt.Sprintf("MaxPagingStateSize(%d)", bytes), bytes > 0, "must be positive", func() { b.settings.Limits.MaxPagingStateBytes = bytes })
}

// Dialer opens raw connections; TLS is applied afterward. Use
// endpoint.DialerFunc to adapt a function. Default net.Dialer.
func (b *ConfigBuilder) Dialer(dialer endpoint.Dialer) *ConfigBuilder {
	return b.set("dialer", "Dialer(...)", dialer != nil, "dialer is required", func() { b.settings.Dialer = dialer })
}

// Resolver resolves contact point names. Default the system resolver.
func (b *ConfigBuilder) Resolver(resolver endpoint.DNSResolver) *ConfigBuilder {
	return b.set("resolver", "Resolver(...)", resolver != nil, "resolver is required", func() { b.settings.DNSResolver = resolver })
}

// LocalAddress sets the source address of every connection. Default any.
func (b *ConfigBuilder) LocalAddress(address netip.Addr) *ConfigBuilder {
	return b.set("local address", fmt.Sprintf("LocalAddress(%v)", address), address.IsValid(), "address is required", func() { b.settings.LocalAddress = address })
}

// ConnectTimeout bounds opening one connection. Default 5s.
func (b *ConfigBuilder) ConnectTimeout(d time.Duration) *ConfigBuilder {
	return b.set("connect timeout", fmt.Sprintf("ConnectTimeout(%v)", d), d > 0, "must be positive", func() { b.settings.ConnectTimeout = d })
}

// DNSTimeout bounds one name lookup. Default 5s.
func (b *ConfigBuilder) DNSTimeout(d time.Duration) *ConfigBuilder {
	return b.set("dns timeout", fmt.Sprintf("DNSTimeout(%v)", d), d > 0, "must be positive", func() { b.settings.DNSTimeout = d })
}

// Nagle keeps Nagle's algorithm on. By default TCP_NODELAY turns it off.
func (b *ConfigBuilder) Nagle() *ConfigBuilder {
	return b.set("nagle", "Nagle()", true, "", func() { b.settings.NoDelay = false })
}

// TCPKeepalive enables TCP keepalive probes with period. Default off.
func (b *ConfigBuilder) TCPKeepalive(period time.Duration) *ConfigBuilder {
	return b.set("tcp keepalive", fmt.Sprintf("TCPKeepalive(%v)", period), period > 0, "must be positive", func() { b.settings.SocketKeepalive = period })
}

// SocketBuffers sets SO_RCVBUF and SO_SNDBUF in bytes. Default the OS's.
func (b *ConfigBuilder) SocketBuffers(receive, send int) *ConfigBuilder {
	return b.set("socket buffers", fmt.Sprintf("SocketBuffers(%d, %d)", receive, send), receive > 0 && send > 0, "must be positive", func() {
		b.settings.ReceiveBuffer = cql.Some(receive)
		b.settings.SendBuffer = cql.Some(send)
	})
}

// Linger sets SO_LINGER in whole seconds; Linger(0) resets connections on
// close. Default the OS's.
func (b *ConfigBuilder) Linger(d time.Duration) *ConfigBuilder {
	return b.set("linger", fmt.Sprintf("Linger(%v)", d), d >= 0 && d%time.Second == 0, "must be nonnegative whole seconds", func() { b.settings.Linger = cql.Some(d) })
}

// ReadTimeout drops a connection that delivers no bytes for d. Default none.
func (b *ConfigBuilder) ReadTimeout(d time.Duration) *ConfigBuilder {
	return b.set("read timeout", fmt.Sprintf("ReadTimeout(%v)", d), d > 0, "must be positive", func() { b.settings.ReadTimeout = d })
}

// WriteTimeout drops a connection that accepts no bytes for d. Default none.
func (b *ConfigBuilder) WriteTimeout(d time.Duration) *ConfigBuilder {
	return b.set("write timeout", fmt.Sprintf("WriteTimeout(%v)", d), d > 0, "must be positive", func() { b.settings.WriteTimeout = d })
}

// Heartbeat sends a CQL OPTIONS request on idle connections every interval
// and drops a connection whose heartbeat takes longer than timeout. It
// replaces NoHeartbeat. Default 30s and 30s.
func (b *ConfigBuilder) Heartbeat(interval, timeout time.Duration) *ConfigBuilder {
	return b.set("heartbeat", fmt.Sprintf("Heartbeat(%v, %v)", interval, timeout), interval > 0 && timeout > 0, "must be positive", func() {
		b.settings.KeepaliveInterval = interval
		b.settings.KeepaliveTimeout = timeout
	})
}

// NoHeartbeat sends no heartbeats. It replaces Heartbeat.
func (b *ConfigBuilder) NoHeartbeat() *ConfigBuilder {
	return b.set("heartbeat", "NoHeartbeat()", true, "", func() { b.settings.KeepaliveInterval = 0 })
}

// CoalesceWrites delays writes up to wait to send them together. Any wait
// turns off the inline execution path, so it costs latency on a healthy
// cluster. Default off.
func (b *ConfigBuilder) CoalesceWrites(wait time.Duration) *ConfigBuilder {
	return b.set("coalesce writes", fmt.Sprintf("CoalesceWrites(%v)", wait), wait > 0, "must be positive", func() { b.settings.CoalesceWait = wait })
}

// ConnectionsPerShard opens n connections per shard of each node. It
// replaces ConnectionsPerHost. Default 1.
func (b *ConfigBuilder) ConnectionsPerShard(n int) *ConfigBuilder {
	return b.set("pool size", fmt.Sprintf("ConnectionsPerShard(%d)", n), n > 0, "must be at least 1", func() {
		b.settings.Pool.ConnectionsPerShard = n
		b.settings.Pool.ConnectionsPerHost = 0
	})
}

// ConnectionsPerHost opens n connections per node regardless of its shard
// count. It replaces ConnectionsPerShard.
func (b *ConfigBuilder) ConnectionsPerHost(n int) *ConfigBuilder {
	return b.set("pool size", fmt.Sprintf("ConnectionsPerHost(%d)", n), n > 0, "must be at least 1", func() { b.settings.Pool.ConnectionsPerHost = n })
}

// MaxExcessPerShard caps connections opened beyond the target per shard.
// Default 2.
func (b *ConfigBuilder) MaxExcessPerShard(n int) *ConfigBuilder {
	return b.set("max excess per shard", fmt.Sprintf("MaxExcessPerShard(%d)", n), n >= 0, "must not be negative", func() { b.settings.Pool.MaxExcessPerShard = n })
}

// MaxRequestsPerConnection caps in-flight requests per connection, at most
// 32768. Default 32768.
func (b *ConfigBuilder) MaxRequestsPerConnection(n int) *ConfigBuilder {
	return b.set("max requests per connection", fmt.Sprintf("MaxRequestsPerConnection(%d)", n), n >= 1 && n <= 32768, "must be 1..32768", func() { b.settings.Pool.MaxRequestsPerConnection = n })
}

// MaxOrphanedStreams caps streams abandoned by cancelled requests before a
// connection is recycled. Default 1024.
func (b *ConfigBuilder) MaxOrphanedStreams(n int) *ConfigBuilder {
	return b.set("max orphaned streams", fmt.Sprintf("MaxOrphanedStreams(%d)", n), n >= 0, "must not be negative", func() { b.settings.Pool.MaxOrphanedStreams = n })
}

// AdvancedShardAwareness picks the shard of each new connection by dialing
// the node's shard-aware port from a local source port chosen from first to
// last. It replaces NoAdvancedShardAwareness. Default on with ports
// 49152-65535, except under ClientRoutes.
func (b *ConfigBuilder) AdvancedShardAwareness(first, last uint16) *ConfigBuilder {
	return b.set("advanced shard awareness", fmt.Sprintf("AdvancedShardAwareness(%d, %d)", first, last), first >= 1024 && first <= last, "needs 1024 <= first <= last", func() {
		b.settings.Pool.ShardAwarePort = true
		b.settings.Pool.SourcePortStart, b.settings.Pool.SourcePortEnd = first, last
	})
}

// NoAdvancedShardAwareness connects through the regular port only and lets
// the node assign the shard; the pool keeps dialing until every shard is
// covered. It replaces AdvancedShardAwareness.
func (b *ConfigBuilder) NoAdvancedShardAwareness() *ConfigBuilder {
	return b.set("advanced shard awareness", "NoAdvancedShardAwareness()", true, "", func() { b.settings.Pool.ShardAwarePort = false })
}

// NoDiscovery connects only to the contact points instead of discovering the
// cluster's nodes. Implied for a Unix socket.
func (b *ConfigBuilder) NoDiscovery() *ConfigBuilder {
	return b.set("discovery", "NoDiscovery()", true, "", func() { b.settings.Metadata.InitialDiscovery = false })
}

// HostFilter skips nodes for which keep returns false. Default all nodes.
func (b *ConfigBuilder) HostFilter(keep func(routing.HostSnapshot) bool) *ConfigBuilder {
	return b.set("host filter", "HostFilter(...)", keep != nil, "function is required", func() { b.settings.HostFilter = keep })
}

// AddressTranslator rewrites node addresses before dialing. Default identity.
func (b *ConfigBuilder) AddressTranslator(translator endpoint.Translator) *ConfigBuilder {
	return b.set("address translator", "AddressTranslator(...)", translator != nil, "translator is required", func() { b.settings.AddressTranslator = translator })
}

// ClientRoutes connects through ScyllaDB client routes.
func (b *ConfigBuilder) ClientRoutes(routes *ClientRoutesBuilder) *ConfigBuilder {
	return b.set("client routes", "ClientRoutes(...)", routes != nil, "builder is required", func() { b.routes = routes })
}

// Reconnect sets the delay between attempts to reopen a node's connections.
// Default jittered backoff from 1s to 32s.
func (b *ConfigBuilder) Reconnect(policy ReconnectPolicy) *ConfigBuilder {
	return b.set("reconnect", "Reconnect(...)", policy != nil, "policy is required", func() { b.settings.Reconnect = policy })
}

// Conviction decides when connection errors mark a node down. Default none.
func (b *ConfigBuilder) Conviction(policy ConvictionPolicy) *ConfigBuilder {
	return b.set("conviction", "Conviction(...)", policy != nil, "policy is required", func() { b.settings.Conviction = policy })
}

// MetadataMode selects how much schema metadata is loaded. Default
// metadata.Full.
func (b *ConfigBuilder) MetadataMode(mode metadata.Mode) *ConfigBuilder {
	return b.set("metadata mode", fmt.Sprintf("MetadataMode(%d)", mode), mode <= metadata.Full, "unknown mode", func() { b.settings.Metadata.Mode = mode })
}

// MetadataKeyspaces limits schema metadata to keyspaces. Default all.
func (b *ConfigBuilder) MetadataKeyspaces(keyspaces ...string) *ConfigBuilder {
	valid := len(keyspaces) > 0 && !slices.Contains(keyspaces, "")
	return b.set("metadata keyspaces", fmt.Sprintf("MetadataKeyspaces(%q)", keyspaces), valid, "needs non-empty names", func() {
		b.settings.Metadata.Keyspaces = append([]string(nil), keyspaces...)
	})
}

// Partitioner sets the token partitioner for the partitioner class name,
// next to the built-in Murmur3 and CDC partitioners.
func (b *ConfigBuilder) Partitioner(name string, partitioner routing.TokenPartitioner) *ConfigBuilder {
	return b.set("partitioner "+name, fmt.Sprintf("Partitioner(%q)", name), name != "" && partitioner != nil, "needs a name and a partitioner", func() {
		partitioners := make(map[string]routing.TokenPartitioner, len(b.settings.Partitioners)+1)
		for k, v := range b.settings.Partitioners {
			partitioners[k] = v
		}
		partitioners[name] = partitioner
		b.settings.Partitioners = partitioners
	})
}

// MetadataRefresh reloads metadata every interval. It replaces
// NoMetadataRefresh. Default 60s.
func (b *ConfigBuilder) MetadataRefresh(interval time.Duration) *ConfigBuilder {
	return b.set("metadata refresh", fmt.Sprintf("MetadataRefresh(%v)", interval), interval > 0, "must be positive", func() { b.settings.Metadata.PeriodicRefresh = interval })
}

// NoMetadataRefresh turns periodic metadata reloads off. It replaces
// MetadataRefresh.
func (b *ConfigBuilder) NoMetadataRefresh() *ConfigBuilder {
	return b.set("metadata refresh", "NoMetadataRefresh()", true, "", func() { b.settings.Metadata.PeriodicRefresh = 0 })
}

// MetadataTimeout bounds one metadata request. Default 31s.
func (b *ConfigBuilder) MetadataTimeout(d time.Duration) *ConfigBuilder {
	return b.set("metadata timeout", fmt.Sprintf("MetadataTimeout(%v)", d), d > 0, "must be positive", func() { b.settings.Metadata.RequestTimeout = d })
}

// MetadataServerTimeout asks the server to bound metadata queries with USING
// TIMEOUT, in whole milliseconds. Default unset.
func (b *ConfigBuilder) MetadataServerTimeout(d time.Duration) *ConfigBuilder {
	return b.set("metadata server timeout", fmt.Sprintf("MetadataServerTimeout(%v)", d), d > 0 && d%time.Millisecond == 0, "must be positive whole milliseconds", func() {
		b.settings.Metadata.ServerTimeout = cql.Some(d)
	})
}

// IgnoreEvents stops listening for the given server events: TopologyEvent,
// StatusEvent, SchemaEvent or ClientRouteEvent. Default none ignored.
func (b *ConfigBuilder) IgnoreEvents(kinds ...EventKind) *ConfigBuilder {
	for _, kind := range kinds {
		call := fmt.Sprintf("IgnoreEvents(%d)", kind)
		var flag *bool
		switch kind {
		case TopologyEvent:
			flag = &b.settings.Metadata.TopologyEvents
		case StatusEvent:
			flag = &b.settings.Metadata.StatusEvents
		case SchemaEvent:
			flag = &b.settings.Metadata.SchemaEvents
		case ClientRouteEvent:
			flag = &b.settings.Metadata.ClientRouteEvents
		}
		b.set(fmt.Sprintf("ignore event %d", kind), call, flag != nil, "not a server event", func() { *flag = false })
	}
	return b
}

// SchemaAgreement waits up to timeout, polling every poll, for nodes to agree
// on the schema. Default 60s and 200ms.
func (b *ConfigBuilder) SchemaAgreement(timeout, poll time.Duration) *ConfigBuilder {
	return b.set("schema agreement", fmt.Sprintf("SchemaAgreement(%v, %v)", timeout, poll), timeout > 0 && poll > 0, "must be positive", func() {
		b.settings.Metadata.AgreementTimeout = timeout
		b.settings.Metadata.AgreementPoll = poll
	})
}

// ManualSchemaAgreement stops waiting for schema agreement after DDL; call
// Session.AwaitSchemaAgreement instead.
func (b *ConfigBuilder) ManualSchemaAgreement() *ConfigBuilder {
	return b.set("automatic schema agreement", "ManualSchemaAgreement()", true, "", func() { b.settings.Metadata.AutomaticAgreement = false })
}

// NoRefreshAfterAgreement skips the metadata reload after schema agreement.
func (b *ConfigBuilder) NoRefreshAfterAgreement() *ConfigBuilder {
	return b.set("refresh after agreement", "NoRefreshAfterAgreement()", true, "", func() { b.settings.Metadata.RefreshAfterAgreement = false })
}

// DefaultProfile chooses execution defaults for statements that pass no
// Profile. choose receives the builder's default profile, so several calls
// add to the same profile.
func (b *ConfigBuilder) DefaultProfile(choose func(*ProfileBuilder)) *ConfigBuilder {
	if choose == nil {
		b.fail("DefaultProfile(nil)", "function is required")
		return b
	}
	choose(b.profile)
	return b
}

// PreparedCacheSize keeps up to n prepared statements. It replaces
// NoPreparedCache. Default 1000.
func (b *ConfigBuilder) PreparedCacheSize(n int) *ConfigBuilder {
	return b.set("prepared cache", fmt.Sprintf("PreparedCacheSize(%d)", n), n > 0, "must be at least 1", func() { b.settings.PreparedCacheSize = n })
}

// NoPreparedCache prepares statements without caching them. It replaces
// PreparedCacheSize.
func (b *ConfigBuilder) NoPreparedCache() *ConfigBuilder {
	return b.set("prepared cache", "NoPreparedCache()", true, "", func() { b.settings.PreparedCacheSize = 0 })
}

// TimestampGenerator supplies client timestamps in microseconds. It replaces
// ServerTimestamps. Default a monotonic clock.
func (b *ConfigBuilder) TimestampGenerator(next func() int64) *ConfigBuilder {
	return b.set("timestamps", "TimestampGenerator(...)", next != nil, "function is required", func() { b.settings.TimestampGenerator = next })
}

// ServerTimestamps sends no client timestamp, so the server assigns one. It
// replaces TimestampGenerator.
func (b *ConfigBuilder) ServerTimestamps() *ConfigBuilder {
	return b.set("timestamps", "ServerTimestamps()", true, "", func() { b.settings.TimestampGenerator = nil })
}

// Application reports the application's name and version in STARTUP.
func (b *ConfigBuilder) Application(name, version string) *ConfigBuilder {
	return b.set("application", fmt.Sprintf("Application(%q, %q)", name, version), name != "", "name is required", func() {
		b.settings.Identity.ApplicationName = name
		b.settings.Identity.ApplicationVersion = version
	})
}

// ClientID reports id in STARTUP.
func (b *ConfigBuilder) ClientID(id string) *ConfigBuilder {
	return b.set("client id", fmt.Sprintf("ClientID(%q)", id), id != "", "must not be empty", func() { b.settings.Identity.ClientID = id })
}

// Logger receives the driver's log lines. Default none.
func (b *ConfigBuilder) Logger(logger StdLogger) *ConfigBuilder {
	return b.set("logger", "Logger(...)", logger != nil, "logger is required", func() { b.settings.Logger = logger })
}

// Observer receives every request and attempt. Default none.
func (b *ConfigBuilder) Observer(observer Observer) *ConfigBuilder {
	return b.set("observer", "Observer(...)", observer != nil, "observer is required", func() { b.settings.Observer = observer })
}

// LifecycleObserver receives session and connection lifecycle events.
func (b *ConfigBuilder) LifecycleObserver(observer LifecycleObserver) *ConfigBuilder {
	return b.set("lifecycle observer", "LifecycleObserver(...)", observer != nil, "observer is required", func() { b.settings.LifecycleObserver = observer })
}

// NoConfigReport stops sending the driver configuration to the server.
func (b *ConfigBuilder) NoConfigReport() *ConfigBuilder {
	return b.set("config report", "NoConfigReport()", true, "", func() { b.settings.ReportConfiguration = false })
}

// String lists the effective settings. Credentials and TLS material are not
// printed.
func (c Config) String() string {
	if c.settings == nil {
		return "gocql.Config{}"
	}
	s := c.settings
	var w strings.Builder
	fmt.Fprintf(&w, "contact points=%v port=%d keyspace=%q protocol=%d compression=%v", s.ContactPoints, s.Port, s.Keyspace, s.Protocol, s.Compression)
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
	choices
	profile implSettings.Profile
}

// NewProfile starts a profile.
func NewProfile() *ProfileBuilder {
	return &ProfileBuilder{profile: *implSettings.DefaultConfig().DefaultProfile}
}

// Build validates the profile, for use as Options.Profile.
func (p *ProfileBuilder) Build() (Profile, error) { return p.build("Profile") }

func (p *ProfileBuilder) build(name string) (Profile, error) {
	errs := append([]error(nil), p.errs...)
	if len(errs) == 0 {
		if err := implSettings.InternalValidateProfile(p.profile); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return Profile{}, fmt.Errorf("gocql: %s: %w", name, errors.Join(errs...))
	}
	return p.profile, nil
}

func (p *ProfileBuilder) set(key, call string, valid bool, reason string, apply func()) *ProfileBuilder {
	if !valid {
		p.fail(call, reason)
		return p
	}
	p.choose(key)
	apply()
	return p
}

// Consistency sets the consistency level. Default LOCAL_QUORUM.
func (p *ProfileBuilder) Consistency(c cql.Consistency) *ProfileBuilder {
	return p.set("consistency", fmt.Sprintf("Consistency(%v)", c), implSettings.InternalValidConsistency(c), "invalid consistency", func() { p.profile.Consistency = c })
}

// SerialConsistency sets the serial consistency of conditional statements:
// SERIAL or LOCAL_SERIAL. It replaces NoSerialConsistency. Default
// LOCAL_SERIAL.
func (p *ProfileBuilder) SerialConsistency(c cql.Consistency) *ProfileBuilder {
	return p.set("serial consistency", fmt.Sprintf("SerialConsistency(%v)", c), c == cql.Serial || c == cql.LocalSerial, "must be SERIAL or LOCAL_SERIAL", func() {
		p.profile.SerialConsistency = cql.Some(c)
	})
}

// NoSerialConsistency sends no serial consistency. It replaces
// SerialConsistency.
func (p *ProfileBuilder) NoSerialConsistency() *ProfileBuilder {
	return p.set("serial consistency", "NoSerialConsistency()", true, "", func() { p.profile.SerialConsistency = cql.Option[cql.Consistency]{} })
}

// Timeout bounds one execution, including retries. It replaces NoTimeout.
// Default 30s.
func (p *ProfileBuilder) Timeout(d time.Duration) *ProfileBuilder {
	return p.set("timeout", fmt.Sprintf("Timeout(%v)", d), d > 0, "must be positive", func() { p.profile.Timeout = d })
}

// NoTimeout leaves executions bounded only by their context. It replaces
// Timeout.
func (p *ProfileBuilder) NoTimeout() *ProfileBuilder {
	return p.set("timeout", "NoTimeout()", true, "", func() { p.profile.Timeout = 0 })
}

// PageTimeout bounds fetching one page. Default none.
func (p *ProfileBuilder) PageTimeout(d time.Duration) *ProfileBuilder {
	return p.set("page timeout", fmt.Sprintf("PageTimeout(%v)", d), d > 0, "must be positive", func() { p.profile.PageTimeout = d })
}

// PageSize sets the rows per page. Default 5000.
func (p *ProfileBuilder) PageSize(rows int32) *ProfileBuilder {
	return p.set("page size", fmt.Sprintf("PageSize(%d)", rows), rows > 0, "must be at least 1", func() { p.profile.PageSize = rows })
}

// NoPrefetch fetches the next page only when the current one is consumed.
func (p *ProfileBuilder) NoPrefetch() *ProfileBuilder {
	return p.set("prefetch", "NoPrefetch()", true, "", func() { p.profile.Prefetch = 0 })
}

// Retry sets the retry policy. Default retry.Safe{MaxAttempts: 3}.
func (p *ProfileBuilder) Retry(policy retry.Policy) *ProfileBuilder {
	return p.set("retry", "Retry(...)", policy != nil, "policy is required", func() { p.profile.Retry = policy })
}

// Routing sets the routing policy. Default token-aware with shuffled replicas.
func (p *ProfileBuilder) Routing(policy routing.Policy) *ProfileBuilder {
	return p.set("routing", "Routing(...)", policy != nil, "policy is required", func() { p.profile.Routing = policy })
}

// Speculation sets the speculative execution policy. Default none.
func (p *ProfileBuilder) Speculation(policy SpeculationPolicy) *ProfileBuilder {
	return p.set("speculation", "Speculation(...)", policy != nil, "policy is required", func() { p.profile.Speculation = policy })
}

// ClientRoutesBuilder assembles ScyllaDB client routes settings.
type ClientRoutesBuilder struct {
	routes implSettings.ClientRoutesOptions
}

// NewClientRoutes routes through the given client connection IDs.
func NewClientRoutes(connectionIDs ...string) *ClientRoutesBuilder {
	return &ClientRoutesBuilder{routes: implSettings.ClientRoutesOptions{ConnectionIDs: append([]string(nil), connectionIDs...)}}
}

// HostnameOverride dials hostname for connectionID.
func (r *ClientRoutesBuilder) HostnameOverride(connectionID, hostname string) *ClientRoutesBuilder {
	overrides := make(map[string]string, len(r.routes.HostnameOverrides)+1)
	for k, v := range r.routes.HostnameOverrides {
		overrides[k] = v
	}
	overrides[connectionID] = hostname
	r.routes.HostnameOverrides = overrides
	return r
}

// TLS requires verified TLS on routed connections.
func (r *ClientRoutesBuilder) TLS() *ClientRoutesBuilder {
	r.routes.TLS = true
	return r
}

func (r *ClientRoutesBuilder) build() (implSettings.ClientRoutesOptions, error) {
	routes := r.routes
	routes.ConnectionIDs = append([]string(nil), routes.ConnectionIDs...)
	return routes, nil
}
