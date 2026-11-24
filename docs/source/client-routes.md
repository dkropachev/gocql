# Client routes

`config.ConfigBuilder.WithClientRoutes` takes one `config.ClientRoute` per
client connection ID; a non-empty `Hostname` is dialed instead of the address
the cluster publishes for it. Each call replaces the whole setting, and
`WithClientRoutes()` with no routes removes it. With TLS configured (`WithTLS`
or `WithTLSSource`) routed connections dial each route's `tls_port`, otherwise
its `port`. `Build` rejects an empty or repeated connection ID and client
routes together with `WithAddressTranslator`, and `Build` with a Cassandra
dialect fails with `*cqlerr.UnsupportedSettingError`.

```go
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithTLS(tlsConfig).
	WithClientRoutes(
		config.ClientRoute{ConnectionID: "conn-a"},
		config.ClientRoute{ConnectionID: "conn-b", Hostname: "b.proxy.example.com"},
	).
	Build()
```

Client routes turn advanced shard awareness off by default, because a proxy in
front of them commonly rewrites the source port that picks the shard. When
every route endpoint forwards to ScyllaDB's Proxy Protocol v2 shard-aware
listener and passes the client's source port, turn it on with
`ConfigBuilder.WithAdvancedShardAwareness(true)`; `WithShardAwareSourcePorts`
picks the source ports. Off, shard-aware routing still
works: the pool keeps dialing the regular port until every shard has a
connection.
The driver uses typed internal metadata queries; route refresh and connection lifecycle obey context cancellation.
