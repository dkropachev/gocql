# Client routes

`config.NewClientRoutes` builds a read-only `config.ClientRoutes`. Each
`WithRoute(connectionID, hostname)` adds a client connection ID; a non-empty
hostname is dialed instead of the address the cluster publishes for it.
`config.ConfigBuilder.WithClientRoutes` replaces the whole setting, and the
zero `config.ClientRoutes{}` removes it. With TLS configured (`WithTLS` or
`WithTLSSource`) routed connections dial each route's `tls_port`, otherwise its
`port`.

```go
routes, err := config.NewClientRoutes().
	WithRoute("conn-a", "").
	WithRoute("conn-b", "b.proxy.example.com").
	Build()
if err != nil {
	return err
}
cfg, err := config.New("10.0.0.1").
	WithTLS(tlsConfig).
	WithClientRoutes(routes).
	Build()
```

Client routes turn advanced shard awareness off by default, because a proxy in
front of them commonly rewrites the source port that picks the shard. When
every route endpoint forwards to ScyllaDB's Proxy Protocol v2 shard-aware
listener and passes the client's source port, turn it on with
`ConfigBuilder.WithDefaultAdvancedShardAwareness()` or
`WithAdvancedShardAwareness(first, last)`. Off, shard-aware routing still
works: the pool keeps dialing the regular port until every shard has a
connection.
The driver uses typed internal metadata queries; route refresh and connection lifecycle obey context cancellation.
