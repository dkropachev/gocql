# Client routes

`config.ConfigBuilder.WithClientRoutes(connectionIDs...)` supplies connection
identities, `WithClientRouteHostname` hostname overrides for route discovery,
and `WithClientRouteTLS(true)` requires verified TLS on routed connections. Client routes turn advanced shard awareness
off, because the proxy in front of them rewrites the source port that picks the
shard; an explicit `WithAdvancedShardAwareness` call makes `Build` fail. Shard-aware
routing still works: the pool keeps dialing the regular port until every shard
has a connection.
The driver uses typed internal metadata queries; route refresh and connection lifecycle obey context cancellation.
