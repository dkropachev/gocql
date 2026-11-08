# Client routes

Pass `gocql.NewClientRoutes(connectionIDs...)` to `ConfigBuilder.ClientRoutes`.
It supplies connection identities and, through `HostnameOverride`, hostname
overrides for route discovery. Client routes turn advanced shard awareness
off, because the proxy in front of them rewrites the source port that picks the
shard; an explicit `AdvancedShardAwareness` call makes `Build` fail. Shard-aware
routing still works: the pool keeps dialing the regular port until every shard
has a connection.
The driver uses typed internal metadata queries and SNI-aware per-host hooks;
route refresh and connection lifecycle obey context cancellation.
