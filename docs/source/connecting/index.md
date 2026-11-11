# Connecting

A `Config` is built, never written by hand. `NewConfig(contactPoints...)`
returns a `ConfigBuilder`, and each method sets one setting; settings you do
not choose keep the driver's behavior, and when a setting is chosen again the
last call wins. Setters only store values. `Build` validates the final configuration and
returns an immutable `Config` or the first validation error. A later setting
replaces an earlier one even if it was invalid:

```go
profile, err := gocql.NewProfile().
    WithConsistency(cql.LocalQuorum).WithTimeout(5 * time.Second).Build()
if err != nil { return err }
config, err := gocql.NewConfig("10.0.0.1", "10.0.0.2").
    WithKeyspace("app").
    WithPoolSize(gocql.PerShard(2)).
    WithDefaultProfile(profile).
    Build()
```

Contact points may include native ports; `WithPort` supplies the port when one is
absent. A single absolute path, optionally prefixed with `unix:`, connects to a
Unix socket: no host discovery is then filled in unless chosen.

## Dialects

`Connect` takes the dialect as a type parameter: the database and the native
protocol version you connect to.

```go
session, err := gocql.Connect[gocql.ScyllaV4](ctx, config)
```

| Dialect | Database | Protocol |
|---|---|---|
| `gocql.ScyllaV4` | ScyllaDB | 4, with ScyllaDB's extensions |
| `gocql.CassandraV4` | Apache Cassandra | 4 |
| `gocql.CassandraV5` | Apache Cassandra 4.0 or later | 5 |

The driver negotiates no version: every connection speaks the dialect's, and
`Connect` fails when the cluster does not. It also fails with
`cqlerr.ServerMismatchError` when the cluster is not the dialect's database.
The dialect fixes how the driver reads results that differ between Cassandra
and ScyllaDB ([differences](../server-differences.md)), at no cost per request.
Protocol 5 has no frame compression, so `CassandraV5` with Snappy fails at
`Connect`; use LZ4 or none.

The dialect is part of the session's type, `*gocql.Session[gocql.ScyllaV4]`.
Prepared statements, queries and batches do not carry it. Code that serves
several dialects is generic over it:

```go
func load[D gocql.Dialect](ctx context.Context, s *gocql.Session[D]) error { ... }
```

Use `NoHeartbeat`, `NoAdvancedShardAwareness`, `NoMetadataRefresh` and
`NoPreparedCache` to disable features; their corresponding `With*` methods
restore them. `WithDiscovery`, `WithConfigReport`, `WithAutomaticSchemaAgreement`
and `WithRefreshAfterAgreement` restore those defaults after their off switches.
`ProfileBuilder.WithPrefetch(0)` disables prefetch and `WithPrefetch(1)` restores it.
Zero optional timeouts disable their budget; nil optional providers clear the
override, including `WithTLS(nil)` for plaintext. `WithDefaultProfile(profile)`
copies and replaces the complete profile. Build profiles with `NewProfile` to
start from driver defaults; a `gocql.Profile{...}` literal would leave every
unset field zero, so `Build` rejects it. A built profile may be copied and its
fields edited. Later edits to a supplied profile do not affect the
configuration. Use `ConfigureDefaultProfile(callback)` to compose changes to
the current profile, including one set by `WithDefaultProfile`; `ConfigureDefaultProfile(nil)` resets
all profile settings. `WithIgnoreEvents` replaces the complete ignored-event
list; calling it without arguments restores all events.

`WithContactPoints` replaces the contact points. Slice arguments are copied,
and `Build` snapshots the finished configuration. `StaticTLS` copies its input
when constructed. Callbacks and provider objects remain shared and must support
concurrent use.

`WithPoolSize` sizes each node's pool: `gocql.PerShard(n)` keeps n connections
to every shard (the default is `PerShard(1)`), and `gocql.PerHost(n)` keeps n
per node regardless of its shard count. `PerHost` turns shard-aware connection
management off: connections use the regular port only, the node picks their
shards, and requests for a shard without a connection take another shard's
connection. `Build` rejects `PerHost` with `WithAdvancedShardAwareness` or
`WithMaxExcessPerShard`. An explicit incremental stream occupies one configured connection through
idle time and pages. Default page-buffered streams use normal multiplexing. Cancellation interrupts work;
Session.Close rejects new work, cancels requests and joins writers/workers.
Queries copy parameters during admission; the next execution waits within its
context for the preceding execution to drain.

`endpoint.Dialer` receives a typed `endpoint.DialTarget` and customizes host
setup. Hooks honor context cancellation and the selected endpoint/source.
`endpoint.Resolver`, `endpoint.Translator`, `HostFilter` and `ReconnectPolicy` are
independent extension points. Synchronous callbacks must be prompt and must
not reenter blocking Close/Wait.

`WithResolver` takes an `endpoint.Resolver`, which resolves contact point and
client route host names. `*net.Resolver` satisfies it; the default is
`net.DefaultResolver`. Each lookup runs on the calling goroutine with a context
bounded by `WithDNSTimeout`, so a resolver must return once that context is
done. To prefer or require IPv4, wrap a `*net.Resolver` and filter its result
or pass `"ip4"` as the network.

`WithAddressTranslator` takes an `endpoint.Translator`. Before each connection
attempt it is called with the setup context, the node's `endpoint.Peer` (host ID,
datacenter, rack, advertised addresses and ports) and each endpoint to dial as a
`netip.AddrPort`: the CQL port and, on ScyllaDB, the shard-aware ports. It
returns the address to dial, or the input unchanged. A translation error fails
that connection attempt like a dial error.
`endpoint.TranslatorFunc` adapts a function.

```{toctree}
:maxdepth: 1

authentication
compression
tls
```

`ConvictionPolicy.AddFailure(error, HostSnapshot)` returns whether a failed host
should be marked down; `Reset(HostSnapshot)` clears application policy state.
The host snapshot is detached read-only metadata that can be retained. Custom
policies must support concurrent callbacks. The default SimpleConvictionPolicy
always convicts and its Reset is a no-op.
