# Migrating from v1

v2 is a new module, `github.com/scylladb/gocql/v2`, with a typed API. It is
not source compatible with v1 (`github.com/scylladb/gocql`): every call site
that builds a cluster, runs a query or reads rows changes. This page lists what
changes and maps the most used v1 calls to their v2 form.

The two modules have different import paths, so one binary can import both and
move one package at a time. Give one of them an import alias, since both
packages are named `gocql`.

## What changes

- **Statements are typed.** A `cql.Statement[P, R]` names its parameter type
  `P` and row type `R`, and carries the encoder and decoder for them. The
  driver does not reflect over `interface{}` values on each call as v1's
  `Query(stmt, values...)` and `Scan(&a, &b)` did. Struct mappings
  (`cql.ParamsOf`, `cql.RowOf`) are built once and checked against the
  server's metadata when the statement is prepared.
- **Preparation is explicit.** v1 prepared every query with bind values behind
  the scenes. In v2, `session.Prepare` returns a `*gocql.Prepared` that you
  keep and share between goroutines. `session.UnpreparedExec` and its siblings
  run occasional CQL text without preparing it.
- **The context comes first.** Every call that talks to the cluster takes a
  `context.Context` as its first argument; there is no `WithContext`.
- **The database and protocol version are chosen, not negotiated.**
  `gocql.Connect(ctx, cfg, dialect)` takes `gocql.ScyllaV4`,
  `gocql.CassandraV4` or `gocql.CassandraV5`, and fails with
  `cqlerr.ServerMismatchError` when the cluster does not match.
- **Configuration is built and validated.** `config.New(...)` returns a
  builder; `Build` validates every setting and returns an immutable
  `config.Config`. Per-request defaults (consistency, timeouts, paging, retry,
  routing, speculation) live in a `config.Profile`, and per-call overrides in
  `exec.Options`.
- **Errors live in `cqlerr`.** Server errors are typed
  (`*cqlerr.WriteTimeoutError`, `*cqlerr.UnavailableError`, ...), and
  `cqlerr.ExecutionStatusOf` reports whether a failed request may have
  changed data.
- **Rows are borrowed.** Text and blob views returned by `cql.TextCodec` and
  `cql.BlobCodec` point into the response page and expire when the iteration
  moves on; `cql.StringCodec` and `cql.ByteSliceCodec` decode owned copies.
  See [streaming](statements/streaming.md) for the lifetime rules.
- **Defaults differ.** The default profile uses `LOCAL_QUORUM` (v1 used
  `QUORUM`), `LOCAL_SERIAL`, a 30 second request timeout, 5000-row pages, one
  page prefetched, token-aware routing and `retry.Safe`. On ScyllaDB the pool
  keeps one connection per shard unless configured otherwise.
- **`USE` is rejected.** A `USE` statement fails with `cqlerr.ErrUseStmt`; set
  the keyspace with `WithKeyspace` or qualify table names.

## Connecting

v1:

```go
cluster := gocql.NewCluster("10.0.0.1", "10.0.0.2")
cluster.Keyspace = "app"
cluster.Consistency = gocql.LocalQuorum
cluster.Authenticator = gocql.PasswordAuthenticator{Username: "u", Password: "p"}
cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy("dc1"))
session, err := cluster.CreateSession()
if err != nil {
	return err
}
defer session.Close()
```

v2:

```go
policy, err := routing.NewPolicy(routing.Options{Datacenter: "dc1", TokenAware: true})
if err != nil {
	return err
}
cfg, err := config.New("10.0.0.1", "10.0.0.2").
	WithKeyspace("app").
	WithAuth(auth.Password("u", "p")).
	ConfigureDefaultProfile(func(p *config.ProfileBuilder) {
		p.WithConsistency(cql.LocalQuorum).WithRouting(policy)
	}).
	Build()
if err != nil {
	return err
}
session, err := gocql.Connect(ctx, cfg, gocql.ScyllaV4)
if err != nil {
	return err
}
defer session.Close()
```

| v1 | v2 |
| --- | --- |
| `gocql.NewCluster(hosts...)` | `config.New(hosts...)`; a host without a port uses `WithPort` (9042 by default) |
| `cluster.CreateSession()` | `cfg, err := builder.Build()`, then `gocql.Connect(ctx, cfg, dialect)` |
| `cluster.ProtoVersion` | the dialect passed to `Connect` |
| `cluster.Keyspace` | `WithKeyspace(name)` |
| `cluster.Consistency`, `SerialConsistency`, `PageSize`, `Timeout` | `ConfigureDefaultProfile` with `WithConsistency`, `WithSerialConsistency`, `WithPageSize`, `WithTimeout` |
| `cluster.ConnectTimeout` | `WithConnectTimeout(d)` |
| `cluster.NumConns` | `WithConnectionsPerShard(n)` keeps shard awareness on ScyllaDB; `WithConnectionsPerHost(n)` keeps n per node |
| `cluster.Port` | `WithPort(port)` |
| `cluster.Authenticator = gocql.PasswordAuthenticator{...}` | `WithAuth(auth.Password(user, password))`; other mechanisms implement `auth.Provider` |
| `cluster.SslOpts` | `WithTLS(*tls.Config)`, or `WithTLSSource` with a source from `config.NewTLSFiles` to reload certificate files |
| `cluster.Compressor` | `WithCompression(cql.LZ4Compression)` or `cql.SnappyCompression` |
| `cluster.PoolConfig.HostSelectionPolicy` | `routing.NewPolicy(routing.Options{...})` in the profile's `WithRouting`; see [load balancing](load-balancing.md) |
| `cluster.RetryPolicy` | the profile's `WithRetry(policy)`, for example `retry.Safe{MaxAttempts: n}` with n attempts in all; see [retries](retry-policy.md) |
| `cluster.ReconnectionPolicy` | `WithReconnect(endpoint.ReconnectPolicy)` |
| `cluster.HostFilter` | `WithHostFilter(func(metadata.HostSnapshot) bool)` |
| `cluster.AddressTranslator` | `WithAddressTranslator(endpoint.Translator)` |
| `cluster.Dialer` | `WithDialer(endpoint.Dialer)` |
| `cluster.Logger` | `WithLogger(*slog.Logger)` |
| `cluster.QueryObserver`, `BatchObserver` | `WithObserver(observe.Observer)` |
| `cluster.DisableInitialHostLookup` (contact points only) | `WithDiscovery(false)` |
| `cluster.DefaultTimestamp = false` | `WithServerTimestamps()` |

Settings v1 spread over `ClusterConfig` fields are all builder methods in
v2; `go doc github.com/scylladb/gocql/v2/config.ConfigBuilder` lists them with
their defaults. See [connecting](connecting/index.md).

## Queries

v1:

```go
var name string
err := session.Query("SELECT name FROM users WHERE id = ?", id).
	WithContext(ctx).Consistency(gocql.One).Scan(&name)
if errors.Is(err, gocql.ErrNotFound) {
	// no row
}
```

v2:

```go
selectName, err := session.Prepare(ctx, cql.Statement[int32, string]{
	CQL:    "SELECT name FROM users WHERE id = ?",
	Params: cql.OneParam(cql.Int32Codec),
	Rows:   cql.OneColumn(cql.StringCodec),
})
if err != nil {
	return err
}
// Prepare once and keep selectName; compile options once too.
one, err := session.CompileOptions(exec.Options{Consistency: cql.Some(cql.One)})
if err != nil {
	return err
}
var name string
found, _, err := selectName.One(ctx, id, &name, one)
if err != nil {
	return err
}
if !found {
	// no row
}
```

| v1 | v2 |
| --- | --- |
| `session.Query(stmt, values...)` | `session.Prepare(ctx, cql.Statement[P, R]{...})` once, then calls on the `*Prepared` |
| `query.Exec()` | `prepared.Exec(ctx, params)`; `session.UnpreparedExec(ctx, statement, params)` for one-off text such as DDL (`cql.Command(text)`) |
| `query.Scan(&a, &b)` | `prepared.One(ctx, params, &row)`; `found` is false instead of `gocql.ErrNotFound` |
| `iter := query.Iter(); for iter.Scan(&a) {...}; iter.Close()` | `prepared.Each(ctx, params, &row, fn)`, `for row, err := range prepared.All(ctx, params, &row)`, or `rows, err := prepared.Run(ctx, params)` with `rows.NextInto(&row)`, `rows.Err()` and `rows.Close()` |
| `iter.MapScan(m)`, `iter.SliceMap()` | rows decoded with `cql.DynamicRows`; `row.Get(name)` returns a `cql.Value` |
| `query.WithContext(ctx)` | `ctx` is the first argument |
| `query.Consistency(c)` | `exec.Options{Consistency: cql.Some(c)}` |
| `query.SerialConsistency(gocql.LocalSerial)` | `exec.Options{SerialConsistency: exec.SerialLocal}` |
| `query.PageSize(n)` | `exec.Options{PageSize: cql.Some[int32](n)}`, or the page size argument of `RunPage` |
| `query.Idempotent(true)` | `exec.Options{Idempotent: true}` |
| `query.RetryPolicy(p)` | `exec.Options{Retry: p}` |
| `query.WithTimestamp(ts)` | `exec.Options{Timestamp: cql.Some(ts)}` |
| `query.Trace(tracer)` | `exec.Options{CQLTrace: true}`, then `cqltrace.Fetch`; see [tracing](tracing.md) |
| a keyspace other than the session's | a qualified table name; on `CassandraV5`, also `exec.Options{Keyspace: name}` or `exec.PrepareOptions{Keyspace: name}` |
| `query.Release()` | nothing to release; a reusable `prepared.Query(options)` is closed with `Close` |

One-shot calls on a `*Prepared` take `exec.CompiledOptions` from
`session.CompileOptions`; compile them once and reuse them. A worker that runs
a statement in a loop can own a `*gocql.Query` from `prepared.Query(options)`,
which takes `exec.Options` directly. See [statements](statements/index.md).

### Struct mapping

v1 had no struct mapping in the driver; gocqlx added it. v2 maps struct fields
by their `cql` tags or names:

```go
type User struct {
	ID   cql.UUID `cql:"id"`
	Name string   `cql:"name"`
}

insertUser, err := session.Prepare(ctx, cql.Statement[User, cql.EmptyRow]{
	CQL:    "INSERT INTO users (id, name) VALUES (?, ?)",
	Params: cql.MustParamsOf[User](),
})
```

`cql.Params(cql.Param(...))` and `cql.Row(cql.Column(...))` map fields
explicitly. See [data types](data-types.md).

## Paging

v1 resumed with `query.PageState(state)` and read `iter.PageState()`. In v2,
`RunPage` takes the state and the page size, and returns one `*gocql.Page`:

```go
page, err := listUsers.RunPage(ctx, cql.EmptyParams{}, state, 100)
if err != nil {
	return err
}
for page.NextInto(&user) {
	// use user
}
next := page.State() // next.Token() is the opaque cursor to hand out
if err := page.Close(); err != nil {
	return err
}
```

`cql.PageStateFromToken(token)` restores a state from a saved token. The token
is server state: authenticate or encrypt it before giving it to an untrusted
client. See [paging](statements/paging.md).

## Batches and lightweight transactions

| v1 | v2 |
| --- | --- |
| `session.NewBatch(gocql.UnloggedBatch)` | `batch := gocql.Batch{Kind: cql.UnloggedBatch}` |
| `batch.Query(stmt, values...)` | `prepared.AddToBatch(&batch, params)`, or `session.UnpreparedAddToBatch` |
| `session.ExecuteBatch(batch)` | `session.ExecBatch(ctx, &batch)` |
| `session.ExecuteBatchCAS(batch, dest...)`, `MapExecuteBatchCAS` | `session.RunConditionalBatch(ctx, &batch, cql.CASRows(decoder))` |
| `query.ScanCAS(dest...)`, `MapScanCAS` | a statement whose `Rows` is `cql.CASRows(decoder)`, read with `One`; `CASResult.Applied` reports the outcome |

`Exec` and `ExecBatch` reject a statement or batch known to be conditional,
since only the rows say whether it applied. Cassandra does not mark
conditional statements in prepared metadata: set
`Kind: cql.ConditionalStatement` there. See [batches](statements/batches.md) and
[lightweight transactions](statements/lightweight-transactions.md).

## Types

| v1 | v2 |
| --- | --- |
| `gocql.UUID`, `gocql.ParseUUID` | `cql.UUID`, `cql.ParseUUID` |
| `gocql.TimeUUID()`, `gocql.UUIDFromTime` | `cql.NewTimeUUID()`, `cql.TimeUUIDFromTime` (type `cql.TimeUUID`) |
| `gocql.Consistency` constants (`gocql.Quorum`, ...) | `cql.Consistency` constants (`cql.Quorum`, ...); `cql.ParseConsistency` |
| `gocql.Marshaler`, `gocql.Unmarshaler` | `cql.Encoder[T]`, `cql.Decoder[T]`, `cql.Codec[T]`; see [custom codecs](custom-codecs.md) |
| `gocql.UnsetValue` | the `Unset` binding state; see [data types](data-types.md) |
| `*T` or `gocql.NullX` for NULL | `cql.NullableCodec` or `cql.Nullable[T]` |

## Errors

| v1 | v2 |
| --- | --- |
| `gocql.ErrNotFound` from `Scan` | `One` returns `found == false` |
| `*gocql.RequestErrWriteTimeout`, `RequestErrReadTimeout`, `RequestErrUnavailable` | `*cqlerr.WriteTimeoutError`, `*cqlerr.ReadTimeoutError`, `*cqlerr.UnavailableError` |
| `gocql.RequestError` and its `Code()` | `cqlerr.ServerError`, or `errors.Is(err, cqlerr.CodeOverloaded)` |
| `gocql.ErrTimeoutNoResponse`, `ErrNoConnections`, `ErrSessionClosed` | the same names in `cqlerr` |

A failed execution returns a `*cqlerr.ExecutionError` that lists every attempt.
`cqlerr.KindOf` groups errors by cause, and `cqlerr.ExecutionStatusOf` reports
whether a retry could apply a write twice. See [errors](errors.md).

## Policies, metadata and observation

| v1 | v2 |
| --- | --- |
| `gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(dc))` | `routing.NewPolicy(routing.Options{Datacenter: dc, TokenAware: true})` |
| `gocql.RackAwareRoundRobinPolicy(dc, rack)` | `routing.Options{Datacenter: dc, Rack: rack, TokenAware: true}` |
| `gocql.HostPoolHostPolicy` and other custom policies | implement `routing.Policy`; see [load balancing](load-balancing.md) |
| `&gocql.SimpleRetryPolicy{NumRetries: n}` | `retry.Safe{MaxAttempts: n + 1}`; it retries a non-idempotent request only after a failure that reports it was not executed, or a read timeout that returned no data |
| `&gocql.SimpleSpeculativeExecution{...}` | implement `retry.SpeculationPolicy`; see [speculation](speculative-execution.md) |
| `session.KeyspaceMetadata(name)` | `session.Metadata().Keyspace(name)` on an immutable `*metadata.Snapshot` |
| `session.AwaitSchemaAgreement(ctx)` | `session.AwaitSchemaAgreement(ctx)` |
| `gocql.ObservedQuery`, `QueryObserver` | `observe.RequestObserver`, `observe.AttemptObserver` |
| `gocql.ConnectObserver`, `HostStateNotifier` | `WithLifecycleObserver(observe.LifecycleObserver)`, or `session.Subscribe(event.Filter{...})` for cluster events |

## Not carried over

- `session.Query(...).Bind` and other mutable query objects: a statement is
  immutable and its parameters are a typed value.
- Reflection over arbitrary Go values on each call: build the encoder and
  decoder once.
- Protocol version negotiation and protocols 3 and older: pick a dialect.
- `USE` statements: set the session keyspace or qualify table names.
