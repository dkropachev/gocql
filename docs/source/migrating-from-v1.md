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
  `config.New` takes `config.ScyllaV4`, `config.CassandraV4` or
  `config.CassandraV5` first, and `gocql.Connect(ctx, cfg)` fails with
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
  `cql.BytesCodec` point into the response page and expire when the iteration
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
cfg, err := config.New(config.ScyllaV4, "10.0.0.1", "10.0.0.2").
	WithKeyspace("app").
	WithAuth(auth.Password("u", "p")).
	WithLocalDatacenter("dc1", "").
	ConfigureDefaultProfile(func(p *config.ProfileBuilder) {
		p.WithConsistency(cql.LocalQuorum)
	}).
	Build()
if err != nil {
	return err
}
session, err := gocql.Connect(ctx, cfg)
if err != nil {
	return err
}
defer session.Close()
```

| v1 | v2 |
| --- | --- |
| `gocql.NewCluster(hosts...)` | `config.New(config.ScyllaV4, hosts...)`; a host without a port uses `WithPort` (9042 by default). v1 learned the port from hosts that all named one; v2 does not, and `Build` rejects such hosts when their port differs from `WithPort`, which is also the port of discovered nodes: call `WithPort(port)` |
| `cluster.CreateSession()` | `cfg, err := builder.Build()`, then `gocql.Connect(ctx, cfg)` |
| `cluster.ProtoVersion` | the dialect passed to `config.New` |
| `cluster.Keyspace` | `WithKeyspace(name)` |
| `cluster.Consistency`, `SerialConsistency`, `PageSize`, `Timeout` | `ConfigureDefaultProfile` with `WithConsistency`, `WithSerialConsistency`, `WithPageSize`, `WithTimeout` |
| `cluster.ConnectTimeout` | `WithConnectTimeout(d)` |
| `cluster.NumConns` | `WithConnectionsPerShard(n)` keeps shard awareness on ScyllaDB; `WithConnectionsPerHost(n)` keeps n per node |
| `cluster.Port` | `WithPort(port)` |
| `cluster.Authenticator = gocql.PasswordAuthenticator{...}` | `WithAuth(auth.Password(user, password))`; other mechanisms implement `auth.Provider` |
| `cluster.SslOpts` | `WithTLS(*tls.Config)`, or `WithTLSSource` with a source from `config.NewTLSFileSource` to reload certificate files |
| `cluster.Compressor` | `WithCompression(cql.LZ4Compression)` or `cql.SnappyCompression` |
| `cluster.PoolConfig.HostSelectionPolicy` | the default policy is token-aware; `WithLocalDatacenter(dc, rack)` sets its locality, and `routing.NewPolicy(routing.Options{...})` in the profile's `WithRouting` changes the rest; see [load balancing](load-balancing.md) |
| `cluster.RetryPolicy` | the profile's `WithRetry(policy)`, for example `retry.Safe{MaxAttempts: n}` with n attempts in all; `WithRetry(nil)` is the default, and `retry.Never{}` disables retries; see [retries](retry-policy.md) |
| `cluster.ReconnectionPolicy` | `WithReconnect(endpoint.ReconnectPolicy)`; its `Delay(attempt)` counts from 1, where v1's `GetInterval(currentRetry)` counted from 0. `WithReconnect(nil)` is the default backoff; a policy whose `Delay` returns false never reconnects |
| `cluster.ConvictionPolicy` | `WithConviction(endpoint.ConvictionPolicy)`; the default is `endpoint.SimpleConvictionPolicy{}` |
| `cluster.HostFilter` | `WithHostFilter(func(metadata.HostSnapshot) bool)` |
| `cluster.AddressTranslator` | `WithAddressTranslator(endpoint.Translator)` |
| `cluster.Dialer` | `WithDialer(endpoint.Dialer)`; wrap `endpoint.DefaultDialer{}` to keep the default dial |
| `cluster.SocketKeepalive` | `WithTCPKeepalive(period)` |
| `cluster.DisableShardAwarePort` | `WithAdvancedShardAwareness(false)`; `WithShardAwareSourcePorts(first, last)` sets the source ports |
| `cluster.ClientRoutesConfig` | `WithClientRoutes(config.ClientRoute{ConnectionID: id, Hostname: host}, ...)` |
| `cluster.Logger` | `WithLogger(*slog.Logger)` |
| `cluster.QueryObserver`, `BatchObserver` | `WithObserver(observe.Observer)` |
| `cluster.DisableInitialHostLookup` (contact points only) | `WithDiscovery(false)` |
| `cluster.DefaultTimestamp = false` | `WithTimestampGenerator(nil)` |

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
selectName, err := session.Prepare(ctx, cql.Select(
	"SELECT name FROM users WHERE id = ?",
	cql.OneParam(cql.Int32Codec),
	cql.OneColumn(cql.StringCodec),
))
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
| `query.PageSize(n)` | `exec.Options{PageSize: n}`, or the page size argument of `RunPage` |
| `query.Idempotent(true)` | `Idempotent: true` on the `cql.Statement`, or `exec.Options{Idempotent: cql.Some(true)}` for one execution |
| a time limit for one query other than the default | `exec.Options{Timeout: cql.Some(d)}`, which replaces the profile's limit (30s by default) for that execution; `cql.Some(time.Duration(0))` removes it |
| `query.RetryPolicy(p)` | `exec.Options{Retry: p}` |
| `query.WithTimestamp(ts)` | `exec.Options{Timestamp: cql.Some(time.UnixMicro(ts))}` through `Session.CompileOptions`; a `Worker` (`Prepared.Worker`) rejects a fixed timestamp |
| `query.Attempts()`, `query.Latency()` | the returned `cql.Result`'s `Attempts` and `AttemptLatency`, the sum of the completed attempts' latencies |
| `iter.NumRows()` | `Result.FetchedRows`, the rows fetched over every page |
| `query.Trace(tracer)` | `exec.Options{CQLTrace: true}`, then `cqltrace.Fetch`; see [tracing](tracing.md) |
| a keyspace other than the session's | a qualified table name; on `CassandraV5`, also `exec.Options{Keyspace: name}` or `exec.PrepareOptions{Keyspace: name}` |
| `query.Release()` | nothing to release; a reusable `prepared.Worker(options)` is closed with `Close` |

v1 sent a client timestamp with every request. v2 stamps only ordinary
statements. A statement left `cql.UnknownStatement`, as a ported v1
statement is, is conditional when its rows decode through `cql.CASRows` or
its CQL is an INSERT, UPDATE, DELETE or BATCH with an `IF` condition, and
ordinary otherwise, on every server; v1's `Query.IsLWT` knew it only from
ScyllaDB's prepared metadata. Declaring `Kind: cql.OrdinaryStatement` on a
statement the driver finds conditional fails before it is sent, as does
reading a SELECT through `cql.CASRows` or declaring it
`cql.ConditionalStatement`.

One-shot calls on a `*Prepared` take `exec.CompiledOptions` from
`session.CompileOptions`; compile them once and reuse them. A goroutine that runs
a statement in a loop can own a `*gocql.Worker` from `prepared.Worker(options)`,
which takes `exec.Options` directly. See [statements](statements/index.md).

### Struct mapping

v1 had no struct mapping in the driver; gocqlx added it. v2 maps struct fields
by their `cql` tags or names:

```go
type User struct {
	ID   cql.UUID `cql:"id"`
	Name string   `cql:"name"`
}

insertUser, err := session.Prepare(ctx, cql.Write(
	"INSERT INTO users (id, name) VALUES (?, ?)",
	cql.MustParamsOf[User](),
))
```

An untagged field maps the column named by its name in snake_case, as gocqlx
maps it: `CreatedAt` is `created_at` and `UserID` is `user_id`. Tag a field
whose column snake_case does not give, such as `UserIDs` (`user_i_ds`) or a
column named in one word (`createdat`).

| gocqlx | v2 |
| --- | --- |
| a `time.Time` field | the same; `cql.TimestampTimeCodec` maps it by default |
| a `*T` field for a nullable column | the same: nil is NULL (`cql.PointerCodec`); `cql.Nullable[T]` also works |
| a `[]T` field for a list, set or vector | the same; the column's kind picks the codec |
| a named type, such as `type Status string` | the same: it maps like its underlying type, also as `[]Status`, `*Status`, `cql.Nullable[Status]` or a map key or value |
| an `int` field | the same; it maps a bigint (`cql.IntCodec`) |
| a `*big.Int` field for a varint | the same (`cql.VarintBigIntCodec`) |
| a UDT struct implementing `gocqlx.UDT` | `cql.WithCodec(cql.MustUDTCodecOf[Address](cql.UDTOptions{Name: "address"}))`, which maps `[]Address`, `*Address` and map values too; `UDTOptions.Name` may be left empty |
| a tuple scanned into a struct | `cql.WithCodec(cql.MustTupleCodecOf[Point]())`, by field position |
| `SELECT *` into a struct missing a column | `cql.RowOf[T](cql.IgnoreExtraColumns())`; without it every result column needs a field |

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
for user, err := range page.All(nil) { // closes the page when the loop ends
	if err != nil {
		return err
	}
	// use user
}
next := page.State()
cursor, err := next.MarshalText() // the opaque cursor to hand out
```

`state.UnmarshalText(cursor)` restores the state, start and completion
included, an empty cursor being the start; `cql.PageStateFromToken(token)` restores one from the server token
alone. Either carries server state: authenticate or encrypt it before giving
it to an untrusted client. See [paging](statements/paging.md).

## Batches and lightweight transactions

| v1 | v2 |
| --- | --- |
| `session.NewBatch(gocql.UnloggedBatch)` | `batch := gocql.Batch{Kind: cql.UnloggedBatch}` |
| `batch.Query(stmt, values...)` | `prepared.AddToBatch(&batch, params)`, or `session.UnpreparedAddToBatch` |
| `session.ExecuteBatch(batch)` | `session.ExecBatch(ctx, &batch)` |
| `session.ExecuteBatchCAS(batch, dest...)`, `MapExecuteBatchCAS` | `session.ApplyBatch(ctx, &batch)` for the applied flag alone; `session.RunConditionalBatch(ctx, &batch, cql.CASRows(decoder))` for the rows |
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
| `gocql.UUID.Time`, `Timestamp`, `Clock`, `Node` | the same methods on `cql.TimeUUID`, with `Clock` returning `uint16` and `Node` `[6]byte`; convert a `cql.UUID` with `cql.TimeUUIDFromUUID`, which checks it is version 1 |
| `gocql.Consistency` constants (`gocql.Quorum`, ...) | `cql.Consistency` constants (`cql.Quorum`, ...); `cql.ParseConsistency` |
| `gocql.Marshaler`, `gocql.Unmarshaler` | `cql.ConvertCodec` for a type that converts to a built-in one; otherwise `cql.Encoder[T]`, `cql.Decoder[T]`, `cql.Codec[T]`; see [custom codecs](custom-codecs.md) |
| `gocql.UnsetValue` | the `Unset` binding state; see [data types](data-types.md) |
| `*T` or `gocql.NullX` for NULL | `cql.NullableCodec` or `cql.Nullable[T]`, and `cql.PointerCodec` for a `*T`, which struct mappings also use; `cql.NonNull(v)` builds a present `Nullable` |
| NULL scanned into a non-pointer, read as the zero value | an error matching `cql.ErrNull`: a column that may be NULL needs a `cql.Nullable[T]` or `*T` field |
| `int64` for a counter column | `int64`, with `cql.Int64Codec`; `cql.BigIntValue` binds a counter increment |
| `gocql.UUID.IsEmpty` | `cql.UUID.IsZero`, and `cql.TimeUUID.IsZero` |

## Errors

| v1 | v2 |
| --- | --- |
| `gocql.ErrNotFound` from `Scan` | `One` returns `found == false` |
| `*gocql.RequestErrWriteTimeout`, `RequestErrReadTimeout`, `RequestErrUnavailable` | `*cqlerr.WriteTimeoutError`, `*cqlerr.ReadTimeoutError`, `*cqlerr.UnavailableError` |
| `gocql.RequestError` and its `Code()` | `cqlerr.ServerError`, or `errors.Is(err, cqlerr.CodeOverloaded)` |
| `gocql.ErrTimeoutNoResponse`, `ErrNoConnections` | the same names in `cqlerr` |
| `context.DeadlineExceeded` from the query's own `Timeout` | an error matching `cqlerr.ErrRequestTimeout`, which also matches `context.DeadlineExceeded`; the caller's own deadline matches only the latter |
| `gocql.Ignore` from `RetryPolicy.GetRetryType` | removed: a policy returns `retry.Stop`, and the caller decides what an ignored failure means |
| `gocql.ErrSessionClosed` | `cqlerr.ErrClosed` |
| `gocql.ErrKeyspaceDoesNotExist` | removed: a schema refresh treats a keyspace the server no longer has as dropped; a missing keyspace is `cqlerr.ErrNoKeyspace` |
| `RequestErrWriteTimeout.WriteType`, `RequestErrWriteFailure.WriteType` (strings) | `cqlerr.WriteType` constants (`cqlerr.WriteTypeSimple`, `WriteTypeBatchLog`, `WriteTypeCAS`, ...); its `String` returns the server's name; a type the driver does not know is `WriteTypeUnknown` |
| `ErrorMap` keyed by an address string | `cqlerr.ErrorMap` keyed by `netip.Addr`; an IPv4 replica is keyed by its 4-byte address |
| `gocql.ErrNoHosts` | `ConfigBuilder.Build` rejects an empty contact point list with a `*cql.ValidationError` |

A failed execution returns a `*cqlerr.ExecutionError` that lists every attempt.
`cqlerr.KindOf` groups errors by cause, and `cqlerr.ExecutionStatusOf` reports
whether a retry could apply a write twice. See [errors](errors.md).

## Policies, metadata and observation

| v1 | v2 |
| --- | --- |
| `gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(dc))` | the default policy with `config.ConfigBuilder.WithLocalDatacenter(dc, "")`, or `routing.NewPolicy(routing.Options{Datacenter: dc})` |
| `gocql.TokenAwareHostPolicy(fallback, gocql.ShuffleReplicas())` | the default: replicas are shuffled unless `routing.Options.FixedReplicaOrder` is set |
| `gocql.DCAwareRoundRobinPolicy(dc)` without token awareness | `routing.Options{Datacenter: dc, NoTokenAware: true}` |
| `gocql.RackAwareRoundRobinPolicy(dc, rack)` | `WithLocalDatacenter(dc, rack)`, or `routing.Options{Datacenter: dc, Rack: rack, NoTokenAware: true}` without token awareness |
| `gocql.HostPoolHostPolicy` and other custom policies | implement `routing.Policy` (Pick and Fallback), and optionally `routing.Validator` and `routing.AttemptObserver`; test it on `routing/routingtest` snapshots; see [load balancing](load-balancing.md) |
| `&gocql.SimpleRetryPolicy{NumRetries: n}` | `retry.Safe{MaxAttempts: n + 1}`; it retries a non-idempotent request only after a failure that reports it was not executed, or a read timeout that returned no data |
| `&gocql.SimpleSpeculativeExecution{...}` | implement `retry.SpeculationPolicy`; see [speculation](speculative-execution.md) |
| `session.KeyspaceMetadata(name)` | `session.Metadata().Keyspace(name)` on an immutable `*metadata.Snapshot` |
| `session.KeyspaceMetadata(ks)` to read one table | `session.Metadata().Table(ks, name)`, which copies one table rather than the keyspace; `Keyspaces()` lists the loaded keyspaces and `Host(id)` finds one host |
| `KeyspaceMetadata.Functions[name]`, `Aggregates[name]` | keyed by signature, such as `"add(int,int)"`: `metadata.Keyspace.Function(name, argTypes...)`, or `FunctionsNamed(name)` for every overload |
| `gocql.ColumnPartitionKey`, `ColumnClusteringKey`, `ColumnRegular`, `ColumnCompact`, `ColumnStatic` | `metadata.ColumnKindPartitionKey`, `ColumnKindClusteringKey`, `ColumnKindRegular`, `ColumnKindCompact`, `ColumnKindStatic` |
| `HostInfo.HostID()` | `metadata.HostSnapshot.HostID` |
| `ViewMetadata.ReadRepairChance`, `DcLocalReadRepairChance` | `metadata.View.Options.ReadRepairChance`, `DCLocalReadRepairChance` |
| `ViewMetadata.KeyspaceName`, `ViewName`, `BaseTableName` | `metadata.View.Keyspace`, `Name`, `BaseTable`, as `metadata.Table` names them |
| `IndexMetadata.KeyspaceName`, `TableName` | `metadata.Index.Keyspace`, `Table` |
| `TableMetadata.Equals` and the other metadata `Equals` methods | `Equal` |
| `TableMetadataOptions.GcGraceSeconds`, `MemtableFlushPeriodInMs`, `DcLocalReadRepairChance`, `BloomFilterFpChance`, `CrcCheckChance` | `metadata.TableOptions.GCGraceSeconds`, `MemtableFlushPeriodInMS`, `DCLocalReadRepairChance`, `BloomFilterFPChance`, `CRCCheckChance` |
| `ColumnMetadata.ClusteringOrder` (string) and `Order` (`ASC`/`DESC`) | `metadata.Column.ClusteringOrder`, a `metadata.ColumnOrder`: `ColumnOrderAsc`, `ColumnOrderDesc`, or `ColumnOrderNone` for a column that is not a clustering column |
| `TableMetadata.ID`, `ViewMetadata.ID`, `BaseTableID` | `metadata.Table.ID`, `View.ID`, `View.BaseTableID` and `TableOptions.Version`, all `cql.UUID` |
| `gocql.Type`, `gocql.TypeInt` and the other kind constants | `cql.Kind`, `cql.KindInt`, ...; `gocql.TypeAscii` is `cql.KindASCII`; an ascii column maps with `cql.StringCodec` or `cql.TextCodec`, which validate ASCII for it |
| `gocql.TypeVarchar` and `gocql.TypeText` | `cql.KindText`: text and varchar are one CQL type, and a column of either has that kind, so their types compare `Equal` |
| `ColumnMetadata.Index` (`ColumnIndexMetadata`) | removed: it described the pre-3.0 schema tables, which v2 does not read; use `metadata.Keyspace.Indexes` |
| `session.AwaitSchemaAgreement(ctx)` | `session.AwaitSchemaAgreement(ctx)` |
| `gocql.ObservedQuery`, `QueryObserver` | `observe.RequestObserver`, `observe.AttemptObserver` |
| `gocql.ConnectObserver`, `HostStateNotifier` | `session.Subscribe(event.Filter{...})`: `event.KindPool` reports a host's pool connecting (`ChangeConnected`) or the host marked down (`ChangeDown`), `KindStatus` and `KindTopology` the cluster's own events |

## Not carried over

- `session.Query(...).Bind` and other mutable query objects: a statement is
  immutable and its parameters are a typed value.
- Reflection over arbitrary Go values on each call: build the encoder and
  decoder once.
- Protocol version negotiation and protocols 3 and older: pick a dialect.
- `USE` statements: set the session keyspace or qualify table names.
