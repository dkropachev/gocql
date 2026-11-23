# Public API review for v2.0.0

The symbol-by-symbol review of the 14 public packages that
`testdata/api.txt` lists, done for the "walk every exported symbol" item of
scylladb/gocql#1188. Each group of symbols gets one decision: **keep**,
**rename**, **unexport**, **remove** or **move**. Reasons are given only where
they are not obvious.

Decisions made earlier in the same review are not repeated here; they apply to
every group below:

- no public name aliases an internal type (`TestPublicAPI`);
- the package split of `docs/package-layout.md`;
- the execution verbs, `Unprepared*`, `With*`/`No*` and `New*`/`Must*` names;
- the execution grid of `docs/api-surface.md` (`TestExecutionGrid`);
- the dialect is a run-time value, not a type parameter, and dialect-only
  settings fail at run time (`docs/server-differences.md`); decision 8 moved
  it from a `Connect` argument into the configuration;
- which interfaces users may implement, and which are sealed;
- enum zero values and `String` output;
- every struct documents its zero value (`TestStructZeroValueDocs`).

The rule for anything uncertain: leave it out of v2.0. A symbol can be added in
v2.1; it can only be removed in v3.

## Removed in this review

| Symbol | Why |
|---|---|
| `cqlerr.ErrNoHosts` | Never returned: `ConfigBuilder.Build` rejects an empty contact point list with a `*cql.ValidationError` before a session sees it. |
| `cqlerr.ErrSessionNotReady` | Never returned: `Connect` returns a session only once it is ready. |
| `cqlerr.ErrSessionClosed` | A second "closed" sentinel. A closed session reports `cqlerr.ErrClosed`. |
| `cqlerr.ErrKeyspaceDoesNotExist` | Never returned by a public call (`TestKeyspaceAbsenceStaysInternal`): the schema refresh treats a keyspace the server no longer lists as dropped. It is an internal sentinel now, which wraps `ErrNoKeyspace`. Decision 5. |
| `cql.UUID.Time`, `Timestamp`, `Clock`, `Node` | Mean something only for a version 1 UUID; `cql.TimeUUID` keeps them. Decision 6. |
| `cqlerr.ErrNotFound` | Never returned: only an internal schema lookup that no public call reaches produced it. A missing keyspace or table is `ErrNoKeyspace` or `ErrNoTable`; a missing row is `found == false`. |
| `cqlerr.ExecutionStatus.Worse` | Engine plumbing for merging attempt logs. Users read the merged status from `ExecutionStatusOf` or `ExecutionError.ExecutionStatus`. The severity order stays documented on `ExecutionStatus`. |
| `endpoint.IdentityTranslator` | The same as setting no translator. |
| `metadata.Column.Index`, `metadata.ColumnIndex` | Described the pre-3.0 `system.schema_columns` index columns. The driver reads only `system_schema`, so they were always empty. Indexes are in `metadata.Keyspace.Indexes`. |
| `metadata.View.ReadRepairChance`, `DcLocalReadRepairChance` | Duplicated `View.Options`. The driver fills `View.Options.ReadRepairChance` and `DCLocalReadRepairChance`, as for a table. |
| `metadata.Column.Order`, `metadata.ASC`, `metadata.DESC` | Duplicated `Column.ClusteringOrder`. Decision 4. |

## `gocql`

| Symbols | Decision |
|---|---|
| `Connect` | Keep. It takes only the `config.Config` (decision 8). |
| `Session` and its methods | Keep. `ID` correlates with `system.clients`; `Closed`, `Metadata`, `Metrics`, `Subscribe`, `RefreshMetadata` and `AwaitSchemaAgreement` are the session-level operations. |
| `Prepared`, `PreparedStream`, `Query`, `StreamQuery`, `PreparedInfo`, `StreamInfo` | Keep. `Query.Routing` lets a caller see the route a value takes without executing. |
| `Rows`, `Page`, `CASBatchRows`, `RowStream`, `PageLease` | Keep. |
| `Batch`, `MaxBatchStatements` | Keep. |
| `StopIteration` | Keep. Named like `filepath.SkipDir`: a signal, not a failure. |

## `auth`

| Symbols | Decision |
|---|---|
| `Provider`, `Exchange`, `Password` | Keep. |

## `config`

| Symbols | Decision |
|---|---|
| `New`, `ConfigBuilder` and its setters, `Config` | Keep. Every setter is documented and used by the docs or examples. `New` takes the dialect first (decision 8). |
| `Dialect`, `ScyllaV4`, `CassandraV4`, `CassandraV5` | Moved here from `gocql` (decision 8). |
| `NewProfile`, `ProfileBuilder`, `Profile` | Keep. `Profile` fields are read by `exec.Options.Profile`. |
| `NewClientRoutes`, `ClientRoutesBuilder`, `ClientRoutes`, `ClientRoute` | Keep. |
| `NewTLSFiles`, `TLSFiles`, `TLSFileSource` | Keep. |

## `cql`

| Symbols | Decision |
|---|---|
| `Statement`, `StreamStatement`, `Command`, `StatementKind` and its values | Keep. |
| `Consistency` and its values, `ParseConsistency`, `MustParseConsistency`, `BatchKind` and its values, `Compression` and its values | Keep. |
| `Option`, `Some`, `Nullable`, `MaybeEmpty`, `Pair`, `Entry`, `CASResult`, `EmptyRow`, `EmptyParams`, `EmptyRowDecoder`, `EmptyParamsEncoder` | Keep. `EmptyParams` is an alias of `EmptyRow` inside the package, so it adds no surface. |
| Codec interfaces and plans (`Codec`, `Encoder`, `Decoder`, `ParamEncoder`, `RowDecoder`, `Stream*Decoder`, `StreamResetter`, `Compiled*`) | Keep (decided under extensibility). |
| Built-in codecs (`*Codec`, `Stream*` values) and combinators (`List*`, `Set*`, `Map*`, `GoMapCodec`, `Vector*`, `Nullable*`, `EmptyCodec`, `BindEncoder`, `Tuple*`, `UDT*`, `NamedScalar`, `Convert*`, `Pair*`, `OneColumn`, `OneParam`, `CASRows`) | Keep. |
| Struct mapping (`RowOf`, `ParamsOf`, `StreamRowOf`, their `Must*` twins, `Row`, `RowInto`, `Params`, `Column`, `ColumnAt`, `Param`, `ParamAt`, `IgnoreColumn`, `StreamColumn*`, `StreamRowInto`, `StructOption`, `WithCodec`, `WithStreamCodec`, `ReuseBuffers`, `StructFields`) | Keep. `InputField`, `OutputField`, `StreamOutputField` and `StructOption` are sealed. |
| Readers and writers (`ValueReader`, `ValueCursor`, `ValueWriter`, `RowReader`, `ParamWriter`, `StreamRowReader`, `StreamValueReader`, `StreamCell`) | Keep. A custom codec needs them. |
| `Bytes`, `Text`, `BorrowBytes`, `BorrowText`, `EncodeValueBorrowed`, `ErrReleased` | Keep. The zero-copy path is a v2 feature. |
| Dynamic values (`Value`, `*Value` constructors, `ValueEntry`, `ValueField`, `ValueIterator`, `DynamicRow`, `DynamicRows`, `DynamicCodec`, `DynamicParams`, `NamedDynamicParams`, `EncodedValue`, `NewEncodedValue`, `EncodeValue`, `DecodeValue`, `CellState` and its values) | Keep. |
| `DataType` and its methods, `Type` and `Type*` values, `*Type` variables and constructors, `ScalarType`, `ColumnSpec`, `Columns`, `NewColumns`, `UDTOptions`, `UDTOptionsFor` | Keep. `DataType.FrozenKnown` stays: result metadata carries no frozen flag, so a caller needs to tell "not frozen" from "unknown". |
| Scalar types (`UUID`, `TimeUUID`, `Date`, `Timestamp`, `TimeOfDay`, `Duration`, `Decimal`, `DecimalParts`, `Varint`, `Counter`) and their helpers, the `Variant*` constants that `Variant` returns | Keep, less `UUID`'s time methods (decision 6). `TypeAscii` is `TypeASCII` (decision 3). |
| `Result`, `CQLTraceRef`, `PayloadEntry`, `PageState`, `PageStateFromToken`, `StartPage`, `Unpaged` | Keep; `StartPage` is a function (fourth pass). |
| `ValidationError`, `CodecError`, `ProtocolError`, `StreamCapabilityError`, `StreamCapacityError` | Keep. |

## `cqlerr`

| Symbols | Decision |
|---|---|
| `ServerError` (sealed), the server error types, `NewOtherServerError`, `ErrorCode` and the `Code*` values, `ErrorMap`, `OpType` and `WriteType` with their values | Keep. `ErrorMap` and `WriteType` are typed (decision 7). `NewOtherServerError` exists because `OtherServerError` keeps its code unexported; a `retry.Policy` test needs it. |
| `ExecutionError`, `AttemptFailure`, `QueryError`, `ExecutionStatus` and its values, `ExecutionStatusOf`, `IsCASWriteUnknown` | Keep. `IsCASWriteUnknown` hides the protocol 4 / protocol 5 difference, which `errors.As` alone does not. |
| `Kind`, `KindOf` and the `Kind*` values | Keep. |
| `ConnectError`, `HostError`, `NoHostsError`, `PartialRefreshError`, `SchemaChangedError`, `ServerMismatchError`, `UnsupportedSettingError` | Keep. |
| Sentinels a request or handle can return: `ErrBusy`, `ErrClosed`, `ErrConditionalExec`, `ErrStaleRows`, `ErrNoRows`, `ErrUseStmt`, `ErrPagingComplete`, `ErrPagingNoProgress`, `ErrPagingStateLimit`, `ErrNoKeyspace`, `ErrNoTable`, `ErrNoConnections`, `ErrNoConnectionsStarted`, `ErrNoControl`, `ErrNoStreams`, `ErrConnectionClosed`, `ErrTimeoutNoResponse`, `ErrReadHeaderTimeout`, `ErrFrameTooBig` | Keep. Each can reach a caller, directly or inside an `ExecutionError` attempt, a `ConnectError` or a `PartialRefreshError`. |
| `ErrScyllaSourcePortAlreadyInUse` | Keep. The driver never returns it; a custom `endpoint.Dialer` returns it to ask for the next source port. `endpoint.DialTarget.LocalAddress` documents this. |

## `cqltrace`

| Symbols | Decision |
|---|---|
| `Fetch`, `Fetcher`, `Info`, `Event`, `ErrNotReady` | Keep. |

## `endpoint`

| Symbols | Decision |
|---|---|
| `Dialer`, `DialerFunc`, `DefaultDialer`, `DialTarget` | Keep. `DefaultDialer` lets a custom dialer wrap the default. |
| `Resolver`, `Translator`, `TranslatorFunc`, `Peer`, `TLSSource`, `ReconnectPolicy` | Keep. |
| `ConvictionPolicy`, `SimpleConvictionPolicy`, `ConvictionEvent`, `ConvictionSource` and its values | Keep. |

## `event`

| Symbols | Decision |
|---|---|
| `Event`, `Filter`, `Kind` and its values, `Subscription` | Keep. |

## `exec`

| Symbols | Decision |
|---|---|
| `Options`, `CompiledOptions`, `PrepareOptions`, `StreamOptions`, `StreamLimits`, `SerialOverride` and its values | Keep. |

## `metadata`

| Symbols | Decision |
|---|---|
| `Snapshot`, `HostSnapshot`, `Route`, `Target`, `TokenRange`, `Mode` and its values | Keep. |
| `Keyspace`, `Table`, `TableOptions`, `View`, `Column`, `Index`, `Type`, `Function`, `Aggregate`, and `ColumnKind`, `ColumnOrder` and `IndexKind` with their values | Keep, less the fields removed above and with decisions 1 to 4 and 7 applied. `Keyspace.ToCQL` and `Clone` stay. |

## `metrics`

| Symbols | Decision |
|---|---|
| `Snapshot` | Keep (decided under observability). |

## `observe`

| Symbols | Decision |
|---|---|
| `Observer`, `RequestObserver`, `AttemptObserver`, `RequestStart`, `RequestEvent`, `LifecycleObserver`, `LifecycleEvent` | Keep (decided under extensibility). |

## `retry`

| Symbols | Decision |
|---|---|
| `Policy`, `PolicyFunc`, `Safe`, `Never`, `Attempt`, `Decision`, `Action` and its values, `SpeculationPolicy` | Keep. |

## `routing`

| Symbols | Decision |
|---|---|
| `Policy`, `Plan`, `PlanOf`, `NewPolicy`, `Options`, `LatencyOptions`, `SingleTarget` | Keep. |
| `Info`, `AttemptStart`, `AttemptEvent` | Keep. |
| `HostRoutes`, `ReplicaRoutes` | Keep. A custom policy builds on them (`docs/source/load-balancing.md`). |
| `TokenPartitioner`, `PartitionKey`, `Murmur3TokenPartitioner`, `CDCTokenPartitioner` | Keep. `config.WithPartitioner` takes them. |

## Second review pass (2026-11-22)

A later pass over the whole surface found these. Each is resolved without
changing a public symbol.

| Finding | Outcome |
|---|---|
| `cqlerr.ErrNoRows` was documented as "a query expected to return a row returned none", but `Clone` and `Retain` on `Rows`, `Page` and `CASBatchRows` return it when there is no current row, and `One` reports a missing row as `found == false` | Documented as "no current row", on the sentinel and on each `Clone` and `Retain`. |
| `metadata.HostSnapshot.Tokens`, `Snapshot.Token` and `routing.TokenPartitioner` are `int64` | Kept: ScyllaDB only uses Murmur3, and it is Cassandra's default. A `RandomPartitioner` or `ByteOrderedPartitioner` cluster fails to connect, naming the invalid token (`TestRandomPartitionerClusterIsRejected`); `docs/source/server-differences.md` says so. |
| Unclear which connection settings apply to a custom `endpoint.Dialer` | All of them: the connect timeout through the dial context, the source address through `DialTarget.LocalAddress` (checked), and the socket options set on the returned connection. Documented on `ConfigBuilder.WithDialer`. |
| `metadata.Column.Type` is a schema string, not a `cql.DataType` | Deferred to a compatible addition; see `docs/post-release-improvements.md`. |
| A `config.Profile` written as a literal is rejected only at run time | Kept. Its fields are exported for `exec.Options.Profile` readers, and the rejection is loud: `WithDefaultProfile`, the query constructors and `CompileOptions` return a validation error, and the type's doc says to use `NewProfile`. |

## Third review pass (2026-11-22)

A third pass found these. Each changes a public symbol, so it lands before
`v2.0.0-rc.1`.

| Finding | Outcome |
|---|---|
| `event.Event.ArgumentTypes` held a ClientRoute event's connection IDs followed by its host IDs, with no boundary between them | `event.Event.ConnectionIDs` and `HostIDs` (`[]cql.UUID`); `ArgumentTypes` is for functions and aggregates only. A host ID the server sends that does not parse is logged and dropped. |
| `event.Event.Change` and `Target`, and `observe.LifecycleEvent.Kind` and `Stage`, were plain strings documented by example | Typed strings with constants: `event.Change` (`ChangeNewNode` ... `ChangeTabletResync`, including the driver's `ChangeResync` and `ChangeRefreshed`), `event.Target`, `observe.LifecycleKind` and `observe.LifecycleStage`. A server's text the driver does not name still arrives unchanged. |
| `observe.LifecycleEvent.Err` and `Stream` were never set, and `LifecycleObserver` promised connecting, startup and authentication events the driver does not send | Both fields removed; the doc lists the events sent. They can come back in v2.1 when the driver fills them. |
| `cqlerr.AttemptFailure.Shard` and `endpoint.ConvictionEvent.Shard` beside `ShardID` everywhere else | Renamed `ShardID`. |
| `cqlerr.ExecutionError.BatchStatements` was a count, while `observe.RequestStart.BatchStatements` and `RequestEvent.BatchStatements` are the statement texts | The count is `ExecutionError.BatchSize`. |
| `cql.Text.String` panicked on an expired view, so formatting or logging one did | Returns `"<released>"`; `Copy` stays the checked form. |
| `metadata.ColumnKind` was the only `int` enum | `uint8`, like the others. |
| `metadata.Keyspace.Clone` named an unexported field; `cqlerr.AttemptFailure.Address` and `HostError.Address` did not say why they are text | Docs fixed: an unresolved contact point has no IP and a Unix socket has a path. |

## Fourth review pass (2026-11-23)

A fourth pass found these. Each changes a public symbol, so it lands before
`v2.0.0-rc.1`.

| Finding | Outcome |
|---|---|
| `metrics.Snapshot.Cancelled` and `routing.AttemptEvent.Cancelled` beside `cqlerr.KindCanceled` and Go's `context.Canceled` | Renamed `Canceled`. |
| `cqlerr.FunctionFailureError.ArgTypes`, while `metadata.Function`, `metadata.Aggregate` and `event.Event` say `ArgumentTypes` | Renamed `ArgumentTypes`. |
| `metadata.Route.Shard` and `WithShard`, missed by the third pass's `ShardID` rename; `metadata.Target.ShardID` is in the same package | Renamed `ShardID` and `WithShardID`. `ShardOf` stays: it computes a shard, not the route's. |
| `endpoint.DefaultDialer.SocketKeepalive`, which the `WithDialer` doc tells users to set to the value of `config.ConfigBuilder.WithTCPKeepalive` | Renamed `TCPKeepalive`. |
| `cql.TimeUUID.Clock` returned a `uint32` and `Node` a `[]byte`, while `cql.TimeUUIDWith` takes a `uint16` clock and a `[6]byte` node | `Clock` returns `uint16` and `Node` `[6]byte`, the zero array for a UUID that is not version 1, so the values round-trip. |
| `ConfigBuilder.WithHeartbeat(interval, timeout)` and `WithSchemaAgreement(timeout, poll)` order their two durations oppositely, and swapping them compiles | `WithSchemaAgreement(poll, timeout)`, the period first in both. |
| `ProfileBuilder.NoTimeout` and `NoPrefetch` were `WithTimeout(0)` and `WithPrefetch(0)`, and `ConfigBuilder.WithServerTimestamps` was `WithTimestampGenerator(nil)`, against the builder rule that zero or nil turns a setting off with no `No*` twin | Removed; the remaining setters document their zero or nil. `NoSerialConsistency` and `NoAdvancedShardAwareness` stay, since zero cannot express them. |
| `cql.Type` was an `int` | `uint16`, the width of the protocol's type ID, so a type ID the driver does not name cannot alias a known one. |
| `cql.StartPage` was a variable the driver itself reads, so assigning it broke paging | `cql.StartPage()`, a function returning the zero `PageState`. |
| `cql.CodecError.Stage` was a string with five documented values | `cql.CodecStage` with constants `CodecStageCompile` ... `CodecStageStreamDecode`; the values and messages are unchanged. `ProtocolError.Stage`, `ConnectError.Stage` and `NoHostsError.Operation` stay strings: they are open-ended diagnostics. |
| `observe` named its kinds `LifecycleSession` and `LifecycleHost` but its stages `StageReady` ... | `KindSession` and `KindHost`. |
| `docs/server-differences.md` and `docs/source/connecting/index.md` still named `AttemptFailure.Shard` and `ConvictionEvent.Shard` | Docs updated to `ShardID`. |

Built-in `retry.SpeculationPolicy` and `endpoint.ReconnectPolicy`
implementations are additions, left for v2.1.

## Decisions

These changed names or removed symbols that users of the earlier v2 code may
rely on, so they waited for a maintainer's call before `v2.0.0-rc.1`. All
seven were decided as recommended and are applied; the migration guide maps
the v1 names. Decision 8 came from a later review pass.

1. **Schema name fields. Decided: renamed.** `metadata.Table` uses `Keyspace`
   and `Name`; `View` used `KeyspaceName`, `ViewName` and `BaseTableName`,
   and `Index` used `KeyspaceName` and `TableName`. They are now
   `View.Keyspace`, `View.Name`, `View.BaseTable`, `Index.Keyspace` and
   `Index.Table`, so the three types read alike.
2. **`Equals` versus `Equal`. Decided: renamed to `Equal`.** `cql.Columns`
   and `cql.DataType` already had `Equal`, the usual Go spelling;
   `metadata.Table`, `TableOptions` and `Column` now match.
3. **Go initialisms. Decided: renamed.** `metadata.TableOptions` has
   `GCGraceSeconds`, `MemtableFlushPeriodInMS`, `DCLocalReadRepairChance`,
   `BloomFilterFPChance` and `CRCCheckChance`. A scan of `testdata/api.txt`
   for other initialisms found one more, `cql.TypeAscii`, now `TypeASCII`
   like `cql.ASCIICodec`.
4. **Clustering order was stored twice. Decided: one typed field.**
   `metadata.Column.ClusteringOrder` is a `metadata.ColumnOrder`
   (`ColumnOrderNone`, `ColumnOrderAsc`, `ColumnOrderDesc`), whose zero value
   is the order of a column that is not a clustering column. The string field
   and the `bool` `Column.Order` with `ASC` and `DESC` are gone.
5. **Two "no such keyspace" sentinels. Decided: unexported.**
   `TestKeyspaceAbsenceStaysInternal` shows that the refreshes a public call
   runs turn a keyspace the server no longer lists into a dropped keyspace,
   and the single-keyspace refresh returns it only to internal callers: the
   post-DDL refresh, which ignores it, the schema event handler, which
   schedules a retry, and the schema describer lookups, which no public call
   reaches.
   `cqlerr.ErrKeyspaceDoesNotExist` is now an internal sentinel that wraps
   `cqlerr.ErrNoKeyspace`, so it classifies as that should it ever escape.
6. **Time methods on `cql.UUID`. Decided: removed.** `Time`, `Timestamp`,
   `Clock` and `Node` remain on `cql.TimeUUID`; convert a `UUID` with
   `TimeUUIDFromUUID`, which checks the version.
7. **String-typed fields. Decided: typed.** `metadata.Table.ID`, `View.ID`,
   `View.BaseTableID` and, for the same reason, `TableOptions.Version` are
   `cql.UUID`. `cqlerr.WriteTimeoutError.WriteType` and
   `WriteFailureError.WriteType` are a `cqlerr.WriteType`, an integer enum
   with the protocol's write types, `WriteTypeUnknown` as its zero value for a
   type the driver does not name (its text is not kept), and a `String`
   method that returns the server's name. `cqlerr.ErrorMap` is keyed by
   `netip.Addr`, an IPv4 replica by its 4-byte address.
8. **Where the dialect lives. Decided: in the configuration.** The dialect
   was the third argument of `gocql.Connect`. It is now the first argument of
   `config.New(dialect, contactPoints...)`, the type and values moved to
   `config.Dialect`, `config.ScyllaV4`, `config.CassandraV4` and
   `config.CassandraV5`, and `Connect` takes only the `config.Config`. The
   dialect is deployment data like the contact points, so it belongs with
   them; `Config.String` reports it; and `Build` now rejects a setting the
   dialect does not support (client routes or a metadata server timeout on
   Cassandra, Snappy on protocol 5) where it was written, instead of at
   `Connect`. `Session.Dialect` returns a `config.Dialect`. The zero
   `Dialect` fails `Build` with a `cql.ValidationError`.
