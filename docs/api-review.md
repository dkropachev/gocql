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
- the dialect is a `Connect` argument, and dialect-only settings fail at run
  time (`docs/server-differences.md`);
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
| `cqlerr.ErrNotFound` | Never returned: only an internal schema lookup that no public call reaches produced it. A missing keyspace or table is `ErrNoKeyspace` or `ErrNoTable`; a missing row is `found == false`. |
| `cqlerr.ExecutionStatus.Worse` | Engine plumbing for merging attempt logs. Users read the merged status from `ExecutionStatusOf` or `ExecutionError.ExecutionStatus`. The severity order stays documented on `ExecutionStatus`. |
| `endpoint.IdentityTranslator` | The same as setting no translator. |
| `metadata.Column.Index`, `metadata.ColumnIndex` | Described the pre-3.0 `system.schema_columns` index columns. The driver reads only `system_schema`, so they were always empty. Indexes are in `metadata.Keyspace.Indexes`. |
| `metadata.View.ReadRepairChance`, `DcLocalReadRepairChance` | Duplicated `View.Options`. The driver fills `View.Options.ReadRepairChance` and `DcLocalReadRepairChance`, as for a table. |

## `gocql`

| Symbols | Decision |
|---|---|
| `Connect`, `Dialect`, `ScyllaV4`, `CassandraV4`, `CassandraV5` | Keep. |
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
| `New`, `ConfigBuilder` and its setters, `Config` | Keep. Every setter is documented and used by the docs or examples. |
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
| Scalar types (`UUID`, `TimeUUID`, `Date`, `Timestamp`, `TimeOfDay`, `Duration`, `Decimal`, `DecimalParts`, `Varint`, `Counter`) and their helpers, the `Variant*` constants that `Variant` returns | Keep. See the open decision on `UUID`'s time methods. |
| `Result`, `CQLTraceRef`, `PayloadEntry`, `PageState`, `PageStateFromToken`, `StartPage`, `Unpaged` | Keep. |
| `ValidationError`, `CodecError`, `ProtocolError`, `StreamCapabilityError`, `StreamCapacityError` | Keep. |

## `cqlerr`

| Symbols | Decision |
|---|---|
| `ServerError` (sealed), the server error types, `NewOtherServerError`, `ErrorCode` and the `Code*` values, `ErrorMap`, `OpType` and its values | Keep. `NewOtherServerError` exists because `OtherServerError` keeps its code unexported; a `retry.Policy` test needs it. |
| `ExecutionError`, `AttemptFailure`, `QueryError`, `ExecutionStatus` and its values, `ExecutionStatusOf`, `IsCASWriteUnknown` | Keep. `IsCASWriteUnknown` hides the protocol 4 / protocol 5 difference, which `errors.As` alone does not. |
| `Kind`, `KindOf` and the `Kind*` values | Keep. |
| `ConnectError`, `HostError`, `NoHostsError`, `PartialRefreshError`, `SchemaChangedError`, `ServerMismatchError`, `UnsupportedSettingError` | Keep. |
| Sentinels a request or handle can return: `ErrBusy`, `ErrClosed`, `ErrConditionalExec`, `ErrStaleRows`, `ErrNoRows`, `ErrUseStmt`, `ErrPagingComplete`, `ErrPagingNoProgress`, `ErrPagingStateLimit`, `ErrNoKeyspace`, `ErrNoTable`, `ErrNoConnections`, `ErrNoConnectionsStarted`, `ErrNoControl`, `ErrNoStreams`, `ErrConnectionClosed`, `ErrTimeoutNoResponse`, `ErrReadHeaderTimeout`, `ErrFrameTooBig` | Keep. Each can reach a caller, directly or inside an `ExecutionError` attempt, a `ConnectError` or a `PartialRefreshError`. |
| `ErrKeyspaceDoesNotExist` | Kept for now; see open decision 5. |
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
| `Keyspace`, `Table`, `TableOptions`, `View`, `Column`, `Index`, `Type`, `Function`, `Aggregate`, and `ColumnKind`, `ColumnOrder` and `IndexKind` with their values | Keep, less the fields removed above. `Keyspace.ToCQL` and `Clone` stay. See the open decisions on names and the clustering order. |

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

## Open decisions

These would change names or remove symbols that users may already rely on, so
they need a maintainer's call before `v2.0.0-rc.1`. Nothing below was changed.

1. **Schema name fields.** `metadata.Table` uses `Keyspace` and `Name`, but
   `View` uses `KeyspaceName` and `ViewName`, and `Index` uses `KeyspaceName`
   and `TableName`. Renaming them to `Keyspace`/`Name`/`Table` would make the
   three types read alike.
2. **`Equals` versus `Equal`.** The `metadata` types have `Equals`; `cql.Columns`
   and `cql.DataType` have `Equal`, the usual Go spelling. Rename the
   `metadata` methods to `Equal`, or unexport them, since the driver uses them
   only to detect schema changes.
3. **Go initialisms in `metadata.TableOptions`.** `GcGraceSeconds`,
   `MemtableFlushPeriodInMs`, `DcLocalReadRepairChance` and
   `BloomFilterFpChance` keep their v1 spelling; Go style would be
   `GCGraceSeconds`, `MemtableFlushPeriodInMS`, `DCLocalReadRepairChance` and
   `BloomFilterFPChance`.
4. **Clustering order is stored twice.** `metadata.Column.ClusteringOrder`
   (`"asc"`, `"desc"`, `"none"`) and `Column.Order` (`ASC`/`DESC`, a `bool`)
   say the same thing. Keep one; `ColumnOrder` being a `bool` also leaves no
   room for "not a clustering column".
5. **Two "no such keyspace" sentinels.** `cqlerr.ErrNoKeyspace` covers both
   an empty name and one the metadata does not have; `ErrKeyspaceDoesNotExist`
   is the server's answer while a keyspace is refreshed. Every refresh path
   seen in this review catches `ErrKeyspaceDoesNotExist` and treats the
   keyspace as dropped, so it looks like engine control flow. Unexport it, or
   fold it into `ErrNoKeyspace`, once a test proves no public call returns it.
6. **Time methods on `cql.UUID`.** `UUID` has `Time`, `Timestamp`, `Clock` and
   `Node`, which mean something only for a version 1 UUID, and `TimeUUID` has
   the same methods with a checked version. Drop them from `UUID` and point
   callers at `TimeUUIDFromUUID`.
7. **String-typed fields that could be typed.** `metadata.Table.ID` and
   `View.ID`/`BaseTableID` are UUID strings rather than `cql.UUID`;
   `cqlerr.WriteTimeoutError.WriteType` and `WriteFailureError.WriteType` are
   strings rather than an enum; `cqlerr.ErrorMap` is keyed by an address string
   rather than `netip.Addr`. Typing them later is a breaking change.
