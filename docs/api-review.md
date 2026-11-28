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
| `Session.RunBatch` | Duplicated `Session.RunConditionalBatch`: only a conditional batch returns rows, and `CASBatchRows[X]` has every method of `Rows[cql.CASResult[X]]` plus `Applied`. It also sent a batch known to have no condition, which `RunConditionalBatch` rejects. |

## `gocql`

| Symbols | Decision |
|---|---|
| `Connect` | Keep. It takes only the `config.Config` (decision 8). |
| `Session` and its methods | Keep. `ID` correlates with `system.clients`; `Closed`, `Metadata`, `Metrics`, `Subscribe`, `RefreshMetadata` and `AwaitSchemaAgreement` are the session-level operations. |
| `Prepared`, `PreparedStream`, `Query`, `StreamQuery`, `PreparedInfo`, `StreamInfo` | Keep. `Query.Routing` lets a caller see the route a value takes without executing. |
| `Rows`, `Page`, `CASBatchRows`, `RowStream`, `PageLease` | Keep. |
| `Batch`, `MaxBatchStatements` | Keep. `Batch.Size` reports the encoded value bytes, which the server's `batch_size_fail_threshold` limits, while `MaxBatchStatements` is only the protocol's count limit. |
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
| `NewTLSFileSource`, `TLSFiles`, `TLSFileSource` | Keep. |

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

## Fifth review pass (2026-11-24)

A fifth pass found these. Each changes a public symbol or a documented
contract, so it lands before `v2.0.0-rc.1`.

| Finding | Outcome |
|---|---|
| `gocql.PreparedInfo.Conditional` was documented as the server's lightweight transaction flag but was `Kind == ConditionalStatement`, true for a statement only declared conditional; `routing.Info.Conditional` was `Kind != OrdinaryStatement`; `retry.Attempt.Conditional` copied the routing value but was documented as "a lightweight transaction". One name, two meanings, both derived from `Kind` | `PreparedInfo.Conditional` and `routing.Info.Conditional` removed; `PreparedInfo.Kind` documents how the server's flag folds into it. `retry.Attempt.Conditional` is `retry.Attempt.Kind`, a `cql.StatementKind`, so a policy can tell an unclassified statement from a conditional one. |
| `endpoint.ReconnectPolicy.Delay(attempt)` did not say where `attempt` starts; the driver counted from 0 while every other attempt number in the API counts from 1. The session also called `Delay(0)` while connecting only to reject a negative delay, so a stateful or jittered policy saw an extra call | `attempt` counts from 1, from the node's last successful connection; the default backoff is unchanged. The sixth pass replaced this probe with one `Delay(1)` call in `Build`. The doc lists every call the driver makes. |
| `cqlerr.HostError`'s doc called its zero value "an error", but the type has no `Error` method | Doc fixed. |
| `metadata.Mode`'s values were `SchemaNone`, `SchemaKeyspaces` and `SchemaFull` | `ModeNone`, `ModeKeyspaces` and `ModeFull`, like the fourth pass's `observe.KindSession`. |
| `config.NewTLSFiles` returned a `*config.TLSFileSource` | Renamed `NewTLSFileSource`. |
| `cql.ValueWriter.BorrowString` beside `cql.BorrowText` | Renamed `BorrowText`. |
| `cql.BatchKind` was declared `byte`; every other enum is `uint8` | `uint8`; the same type to the compiler. |
| `cql.UUID.UnmarshalText(text []byte) (err error)` beside `cql.TimeUUID.UnmarshalText(data []byte) error` | Spelled as on `TimeUUID`; behaviour unchanged. |

## Sixth review pass (2026-11-24)

A sixth pass, with one reviewer per package group, found these. Each changes
a public symbol or a documented contract, so it lands before `v2.0.0-rc.1`.
Two findings were not taken: `gocql.Rows` and `CASBatchRows` stay values,
since a pointer would cost an allocation per `Run` or lose stale-handle
detection, and `cql.StreamStatement` keeps its field order, which the
field-alignment lint requires.

### routing / config locality

| Finding | Outcome |
|---|---|
| The zero `routing.Options` turned off token awareness, replica shuffling and leader-aware routing, so `NewPolicy(Options{Datacenter: "dc1"})` silently lost token and shard awareness, and the migration guide's snippet lost shuffling and leader awareness | `TokenAware`, `ShuffleReplicas` and `LeaderAware` are `NoTokenAware`, `FixedReplicaOrder` and `NoLeaderAware`: the zero `Options` is the session's default policy. `NoLeaderAware == false` with `NoTokenAware` is accepted as is. |
| The default policy had no local datacenter while the default consistency is LOCAL_QUORUM, and a profile from `NewProfile()` did not inherit the session's routing | `ConfigBuilder.WithLocalDatacenter(dc, rack)` sets the session's locality, which every built-in policy without a `Datacenter` of its own takes, in every profile. `Build` rejects a rack without a datacenter; Connect fails through the policy's validation when the datacenter or rack has no eligible host, and logs a warning for a multi-datacenter cluster with no locality set. The driver configuration report shows the effective locality. |
| `exec.Options.HostID`/`ShardID` were ignored under a custom routing policy | A pinned request follows the built-in pinned plan before any policy is consulted or validated, `SingleTarget` included. |
| `Policy.Validate` ran per page, not once per snapshot as documented | An application policy's `Validate` runs once per snapshot until it passes; the built-in policy already cached by host index. |
| `routing.Policy` required `Validate` and `Observe` of every policy | `Policy` is `Pick` and `Fallback`; the optional `routing.Validator` and `routing.AttemptObserver` are found by type assertion. |
| `routing.AttemptStart` lived in `routing` though only observers use it | Moved to `observe.AttemptStart`. |
| No `*Func` adapter for `TokenPartitioner` | Added `routing.TokenPartitionerFunc`. |
| A custom policy could not be tested without a session: the built-in policies found no route and no public snapshot constructor existed | New package `routing/routingtest` with `NewSnapshot(Cluster)`, from hosts, schema, token ranges, tablets and a session locality; it is layer 3 because it links the engine, so the built-in policies route over its snapshots. |

### Execution semantics

| Finding | Outcome |
|---|---|
| A statement left `UnknownStatement`, `cql.Command` included, sent no client timestamp, kept a fixed replica order and a strict shard pin, and never speculated on Cassandra and for every Unprepared call, while the `WithTimestampGenerator` doc said every request is stamped | A statement left `UnknownStatement` whose `R` is `cql.EmptyRow` executes as `OrdinaryStatement`, prepared or not; as a batch member it stays unknown, so a conditional batch of `EmptyRow` members still reaches `RunConditionalBatch`. `UnknownStatement` and `WithTimestampGenerator` document the effects. |
| Idempotence was per call only, so speculation and `retry.Safe` needed options at every call site | Added `cql.Statement.Idempotent` (and `StreamStatement.Idempotent`), carried into prepared statements, their queries and batch members; a batch is idempotent when every member is. `exec.Options.Idempotent` is a `cql.Option[bool]` that overrides it. `retry.Safe` and the speculation gate read the effective value. |
| `exec.Options.Timeout` could only shorten the profile's 30s limit, which covers every page, so a long `All` scan died at 30s | A set `Timeout` or `PageTimeout` replaces the profile's, longer or shorter; `cql.Some(time.Duration(0))` removes it. |
| `exec.Options.PageSize` was a `cql.Option[int32]` although zero was never valid | `PageSize` is an `int32`; zero inherits the profile's, a negative value is rejected. |
| Unprepared calls with parameters and no `BindMarkers` silently bound nothing (`ParamsOf`) or failed late (`DynamicParams`) | They fail with a `cql.ValidationError` before anything is sent. `cql.DynamicParams` without markers takes their types from the values. `NamedDynamicParams` still needs `BindMarkers`: deriving them from the fields' order would defeat its name-to-marker mapping. |
| `All`/`Each`/`One` required `dst` and yielded the same pointer, so `append(out, row)` aliased every element | A nil `dst` decodes each row into a new `R` the caller may keep (`One` then only reports whether a row exists); the docs spell out that a non-nil `dst` is the reused pointer. |
| `Rows`, `Page` and `CASBatchRows` had no range form | Added `All(dst) iter.Seq2[*R, error]` on each; the loop closes the handle, and a page's `State` stays readable. |
| `Rows`/`CASBatchRows` are values while `Page`/`RowStream` are pointers | Kept as values. A `Rows` handle is a generation-checked value so that a stale copy reports `ErrStaleRows` while the query reuses its storage allocation-free; a pointer would cost an allocation per `Run`/`RunConditionalBatch` (breaking the documented allocation-free resend) or lose stale detection. `Page` is a pointer because each `RunPage` allocates one anyway. |
| `Prepared.Query(exec.Options)` was the only method with required options | `Prepared.Query` and `PreparedStream.Query` take at most one options value, variadic. |
| `PageState` had no full-state serialization, and `PageStateFromToken(nil)` is a continuation, not the start | `PageState` implements `encoding.BinaryMarshaler`/`TextMarshaler` and their unmarshalers: a kind byte then a continuation's token, and unpadded URL-safe base64 of it as text, so start and completion round-trip. |
| `example_paging_test.go` used a worker query and `NextInto` | Rewritten around `Prepared.RunPage(ctx, v, state, 2)`, `page.All(nil)` and a text cursor. |

### Errors and retry

| Finding | Outcome |
|---|---|
| `ExecutionError.Unwrap` returned every attempt's error, so `errors.Is(err, CodeOverloaded)` could hold while `KindOf` said timeout, and `errors.As` could return an unrelated earlier attempt's error | `Unwrap() error` returns `Err` only; earlier attempts are read from `Attempts`. `IsCASWriteUnknown` checks every attempt, so an LWT timeout the caller's cancellation ended is still reported. |
| `retry.IgnoreWrite` turned any failure into an empty success, reads included, so a read timeout became `One` reporting "no row" | Removed. Its old value fails the request as an unknown action, on every execution path. |
| The caller's expired context was `KindTimeout` and Transient, indistinguishable from the driver's own budget, so a `for KindOf(err).Transient()` loop spun forever | The execution and page budgets use `context.WithTimeoutCause(…, cqlerr.ErrRequestTimeout)` and errors report the context's cause. `ErrRequestTimeout` matches `context.DeadlineExceeded` and is `KindTimeout`; the caller's own deadline does not match it. `Kind.Transient`'s doc says to check `ctx.Err()` in a retry loop. |
| `ExecutionStatus`'s numeric order (Maybe=0, Not=1, Partial=2) differed from the documented severity order, so `s >= MaybeExecuted` was always true | The doc says to compare with `==` or a switch and that the numeric order is not the severity; the values keep MaybeExecuted as the safe zero. |
| `ErrNoRows` reads as database/sql's "query found nothing" | Renamed `ErrNoCurrentRow` ("gocql: no current row"); integration helpers that used it for "no row found" have their own sentinel. |
| `QueryError` had no exported fields | Unexported; the engine builds it through a `link` constructor slot. Its error and status stay reachable through `errors.Is`/`As` and `ExecutionStatusOf`. |
| `CDCWriteFailureError` had no fields | Removed; the code decodes as `OtherServerError` with `CodeCDCWriteFailure`. |
| `AttemptFailure.Status` | Renamed `ExecutionStatus`, as on `retry.Attempt`. |
| No way to build an `ExecutionError` in a test | Added `NewExecutionError(err, status, attempts...)`. |
| `retry.Safe` stopped on an idempotent logged batch's BATCH_LOG write timeout | It retries it in place (`Same`); a non-idempotent one still stops. |
| Sentinel messages: `ErrNoKeyspace` "no keyspace provided" also meant not found; "from cassandra"; missing `gocql:` prefixes | `ErrNoKeyspace`/`ErrNoTable` say "… not found or not set"; `ErrTimeoutNoResponse` says "no response from server within timeout"; `ErrReadHeaderTimeout`, `ErrFrameTooBig`, `ErrScyllaSourcePortAlreadyInUse` and `ConnectError` start with `gocql:`. `Snapshot.Token`/`Replicas`/`TokenRanges` on a keyspace or table the snapshot does not hold return a `cql.ValidationError` saying it is not loaded, wrapping `ErrNoKeyspace`/`ErrNoTable`. |
| `SpeculationPolicy.Delay(attempt)` named its argument as if it were an attempt number, and the eligibility rule was stated only in the guide | The parameter is `started`; `SpeculationPolicy` and `WithSpeculation` state that only idempotent executions of ordinary statements speculate. |

### config / endpoint / auth

| Finding | Outcome |
|---|---|
| nil meant "default" for some setters and "off" for others: `WithReconnect(nil)` disabled reconnection, `WithRetry(nil)` retries, `WithRouting(nil)` switched to plan-less pool selection | A nil provider or policy restores the driver's default in every setter: `WithReconnect(nil)` the jittered backoff, `ProfileBuilder.WithRetry(nil)` `retry.Safe{MaxAttempts: 3}`, `WithRouting(nil)` the default policy; a nil `Profile.Retry`/`Routing` converts to the same defaults. "Off" is explicit: `retry.Never{}`, or a reconnect policy whose `Delay` returns false (doc example). `WithTimestampGenerator(nil)` stays "server timestamps", documented as the one exception. |
| `WithPort` also applies to discovered peers, so a contact point on :19042 made the driver dial peers on :9042, and `Build` accepted `"h:abc"` | `WithPort` documents that it is the port of discovered nodes whose metadata reports none. `Build` parses the contact points (host or host:port, port 1-65535) and rejects contact points that all name one port other than `WithPort`, unless discovery is off or client routes are set. The legacy `learnPortFromHosts` is deleted: the session always sets the port. |
| Build errors named an internal group ("invalid pool limits") rather than the setter to fix | `cql.ValidationError.Field` names the `ConfigBuilder`/`ProfileBuilder` setter (`WithMaxRequestsPerConnection`, `WithPageSize`, ...) and the message gives the limit and the value (`must be 1 to 32768, got 0`). Build-time `UnsupportedSettingError.Setting` names the setter too. An unbuilt `Profile` literal is reported as `Profile`. |
| `WithSocketBuffers(r, 0)` shrank the send buffer to the kernel minimum | Split into `WithReceiveBuffer(bytes)` and `WithSendBuffer(bytes)`; zero keeps the OS's size. |
| Advanced shard awareness had three methods | `WithAdvancedShardAwareness(on bool)` and `WithShardAwareSourcePorts(first, last)`; the ports setter does not turn awareness on, and Build rejects a range that is empty or starts below 1024. |
| Client routes needed their own builder with its own Build and error | `WithClientRoutes(routes ...ClientRoute)`; none removes them. `ClientRoutes`, `ClientRoutesBuilder` and `NewClientRoutes` are removed; `Build` rejects an empty or repeated connection ID. |
| `DefaultDialer.ConnectTimeout`/`TCPKeepalive` had no effect, but `WithDialer`'s doc told users to set them | `endpoint.DefaultDialer` is `struct{}`: its dial is bounded by the context, which carries the connect timeout, and the session sets keepalive and the socket options afterwards. The `WithDialer` doc says so. |
| The reconnect `Delay(1)` probe came back in fce465b7 and was called twice (Connect's validation and the reconnection loop), contradicting the fifth-pass row that said the probe was gone | `Build` calls `Delay(1)` once, rejects a negative delay with a `WithReconnect` validation error, and the session's reconnection loop uses that answer. The fifth-pass row is superseded: the probe stays, once, at Build. |
| `SimpleConvictionPolicy` had pointer receivers | Value receivers, so `endpoint.SimpleConvictionPolicy{}` is a policy. |
| `auth.Provider.Start` had unnamed parameters | Named `ctx`, `host`, `authenticator`. |

### cql struct mapping and codecs

| Finding | Outcome |
|---|---|
| The default column name was `strings.ToLower` (`CreatedAt` → `createdat`), unlike gocqlx | An untagged field maps its name in snake_case, as gocqlx's `CamelToSnakeASCII` does (`CreatedAt` → `created_at`, `UserID` → `user_id`, `HTTPServer` → `http_server`; `UserIDs` → `user_i_ds`, so tag it). cqlgen and reflection share `structmap.SnakeCase`; the migration guide says so. |
| `time.Time` fields had no codec | Added `cql.TimestampTimeCodec` (UTC on decode, the millisecond instant on encode, an error outside the int64 range), the default for `time.Time`, `Nullable[time.Time]`, `[]time.Time` (list, set, vector) and `map[K]time.Time` values. Not a map key (`==` compares location and monotonic reading), and `StreamRowOf` has no `time.Time` decoder: streaming keeps `cql.Timestamp`. |
| `BindValue[T]` fields failed in `ParamsOf`, and the suggested `WithCodec(BindEncoder(...))` did not compile | `BindValue[V]` is a built-in parameter field for every built-in V and for a V `WithCodec` or new `WithEncoder[V](Encoder[V])` maps. It binds parameters only: `RowOf` rejects it, and cqlgen generates `Params` with nil `Columns` for such a struct. |
| UDT and tuple fields needed a hand-written five-method Codec, and `UDTOptions` required `Keyspace` | Added `UDTCodecOf[T](UDTOptions, ...StructOption)`/`MustUDTCodecOf`, which maps the UDT's fields to T as `RowOf` maps columns (generated `CQLFields` included), and `TupleCodec(enc ParamEncoder[T], dec RowDecoder[T]) Codec[T]`. An empty `UDTOptions.Keyspace` matches the UDT in any keyspace; `Name` stays required. |
| `WithCodec[P, V]`, `ReuseBuffers[P]` and `WithStreamCodec[P, V]` could not infer P, and an option passed to the wrong constructor was silently ignored | `StructOption` is not generic: `WithCodec[V]`, `WithEncoder[V]`, `WithStreamCodec[V]`, `ReuseBuffers()`, `IgnoreExtraColumns()`. Options build their field accessors for a stand-in struct and are bound to P by the constructor; a parameter field of an option reads P through a pooled copy, so binding stays allocation-free. A constructor rejects an option it does not apply with a `ValidationError` whose `Field` is the option's name. |
| `SELECT *` broke after `ALTER TABLE ADD` because RowOf is strict, while `StreamRowInto` silently skipped unmapped columns | Added `IgnoreExtraColumns()` for `RowOf` and `StreamRowOf`. `StreamRowInto` is exact, like `RowInto`: every column needs a field and every field a column; the new `StreamIgnoreColumn[R](name)` skips one, as `IgnoreColumn` does for `RowInto`. |
| A nil `StreamRowInto` reset was rejected, and an absent field needed the optional `StreamResetter` | A nil reset is valid. `StreamResetter` is folded into `StreamDecoder` as a required `Reset(*T)`; the "needs StreamResetter" compile error is gone. A custom `StreamRowDecoder` used in `StreamTuple`/`StreamUDT` resets through its `Reset(*R)` method when it has one, or is zeroed. |
| `CompiledRow` required `Decode` and `Clone` beside `DecodeInto` and `RowDecoder.Clone`, and custom-codecs.md said a plan clears the destination while `DecodeInto`'s doc said the driver does | `CompiledRow` is `DecodeInto` only. The driver detaches a custom plan's rows with the `Clone` of the `RowDecoder` that compiled it (`Page.Clone`, `Each`/`One`, pairs and records keep the decoder). custom-codecs.md says the driver clears the destination before a custom plan's `DecodeInto`. |
| Docs named `valueCodec` and `convert`, which are not the parameters; `StreamStatement` fields were ordered unlike `Statement` | `Param`/`Column` docs say "with codec", `ConvertParams`/`ConvertRows` name `mapValue`. `StreamStatement` keeps its order (`Params`, `Rows`, `CQL`), which fieldalignment requires; its doc says the order is for memory layout, and the spec lists the fields in that order, with `Idempotent`. |

### cql value types

| Finding | Outcome |
|---|---|
| NULL into a non-nullable destination gave an error nothing could match (v1 gave the zero value) | Added `cql.ErrNull`. Buffered and row decoders keep their message and match it; a streaming decoder and a `Value` accessor on a NULL wrap it too. The migration guide and errors.md say to use `Nullable[T]`. |
| `Date.Time()`/`Timestamp.Time()` returned an always-nil error | They return `time.Time`. |
| `int64` could not map a counter column, which forced `cql.Counter` | `Int64Codec` and `StreamInt64` accept counter, and `BigIntValue` binds a counter parameter (a decoded counter `Value` keeps its type). `Counter`, `CounterCodec`, `StreamCounter` and `CounterValue` are removed. |
| `TextValue` and the other checked scalar constructors returned errors, so a parameter list could not be written inline | `TextValue`, `ASCIIValue`, `InetValue`, `TimeUUIDValue`, `TimeValue` and `DurationValue` return `Value`; an invalid payload fails when it is bound (the codecs already checked it), and a text one also when `Text` reads it. |
| No `String()` on `Value`, `Varint`, `Decimal`, `Timestamp` or `Date` | Added on those and on `TimeOfDay` and `Duration`: `Value` prints a CQL literal (`'a'`, `[1, 2]`, `{k: 1}`, `null`), `Decimal` as Java's `BigDecimal.toString`, `Duration` in CQL units (`1y2mo3d4h`), `Timestamp` in RFC 3339 with milliseconds. A released view prints `<released>` instead of panicking. |
| `ValueIterator` was Next/Value/Err only, with no `Field(name)`/`At(i)` | Added `ValueIterator.All() iter.Seq2[T, error]` and `Value.Len`, `At(i)` (list, set, vector, tuple or UDT member) and `Field(name)` (UDT). |
| An empty cell had two shapes for a decoder (Present with no body, or `EmptyCell`) | `DecodeValue` passes an `EmptyCell` value as Present with an empty body, as a response holds it; `ValueReader.State` is Present or Null. A dynamic `Value` still reports `EmptyCell`. |
| No time accessors on `Value` | Added `Value.Time` (timestamp, or a date at midnight UTC), `TimeOfDay` and `TimeUUID`. |
| A vector's `Kind()` was `TypeCustom`, the kind of the invalid zero `DataType` | Added the driver-defined `TypeVector` (0x0100, not a protocol ID), which a vector's `Kind()` returns. |
| `UUID.IsEmpty`; no constructor for a present `Nullable` | Renamed `IsZero` on `UUID` and `TimeUUID`; added `cql.NonNull(v)`. |
| The `Text` doc said `String` panics on a released view; the `Option` doc covered only inherited settings | `Text` says `String` returns `"<released>"`; `Option` covers both settings and result values that may be unknown, and points at `Nullable` for NULL. |

### metadata / event

| Finding | Outcome |
|---|---|
| `HostID` was missing from REMOVED_NODE events, and the `Event.HostID` doc named Control events only | A node event takes the ID from the snapshot before the ring refresh when the current one has dropped the node; the doc covers Topology, Status and Control events. |
| No `Snapshot.Keyspaces()`; `Keyspace()` deep-copies a whole keyspace to read one table; no host lookup while `Hosts()` copies every token | Added `Snapshot.Keyspaces` (sorted), `Table(ks, name)` and `Host(id)`; the `Snapshot` doc says every returned value is a private copy. |
| `Functions`/`Aggregates` keys changed from `name` to `name(types)` once an overload existed | Always keyed by signature (`"add(int,int)"`, `"now()"`); added `Keyspace.Function(name, args...)`, `FunctionsNamed`, `Aggregate` and `AggregatesNamed`. |
| `Keyspace.Clone` was redundant: snapshot results are already private copies | Removed. |
| `ColumnKind` and `event.Kind` constants lacked the type prefix | `ColumnKindPartitionKey` ... `ColumnKindStatic`; `event.KindUnknown` ... `event.KindOverflow`. |
| `HostSnapshot.ID`; `event.Target` clashed with `metadata.Target` | `HostSnapshot.HostID`; `event.ObjectKind` with `ObjectKeyspace` ... `ObjectAggregate`, in the field `Event.ObjectKind`. |
| `RefreshMetadata` doc said topology only; no `Subscribe` example; stale `WithShard` in the `Route` doc | `RefreshMetadata` documents that it reloads topology, client routes and schema, and its error shapes; added `ExampleSession_Subscribe` (Generation/Overflow loop, run by `TestClusterExamples`); the doc names `WithShardID`. |

### observe / metrics

| Finding | Outcome |
|---|---|
| `LifecycleObserver` overlapped `Subscribe`; its Ready and Closed events carried no information | Removed `LifecycleObserver`, `LifecycleEvent`, `LifecycleKind`, `LifecycleStage` and `ConfigBuilder.WithLifecycleObserver`. A pool connecting or a host marked down is an `event.KindPool` event with `ChangeConnected` or `ChangeDown`, delivered after the metadata publication that shows it. |
| No `observe.Join`; `exec.Options.Observer` replaced the session observer with no way to add to it | Added `observe.Join`: it implements the union of its observers' hooks, chains `StartRequest` contexts, and keeps one observer's panic from skipping the others. The `exec.Options.Observer` doc shows `observe.Join(sessionObserver, extra)`. |
| No function adapters for the observer hooks | Added `observe.RequestObserverFunc` and `AttemptObserverFunc`, with `ExampleJoin`. |
| `metrics.Snapshot.Requests` could decrease while an admission was refused; `Attempts` was described as requests | `Requests` is counted once the execution is registered, so it never counts a refused admission and never decreases; the doc says attempts. |
| `observe` doc named `config.Build` and a per-host lifecycle; `Routing` did not say what an unknown field holds | Names `ConfigBuilder.Build`, points at `Subscribe` for pool and cluster changes, and says an unknown `Routing` field is empty or unset. |
| `go vet -tags gocql_perf` failed after the row plan change | The perf tests decode through `DecodeInto` (separate commit). |

## Seventh review pass (2026-11-25)

A seventh pass, with one reviewer per area, made about 80 proposals; all
are taken and land in six changes before `v2.0.0-rc.1`. This first part
fixes silent run-time failures, with little or no API change; the second
reworks the `cql` types and codecs; the third the `cql` statements, results
and paging; the fourth the root execution API and `exec`; the fifth `config`,
`auth` and `endpoint`; the sixth errors, observability, metadata and routing
extras, and a sweep of the docs. Rows marked
**reverses** undo a decision of an earlier pass.

### transport / config

| Finding | Outcome |
|---|---|
| A dialer's connection whose `LocalAddr` is a `*net.TCPAddr` but that lacks `SetNoDelay` or `SetKeepAlive` failed to open, though the user chose neither option | The defaults (TCP_NODELAY on, keepalive off) are applied best-effort; only an option a setter chose (`WithNagle`, `WithTCPKeepalive`, buffers, linger) fails with an `UnsupportedSettingError`. Controls are also found through an `Unwrap() net.Conn` chain. |
| `WithShardAwareSourcePorts` was accepted where advanced shard awareness is off and on Cassandra | `Build` rejects it under `WithConnectionsPerHost`, `WithAdvancedShardAwareness(false)` and `WithClientRoutes`, and with a Cassandra dialect as an `UnsupportedSettingError`. |
| `WithMetadataKeyspaces` under `metadata.ModeNone` did nothing | `Build` rejects it. |
| Zero for `WithConnectTimeout`, `WithDNSTimeout` or `WithMetadataTimeout` removed the bound | `Build` rejects zero. **Reverses** zero meaning "no limit" for these three; zero still turns `WithMetadataRefresh` and `WithMetadataServerTimeout` off. |
| `WithMetadataServerTimeout` could be at or above the client timeout, and its 30s default ignored a shorter `WithMetadataTimeout` | `Build` rejects a chosen server timeout at or above `WithMetadataTimeout`; the default is one second below it (half of it at 2s or less). |
| A session keyspace left out of `WithMetadataKeyspaces` lost replica routing silently | Connect logs a warning. |

### routing / metadata

| Finding | Outcome |
|---|---|
| `NewPolicy` and `SingleTarget` returned a `routing.Policy`, so a policy embedding the result lost `Validate` and `Observe` unless it forwarded them | Both return `*routing.Builtin`, which has `Pick`, `Fallback`, `Validate` and `Observe`; the zero `Builtin` is the default policy. The routingtest example and load-balancing.md embed it without forwarding. |
| `ReplicaRoutes` routed vnode replicas to any shard, and `Route.ShardOf` was unset until the host's pool connected | Vnode replica routes carry the token's shard from `HostSnapshot.Shards`/`ShardingIgnoreMSB`, as the built-in plan does; `ShardOf` falls back to the snapshot's sharding. |
| Under `metadata.ModeKeyspaces` no table is loaded, so routing found no replicas | A vnode keyspace's tables route by the keyspace's ring when the table is not loaded; tablet keyspaces still need the table. `metadata.Mode` documents what each mode costs routing. **Reverses** the routing cache test's "a table missing from the snapshot has no replicas". |

### event

| Finding | Outcome |
|---|---|
| An overflow closed the subscription, so every subscriber had to resubscribe | The overflow discards the queue and queues one `KindOverflow` event with the highest discarded generation; the subscription stays open. `ExampleSession_Subscribe` drops its resubscribe branch. **Reverses** the API spec's terminal Overflow, which closed the subscription after the marker. |
| A dropped tablet routing hint was delivered as an Overflow event (`TABLET_RESYNC`) though no metadata changed | Not delivered; `event.ChangeTabletResync` is removed. |
| Whether the snapshot at `Event.Generation` holds the change was not stated | `Event.Generation`, `Subscription` and `Session.Subscribe` say an event is delivered after the publication that reflects it. |

### metrics / errors

| Finding | Outcome |
|---|---|
| `Snapshot.Canceled` and `AttemptEvent.Canceled` counted the driver's own timeouts, which match `context.DeadlineExceeded` | `Canceled` counts `KindCanceled` only; added `Snapshot.Timeouts` (`KindTimeout`), `Snapshot.ErrorsByKind` and `AttemptEvent.TimedOut`. |
| `ExecutionStatusOf(nil)` returned `NotExecuted` | Added `cqlerr.Executed`, which it returns. |
| A logged batch that failed after its batch log was written was `PartiallyExecuted`, though the server replays it whole | `MaybeExecuted`. `PartiallyExecuted` says to treat it as executed for work that is not idempotent. |

### exec

| Finding | Outcome |
|---|---|
| `exec.Options.Timestamp` was microseconds in an `int64`, and a worker query stamped all its writes with it | `Timestamp` is a `cql.Option[time.Time]`; `Prepared.Query` and `PreparedStream.Query` reject it with a `ValidationError`. |
| `exec.Options.Keyspace` and `PrepareOptions.Keyspace` on protocol 4 failed only per attempt, or did nothing for a qualified statement | `CompileOptions`, admission and `Prepare` reject a keyspace other than the session's on ScyllaV4 and CassandraV4 with an `UnsupportedSettingError`; both fields document it. |

### cql types and codecs

| Finding | Outcome |
|---|---|
| Text and varchar were two kinds, so `DataType.Equal` told a text column from a varchar one and every text path checked both | One kind, `KindText`, with varchar's ID 0x000D; the protocol's text ID 0x000A and the schema names `text` and `varchar` decode to it, and `String` prints `text`. `TypeVarchar` and `VarcharType` are removed. |
| `ASCIIStringCodec`, `ASCIICodec` and `StreamASCII` differed from the text codecs only in the kind they accepted | `StringCodec`, `TextCodec` and `StreamText` take text and ascii and validate UTF-8 or ASCII for the column, both ways; the three are removed, and the `codec=ASCIIStringCodec` and `stream=ASCII` tags stay as aliases. |
| `UUIDCodec` rejected a timeuuid column, so a `cql.UUID` field could not map one | `UUIDCodec` and `StreamUUID` read timeuuid; encoding into one checks for version 1. **Reverses** the codec-per-kind split between `UUID` and `TimeUUID` columns; the two Go types stay distinct. |
| A `[]V` field mapped only a list unless tagged | It maps whichever of list, set or vector the column is, through the new `SliceCodec` and `StreamSlice`; `codec=ListCodec`, `SetCodec` or `VectorCodec` requires one. |
| A named type such as `type Status string` needed its own codec, and `int`, `*V`, `cql.Text` and `*big.Int` fields had none | A named type maps like its underlying type when that has a built-in codec, reading the field as that type, and so do slices, pointers, `Nullable`, `BindValue` and map keys and values of it (`[]Status` like `[]string`); cqlgen converts in the accessors, through `unsafe.Pointer` where Go has no conversion, as for `[]Status` to `[]string`. Added `IntCodec` (bigint; decode checks the range), `PointerCodec` (nil is NULL) and `VarintBigIntCodec`; `cql.Text` maps with `TextCodec`. `int` maps bigint on every platform, not `int` on 32-bit ones, so a struct maps one column type everywhere; it maps as a list, set or vector element and as a map key or value like the other integer types. **Reverses** "a field maps by its exact Go type". |
| `WithCodec` of a UDT codec did not reach `[]V` or map values of it | `WithCodec[V]` also maps `[]V`, `*V` and `map[K]V` for the built-in key types; `WithEncoder` maps `[]V`. |
| `UDTOptions.Name` was required though metadata carries it, and `UDTCodecOf` rejected `IgnoreExtraColumns` | An empty `Name` matches any UDT, so the zero options match by fields; `UDTCodecOf` takes `IgnoreExtraColumns` as `AllowExtraDatabaseFields`. **Reverses** the required UDT name. |
| A tuple mapped to a struct only through `TupleCodec` with hand-written fields | Added `TupleCodecOf[T]`/`MustTupleCodecOf`, which map members to fields by position, and `PairCodec` for a two-member tuple. |
| `BlobCodec`, `TimeCodec`, `NamedScalar` and `WithStreamCodec` named something other than what they map | `BytesCodec`, `TimeOfDayCodec`, `ConvertCodec` and `WithStreamDecoder`. The custom codec example converts through `ConvertCodec`. |
| `ListValue`, `SetValue` and `VectorValue` needed the collection type first; `Elements`, `Entries` and `Fields` returned an iterator and an error; `At(i)` rescanned a decoded value per call | The constructors take the element type and variadic values, a vector's dimension being their count. The three return `iter.Seq2[..., error]`; `ValueIterator` and `Value.At` are removed. Added `Value.Str`, an owned string; the examples copy text with `Copy` or `Str` instead of `Text.String`. |
| Reading a small varint or a decimal from text took `big.Int` arithmetic | Added `VarintFromInt64`, `Varint.Int64`, `ParseDecimal` and `Decimal.Rat`. |
| `EncodeValueBorrowed` exposed a zero-copy mode only routing needs; `NewEncodedValue` took a `Bytes`; `Bytes.OwnedBytes` was `Copy` with a panic | `EncodeValueBorrowed` is internal, through `internal/link`; `NewEncodedValue` takes a `[]byte`, which it borrows; `OwnedBytes` is removed. |
| A nested type took one constructor and error check per level | Added `ParseDataType` and `MustParseDataType` for scalars, collections, tuples, vectors and `frozen`; a UDT needs its fields, so they reject one. |
| `cql.Type` named a kind next to `DataType`, so `DataType.Kind` returned a `Type` | `cql.Kind` with `KindText`, `KindInt`, ...; the last change of this part, being mechanical. |

### cql statements, results and paging

| Finding | Outcome |
|---|---|
| A statement left `UnknownStatement` ran as possibly conditional unless its `R` was `EmptyRow`, so every ported SELECT, and on Cassandra every unclassified statement that returned rows, lost client timestamps, replica shuffling and speculation; one declared ordinary but written with `IF` ran with all three | The driver resolves `UnknownStatement` when a statement is prepared or, unprepared, bound: a `CASRows` decoder, or an INSERT, UPDATE, DELETE or BATCH whose CQL has an `IF` outside string literals, quoted identifiers and comments, is conditional; anything else, a SELECT included, is ordinary. A statement declared `OrdinaryStatement` that this finds conditional is rejected with a `ValidationError` before anything is sent, and so is a statement that can never be conditional, one whose first word is not INSERT, UPDATE, DELETE or BEGIN (a SELECT, DDL), declared `ConditionalStatement` or decoded with `CASRows`. Prepare still fails on PREPARE metadata that lacks the leading boolean `[applied]` column a `CASRows` decoder needs, or on a ScyllaDB that reports the statement is not a lightweight transaction; only the existing row's columns, and metadata Cassandra omits, wait for EXECUTE. A batch member resolves the same way, so an `IF` member makes the batch conditional. **Reverses** "the driver never parses CQL to infer the kind" (`StatementKind`) and the sixth pass's "as a batch member it stays unknown": a lexical scan for `IF` in a DML statement is exact, since `IF` is a reserved word, and costs one pass over the text per preparation, or per unprepared call, while leaving reads unclassified cost them their routing and speculation on Cassandra. |
| `PageState.UnmarshalText` and `UnmarshalBinary` rejected empty input, so a request without a cursor needed its own branch | Empty input decodes to the start state, and the start state encodes as empty; `"AA"` and the zero byte, its earlier encodings, still decode to it. The paging example decodes every cursor. |
| `ParseConsistency` ignored case but `Consistency.UnmarshalText` did not, so the same text worked in one and failed through JSON or a flag | `UnmarshalText` ignores case; `ParseConsistency` reads through it and moves to `consistency.go`. |
| `Result.Rows` read like the rows themselves and `Result.Latency` like the request's wall time | `Result.FetchedRows`, also on `StreamInfo.Result`; `Result.AttemptLatency`, documented as the sum over completed attempts, speculative ones overlapping. |
| A `Statement` literal repeated `P` and `R` that the codecs fix; the driver filled a nil `Params` or `Rows` from the exported, reassignable `EmptyParamsEncoder` and `EmptyRowDecoder` | Added `NewStatement`, `Select` (ordinary and idempotent) and `Write` (`R` is `EmptyRow`), which infer their type arguments, and `NoParams` for a statement without markers. The driver fills nil codecs with its own values; `EmptyParamsEncoder` and `EmptyRowDecoder` are removed. |
| `CASRows(nil)` failed to compile whenever the server returned the existing row, so reading only `[applied]` took `CASRows(Row[EmptyRow]())` | A nil decoder skips the existing row's columns; `CASApplied` is `CASRows[EmptyRow](nil)`. |
| `Columns` had only `Len` and `At`, so finding a column by name took an index loop | Added `Columns.All`, an `iter.Seq2` of index and column, and `Columns.Index`. |

### gocql

| Finding | Outcome |
|---|---|
| `Prepared.Parameters` and `PreparedStream.Parameters` named what `Statement.BindMarkers` supplies for an unprepared call | Renamed `BindMarkers`. |

### gocql execution API

| Finding | Outcome |
|---|---|
| `Prepared.Query` returned a `*Query`, a name that reads like CQL text or v1's mutable query builder rather than the reusable execution one goroutine owns | `Prepared.Worker` returns a `*Worker`, `PreparedStream.Worker` a `*StreamWorker`; the grid's worker-owned row, the docs and the examples follow, and `ErrBusy` names the worker. **Reverses** the first pass's "keep `Query`, `StreamQuery`": the type was named for the request, while what a caller manages is a per-goroutine execution with buffers to close. The rename is the first change of this part, so the later ones use the new names. |
| Reading a value's route took a `Worker` to create and close, and `Routing` left out the consistency and host and shard pins its doc promised | `Prepared.Routing(value, options...)`, with the shared form's options, encodes into pooled scratch and fills in the consistency, the pins, the session keyspace for a statement that names none and conditional routing for a SERIAL read. `Worker.Routing` is removed. |
| A conditional batch's applied flag took `RunConditionalBatch`, `Applied` and `Close` | `Session.ApplyBatch(ctx, batch, options...) (applied bool, cql.Result, error)` runs it with `cql.CASApplied` and closes the rows before returning, so it keeps no pooled query. The grid gains a Batch row (`ExecBatch`, `ApplyBatch`, `RunConditionalBatch`, pages empty by design), whose verbs `TestExecutionGrid` checks with the `Batch` suffix; `ErrConditionalExec` names `ApplyBatch`. |
| `RowStream` had no range form, though `Rows`, `Page` and `CASBatchRows` do | `RowStream.All(dst)` decodes into the seeded `dst` and closes the stream when the loop ends, so `break` releases the execution and any reserved connection. |
| A statement always read at one consistency passed the same compiled options on every call | `Prepared.With(defaults)` returns a view that shares the preparation and its pool; its calls without options use `defaults`, options passed to a call replace them whole, as they replace the session's, and its `Worker` takes their values, a `Timestamp` among them rejected. |
| Most throwaway calls were `UnpreparedExec(ctx, cql.Command(text), cql.EmptyParams{})` | `Session.UnpreparedCommand(ctx, text, options...)`, the grid's first documented exception: a new Exceptions section of `docs/api-surface.md` lists it, and `TestExecutionGrid` checks each exception is documented there and ends in its row's options form. The examples use it. |
| `Rows`, `Page` and `CASBatchRows` returned their `cql.Result` from `Info`, the name `Prepared.Info` uses for preparation data; `RowStream.Info` returned a `StreamInfo` whose `DeliveredRows` repeated `Result.FetchedRows` | `Result()` on all four. What was left of `StreamInfo`, `Pages` and `OutputStarted`, is a `StreamProgress` from `RowStream.Progress()`: progress counters, not the information about the stream that `Info` named, and no longer holding the result `Result()` returns. |
| `Session.ID` returned the UUID as a string, and a zero `Session`'s as `""` | Returns a `cql.UUID`, as `HostID` and the trace IDs are, and the zero UUID for a zero `Session`. |
| `One`'s `(found bool, cql.Result, error)` was proposed to change with the other accessors | Kept: `found` is the one-row answer, as `ApplyBatch`'s `applied` is, and the `Result` is returned beside it on every one-shot call. |

### exec observers

| Finding | Outcome |
|---|---|
| `exec.Options.Observer` replaced the session's observer, so adding a span or counter to one execution dropped the session's tracer and metrics unless the caller joined them by hand | The per-call observer is joined after the session's, as `observe.Join` orders them: each hook calls the session's first, and the added one's `StartRequest` gets the session's context. `Options.NoSessionObserver` leaves the session's out, and `Session.Observer` returns it. **Reverses** the sixth pass's "replace, and show `observe.Join(sessionObserver, extra)`": joining was what every caller wanted, and replacing needed the session's observer at each call site; `NoSessionObserver` keeps replacement for the rare caller that wants it. |

### config

| Finding | Outcome |
|---|---|
| Contact points that all named one port, such as `"10.0.0.1:19042"`, failed `Build` unless `WithPort` repeated the port, and the streaming example parsed its own flag to call `WithPort` | Without `WithPort`, contact points that all name one TCP port make it the default, which is also the port of discovered nodes, and `Config.String` shows it. A chosen `WithPort` is still checked against them, and mixed or partly named ports keep 9042. **Reverses** the sixth pass, which deleted v1's `learnPortFromHosts` and had `Build` reject such contact points without `WithPort`: rejecting was safe but made every caller say the port twice, while inferring it from a list that agrees cannot dial the wrong port, which was the reason for rejecting, and is what v1 did. |
| `WithLocalDatacenter(dc, rack)` made the common datacenter-only call pass an empty rack, and a rack read like a second datacenter | `WithLocalDatacenter(dc)` and `WithLocalRack(rack)`; `Build` rejects a rack without a datacenter, naming `WithLocalRack`. `Config.String` prints both. **Reverses** the sixth pass's one setter for the locality: the pair kept the rack next to its datacenter, but most callers have no rack, and two one-argument setters read unambiguously at the call site. |
| A built `Config` could not say which default profile or observer it held, and `ConfigureDefaultProfile` was a second, callback-based way to set the profile whose nil meant reset | Added `Config.DefaultProfile`, `Config.Observer`, which agrees with `Session.Observer`, and `Profile.Builder`, which starts a builder from a profile's values. `ConfigureDefaultProfile` is removed: `WithDefaultProfile(cfg.DefaultProfile().Builder()....MustBuild())` derives a profile, as the integration fixtures now do. |
| A dialect or compression read from a flag or a file took a hand-written switch | Added `config.ParseDialect` and `cql.ParseCompression`, which read the `String` names in any case, and `MarshalText`/`UnmarshalText` on both types, so they work with `flag.TextVar` and JSON; the streaming example uses them. Marshaling a value `String` does not name fails. |
| A one-line reconnect backoff needed a named type; a profile from constants needed an error check; a base builder could not be specialized twice | Added `endpoint.ReconnectFunc`, `ProfileBuilder.MustBuild` and `ConfigBuilder.Clone`, which copies the builder's slices, maps and profile. |
| Idempotence was per statement or per call, with nothing for v1's `DefaultIdempotence` | `ProfileBuilder.WithIdempotent` and `Profile.Idempotent`: an execution is idempotent when its statement or its profile is; `exec.Options.Idempotent` overrides both. The driver configuration report shows it, and the migration guide maps `DefaultIdempotence` to it. |
| `WithAutomaticSchemaAgreement` and `WithRefreshAfterAgreement` were two bools for three behaviours: the refresh did nothing without automatic agreement | `WithSchemaChangeWait(SchemaChangeWait)` with `SchemaChangeWaitNone`, `SchemaChangeWaitAgreement` and `SchemaChangeWaitRefresh`, the default as before; `Build` rejects another value. |
| Loading a CA, certificate and key took a dozen lines of `x509` and `tls` calls, though `NewTLSFileSource` had the loader | Added `config.LoadTLSFiles(base, TLSFiles)` for `WithTLS`, with `NewTLSFileSource`'s checks. The streaming example uses it; the migration guide maps v1's `SslOptions` paths and says that v1 skipped verification by default while v2 verifies the node's IP address unless the config names a server. |
| `UnsupportedSettingError.Setting` named the setter at `Build` but an internal notion ("Unix TLS", "dial source", "TCP_NODELAY", "compression", "schema metadata", "prefetch") once connected | It names the `ConfigBuilder` setter everywhere (`WithTLS`, `WithDialer`, `WithNagle`, `WithCompression`, `WithMetadataMode`, `New` for the dialect) or the `exec.Options` field (`Prefetch`, `Keyspace`); the field documents it. The one left is an internal guard on the transport's `Conn.Read`, which no public API reaches. |

### auth

| Finding | Outcome |
|---|---|
| `auth.Password` sent the credentials to any server that asked, and v1's `AllowedAuthenticators` had no counterpart | `auth.Password(user, password, allowed ...string)`: a server naming a class not in `allowed` fails the connection before the credentials are sent; none allowed accepts any class, as in v1. |

### endpoint

| Finding | Outcome |
|---|---|
| `ConvictionSource`, `ConvictionPolicy` and `Reset` docs said "with Shard set" for the `ShardID` field | They say `ShardID`. |

### errors and retry

| Finding | Outcome |
|---|---|
| A retry loop combined `KindOf(err).Transient()`, `ExecutionStatusOf` and `ctx.Err()`, the last because `KindTimeout` also covers the caller's own deadline | Added `cqlerr.Retryable(err, idempotent)`, which needs no context: an error matching `context.Canceled`, or `context.DeadlineExceeded` without `ErrRequestTimeout`, is the caller's own end and never retryable; otherwise a transient error is retryable when not executed, or when idempotent. The errors guide and `Example_errors` loop on it. |
| The attempt number was `Attempt` on the observer events and `Number` on `AttemptFailure` and `retry.Attempt`; `AttemptFailure` named its host with `HostID` and `ShardID` where the events had a `Target`; `retry.Attempt.Kind` did not say what it classified | `Attempt` everywhere; `AttemptFailure.Target` (a `metadata.Target`) replaces `HostID` and `ShardID`; `retry.Attempt.StatementKind`. |
| `ErrNoConnections` read "no hosts available in the pool" though hosts may be available | "gocql: no connections available". |
| `ServerError` had no `Kind`, though every server error type did | `ServerError.Kind()`. |
| `ErrorMap` mapped replicas to a bare `uint16` | `cqlerr.FailureReason`, with the codes Cassandra and ScyllaDB share and a `String` that prints the server's name, or the number for a code it does not name, such as ScyllaDB's own. |
| A speculation policy needed a named type | Added `retry.SpeculationPolicyFunc`; the speculation guide uses it. |

### observe

| Finding | Outcome |
|---|---|
| `observe.Observer` was `any`: a value implementing neither hook, or a hook method with a typo or the wrong receiver, compiled and was rejected or ignored at run time, and a one-function observer needed `RequestObserverFunc` or `AttemptObserverFunc` | `observe.Hooks{Request, Attempt}`, a struct of hook functions whose zero value observes nothing; a nil field is skipped with its event, and a new hook is a new field. `Join` merges `Hooks` field by field, keeping a single function unwrapped. `WithObserver`, `exec.Options.Observer`, `Config.Observer` and `Session.Observer` take or return `Hooks`. `Observer`, `RequestObserver`, `AttemptObserver` and the two adapters are removed. **Reverses** the first pass's "keep `Observer`, `RequestObserver`, `AttemptObserver` (decided under extensibility)": a struct of function fields is as extensible, since a later hook is a field the zero value leaves unset, and the compiler checks it, where `any` deferred every mistake to `Build` or to silence. |
| An observer saw neither the address an attempt went to nor its consistency, which `AttemptFailure` carried, nor what a failed attempt may have executed | `AttemptStart` and `AttemptEvent` gain `Address` and `Consistency`; `AttemptEvent` gains `ExecutionStatus`, `Executed` for a success. |
| Package `observe` imported `routing` as `routing2` | Imports it as `routing`. |
| No example of exporting request metrics | `Example_prometheus` records latency by outcome and attempts by host through hooks shaped like the Prometheus client's, which the driver does not import. |

### routing

| Finding | Outcome |
|---|---|
| `routing.AttemptObserver` and its `Observe` shared the observer vocabulary of package `observe`, though a policy learns from attempts rather than instruments them | `routing.Learner` with `Learn(AttemptEvent)`; `Builtin.Learn`. |
| A policy's `Pick` allocated through `ReplicaRoutes`, routing to one host meant searching `HostRoutes`, dropping hosts from a built-in plan meant draining it, and a route's datacenter took a snapshot lookup | Added `AppendReplicaRoutes`, `RouteTo(snapshot, target)`, `FilterPlan`, which filters lazily, `Routes`, an `iter.Seq` over a plan, and `metadata.Route.Datacenter` and `Rack`, from the host's handle. The routingtest example uses `FilterPlan` and `Routes`. |

### routing/routingtest

| Finding | Outcome |
|---|---|
| A test host needed `Enabled`, `Up` and `Connected` spelled out, and routing by token needed a keyspace and table literal | Added `routingtest.Host(id, datacenter, rack)` and `Cluster.Tables`, which adds tables without schema and leaves `Keyspaces` unmodified. |

### metadata / exec

| Finding | Outcome |
|---|---|
| A shard named by `exec.Options.ShardID` was strict while one named by a `SingleTarget`'s `metadata.Target` was a hint unless `StrictShard` was set, and a pinned request could not allow the fallback | `metadata.Target.AnyShardFallback` replaces `StrictShard`, so the zero value is strict in both places, and `exec.Options` and `routing.Info` gain `AnyShardFallback`; it requires a shard ID. `exec.Options.HostID`, `SingleTarget` and the load-balancing guide state that an `exec.Options` pin takes precedence over a `SingleTarget` policy, its own `AnyShardFallback` deciding. **Reverses** `SingleTarget`'s shard as a hint by default: a pin's zero value meaning strict in one place and loose in the other invited the wrong one, and strict is the safe default, as a request that names a shard usually needs it. |
| `RefreshMetadata` returned only an error, so the caller read the result with a second call that could see a later snapshot | Returns `(*metadata.Snapshot, error)`: the snapshot once the refresh ended, also beside a `PartialRefreshError`. |
| A view or type took `Keyspace`, which copies the whole keyspace; `OrderedColumns` listed names, so walking columns in order took a map lookup per name | Added `Snapshot.View` and `Snapshot.Type`; `OrderedColumns` on `Table`, `View` and `Index` is a `[]*Column`. |

### cqltrace

| Finding | Outcome |
|---|---|
| Reading the trace sessions of a failed request took a loop over `ExecutionError.Result.CQLTraces` that stopped at the first one not yet written | `FetchAll` and `Fetcher.FetchAll` read every session, return one `Info` per reference and join the failures; `ExampleFetchAll` and the tracing guide trace a failed request. |

### gocql / config logging

| Finding | Outcome |
|---|---|
| Request-path log records, such as a panicking hook or a rejected tablet hint, carried no context, so a handler could not correlate them with the caller's trace | They are logged with the request's context, or the one its `Request` hook returned. |
| Which log attributes a handler may rely on was not stated | The package doc lists `logger`, `host`, `host_id`, `dc`, `shard`, `keyspace`, `table`, `stream`, `err` and `suppressed` as stable; messages and other attributes may change. |
| The 10-second collapse of repeated log records could not be changed | `ConfigBuilder.WithLogRateLimit(window)`: zero turns it off, a negative window fails `Build`, the default stays 10s. |

### metrics

| Finding | Outcome |
|---|---|
| A session's open connections could not be read | `Snapshot.Connections`, the connections of every host pool, the control connection not included. |
| The latency percentiles invited charting though they are a sample of ten seconds of successes | Their doc says they serve decisions that follow the current latency, such as a speculation delay, and that charts want a histogram fed from a `Request` hook, as `Example_prometheus` does; the tracing guide gains a Metrics section. |

### docs

| Finding | Outcome |
|---|---|
| The guides' Go snippets were checked only by reading, and some called renamed or removed symbols | `TestDocSnippets` builds every Go block of `docs/source` as a package, fragments in a function body and type and function declarations at package level, with the names a block assumes declared in an HTML comment above its fence, which the rendered page hides, and v1 code marked to be skipped. The batch guide's `Idempotent: true` is now `cql.Some(true)`. |
| The retry and speculation guides described attempt fields and statement classification as they were before this pass; `HostSnapshot.HostID` read "the host's host HostID"; the streaming example passed zero options explicitly | Corrected. |

### Follow-ups

Issues the phase reviews found outside their phase's items, fixed after the six parts merged.

| Finding | Outcome |
|---|---|
| `Rows.All`, `Page.All` and `CASBatchRows.All` closed the rows when the loop broke or ended, but not when its body panicked, so a recovered panic held the execution and any reserved connection until garbage collection | The close is deferred, as `RowStream.All` already did. |
| A TLS file that could not be read or parsed failed with a `cql.ValidationError` whose `Field` was "TLS files" | The field is the call that loaded the files: `LoadTLSFiles`, `NewTLSFileSource` for its first load, `TLSFileSource` for a reload. |
| `feature-specs/gocql-v2-api.md` still used names from before this pass (`Route.Shard`, `WithShard`, `routing.Route`, `routing.Target`, `routing.HostSnapshot`, `Snapshot.HostRoutes` and `ReplicaRoutes`, a `dynamic` package) | It uses the names of `testdata/api.txt`. |

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

## Eighth review pass (2026-11-26)

| Finding | Change |
|---|---|
| `PageState.Token` returned nil for both the start and completion, and `PageStateFromToken(nil)` made a continuation, so a cursor built from the obvious pair turned a finished query into an empty continuation. Since the sixth pass the `Marshal*` methods carry the whole state, so the pair was only a lossy duplicate. **Reverses** the first pass's keep of `PageStateFromToken`. | `PageState.Token` and `PageStateFromToken` are removed. `cql.ParsePageState(text string) (PageState, error)` decodes a `MarshalText` cursor in one call. |
| `event.ChangeDown` and `ChangeUpdated` meant different things under different kinds: a server DOWN or a pool marked down, a schema ALTER or changed client routes, so a switch on `Change` alone mixed them. `ObjectTable` covered views, so `Snapshot.Table` missed a view's event. **Reverses** the sixth pass's `ChangeConnected`. | Each `Change` belongs to one `Kind`: Pool events are `ChangePoolUp` and `ChangePoolDown`, ClientRoute events `ChangeRoutesUpdated` and `ChangeRoutesRefreshed`. `ObjectView` names a view's Schema event, from the event's snapshot or, for a drop, the one before. |
| `cqlerr.Retryable(err, idempotent)` made the caller restate an idempotence the driver had already resolved from `exec.Options`, the statement and the profile; a wrong bool silently allowed or blocked a replay. | `Retryable(err)` reads `ExecutionError.Idempotent`, which the driver sets on every request's failure. An error outside an `ExecutionError` that may have executed is not retryable. |
| `SpeculationPolicy.Delay(started int)` could not read the latency, deadline or table it is meant to decide on, and nothing let an application pass its own data to a policy. | `Delay(retry.Speculation)`: a struct the driver fills, with the attempts started, the time since the first, the deadline, keyspace, table, consistency and the session's latency percentiles, which later releases can extend. Its `Context` is the execution's context, carrying the caller's values; `retry.Attempt.Context` does the same for retry policies. `routing.Info` does not get one: plans are stored with the request and reused, so a context there would outlive its execution. |
| Overrides came in three shapes chosen by entry point: `exec.CompiledOptions` for prepared calls, plain `exec.Options` for `Prepared.Worker` and the `Unprepared*` calls, and `exec.StreamOptions` nesting a whole `Options` for streams. | `Session.CompileOptions` stays the only constructor, so the session chooses the compiled form, which a later protocol (CQL v5, v6) may change; a session-free `exec.Compile` would freeze one format for every session. Every execution entry point takes `...exec.CompiledOptions`: `Prepared.Worker`, `UnpreparedExec`, `UnpreparedOne`, `UnpreparedEach`, `UnpreparedAll` and `UnpreparedCommand` included. `PreparedStream.With` sets a stream's overrides, as `Prepared.With` does, and `exec.StreamOptions` keeps only `Limits` and `Incremental`. |
| Speculation could be chosen per statement or per call only by swapping the whole profile through `exec.Options.Profile`, which then had to repeat the default's consistency, timeouts and retry; `exec.Options` overrode `Retry` but not speculation. | `exec.Options.Speculation` replaces the profile's policy, nil keeps it, and `retry.NoSpeculation{}` disables it, as `Retry` and `retry.Never{}` do. `Prepared.With` speculates one statement; a policy can still decide per call from `Speculation.Context`. |
| One `tls.Config` named one `ServerName` for every node, which crypto/tls uses as both the SNI and the verified name, so SNI proxies, nodes dialed by IP with DNS-named certificates, and identity checks beyond a name each needed `InsecureSkipVerify` with hand-written verification. A cluster could not be reached while TLS was being enabled on it, and session tickets were shared by nodes with one SNI. | `WithTLSIdentity(func(endpoint.DialTarget) endpoint.TLSIdentity)` chooses each dial's `ServerName` (SNI), `VerifyName`, `ChainOnly` and `Verify func(tls.ConnectionState) error`, over the one shared configuration; a contact point's target has a zero host ID. `WithTLSPort` dials TLS on `native_transport_port_ssl`, and `WithTLSFallback(true)` falls back to plaintext for a node that does not speak TLS: never after a verification failure, never again for a node that has answered TLS, and not at all once every node has. The session cache is keyed by node. |

## Ninth review pass (2026-11-27): execution

| Finding | Change |
|---|---|
| Options passed to a call on a `Prepared.With` view replaced the view's defaults whole, so a view that speculated lost its speculation as soon as a call added `CQLTrace` or a timeout, and the call had to repeat every field of the view. **Reverses** the seventh pass's "options passed to a call replace them whole" for `Prepared.With`: the argument was symmetry with the session defaults, but the session's defaults are themselves overridden field by field, so whole replacement was the odd one out. | A call's options are laid over the view's defaults field by field, by the rule that lays options over a profile; the order, lowest first, is the session's default profile, `exec.Options.Profile` (the call's, else the view's), the view's other fields, then the call's. Observers join, the view's first, flags add up, and a `HostID` brings its `ShardID` and `AnyShardFallback`. The combination is compiled once and cached in a one-entry atomic pointer on the call's compiled options, keyed by the view's, so a warm call allocates nothing; a combination that does not compile (an unpaged view and a call's `Prefetch`) fails the call before sending. `With` on a view still replaces its defaults. |
| `Prepared.Worker(options...)` and `Prepared.With(o).Worker()` were two ways to give a worker options, with a rule for which won. **Reverses** the eighth pass's `...exec.CompiledOptions` on `Prepared.Worker`. | `Prepared.Worker()` takes no arguments and uses the view's defaults, or the session's: `p.With(o).Worker()`, as `PreparedStream.With(o).Worker()` already was. |
| A per-call profile could only start from `config.NewProfile()`, the driver's defaults, so a profile that changed only routing silently reset the session's consistency, timeouts and retry; `Config.DefaultProfile` needed the `Config` kept around. | `Session.DefaultProfile() config.Profile`; examples derive per-call profiles with `DefaultProfile().Builder()`, and the `NewProfile` doc says it starts from the driver's defaults. |
| `Profile.Idempotent`, meant as v1's `DefaultIdempotence`, also marked conditional statements, SERIAL reads and counter writes idempotent, so retries and speculation could apply an LWT or a counter increment twice. **Narrows** the seventh pass's profile idempotence. | The profile's flag no longer marks a conditional request (a SERIAL read included) or a counter write; the statement's own `Idempotent` and `exec.Options.Idempotent` still do. |
| Whether a conditional statement applied took a `cql.CASRows` decoder on the statement and `One`, while a batch had `ApplyBatch`; `One` with a nil `dst` reported a row that every conditional write returns, which reads like "applied". | `Prepared.Apply` and `Worker.Apply` return `(applied bool, cql.Result, error)`, reading `[applied]` through `cql.CASApplied` whatever `R` is, and reject a statement known not to be conditional before sending. They are exceptions to the grid in `docs/api-surface.md`. `One` with a nil `dst` on a conditional statement fails with a `cql.ValidationError` before sending. |
| `Exec` on a SELECT sent it, paid the read, then failed on the result columns. | `Exec` of a prepared statement whose PREPARE metadata has result columns fails with a `cql.ValidationError` before sending. A SERIAL or LOCAL_SERIAL read is still sent, since it completes in-progress Paxos rounds, and fails on its columns as before. Unprepared statements have no metadata to check. |
| A copied `Batch` or `Worker` was reported by `go vet` only through incidental fields (an `atomic.Bool`, a mutex deep in iterator storage), which a refactor could remove. | `Batch`, `Query` (behind `Worker`) and `StreamQuery` (behind `StreamWorker`) hold a zero-size `noCopy` as their first field; `TestNoCopyVet` runs `go vet` on `testdata/vetcopy` and requires each copy reported through it. |
| `RunConditionalBatch` required a decoder even to read `[applied]` alone. | A nil decoder reads `[applied]` alone, as `cql.CASRows[X](nil)` does. The decoder stays a parameter rather than a variadic, which would collide with the options. |
| `Worker.Close`, `StreamWorker.Close` and `PageLease.Close` returned nothing, unlike every other handle, so none was an `io.Closer`. | They return `error`, always nil. |
| Page sizes were `int32`, so callers converted from `int`, and an out-of-range conversion wrapped silently. | `exec.Options.PageSize` and the `RunPage` page size are `int`, rejected below zero or above `cql.Unpaged` with a `cql.ValidationError`. `cql.Unpaged` is an untyped constant, so it fits both `int` and `config.Profile.PageSize`, which stays `int32` (a config change, left to the config pass). |
| Tests compiled options through three hand-written must-helpers. | `Session.MustCompileOptions`; the root package's `mustCompileOptions` and `bindOptions` helpers and `TestPreparedWith`'s `compile` closure are gone. The integration `bindOptions`, which also compiles internal options, stays. |
| Doc nits: no `ApplyBatch` example, while the `RunConditionalBatch` example only read `Applied`; `migrating-from-v1.md` shadowed `row` in `for row, err := range prepared.All(ctx, params, &row)`; `Session.Close` said "Queries need no Close"; `doc.go` said "Each takes", ambiguous with `Each`; `CASBatchRows.Applied` after `Close` was undocumented; the timestamp guidance led with `exec.Options.Timestamp`. | `ExampleSession_ApplyBatch` replaces the `RunConditionalBatch` example (`ExampleCASBatchRows` reads rows); the migration row uses `&dst`; `Session.Close` says workers stop with the session; `doc.go` says "Every call takes"; `Applied` keeps reporting after `Close` once the end was reached, until the batch is sent again; the statements guide and migration table point to a `USING TIMESTAMP ?` marker first. |

## Ninth review pass (2026-11-27): config, auth, endpoint

| Finding | Change |
|---|---|
| `WithReadTimeout` and `WithWriteTimeout` read as request timeouts, as v1's `Timeout` was, but they drop a connection whose socket stalls; a request's limit is the profile's `WithTimeout`. **Reverses** the first pass's keep of the setter names: the argument was v1's field names, but v1's `ReadTimeout` meant something else too, so the names misled both ways. | `WithReadStallTimeout` and `WithWriteStallTimeout`. Their docs and the `ConfigBuilder` type doc point to `ProfileBuilder.WithTimeout`; the migration table maps v1's `ReadTimeout` and `WriteTimeout` to them and v1's `Timeout` to the profile. |
| `endpoint.Translator` returned a `netip.AddrPort`, so a translator that sends nodes to a load balancer's or a per-node DNS name had to resolve it itself, outside `WithResolver` and `WithDNSTimeout`, and TLS then verified the node's untranslated IP. **Reverses** the first pass's keep of `Translator`: the argument for a `netip.AddrPort` was a typed address the dialer could use as is, but the driver already resolves client route names, and a name is what TLS needs. | `Translate` returns an `endpoint.Address{Host string; Port uint16}`; `endpoint.AddressOf(addr)` leaves an address unchanged and `Address.String` prints `host:port`. A DNS `Host` is resolved on every dial through the session's resolver within its DNS timeout, as client route names are, keeping the address the node's connections were dialed on while the name still resolves to it, so a rotating DNS answer does not supersede connections being opened. The name becomes the default TLS name of the node (of its TLS port's translation when `WithTLSPort` is set); a `ServerName` or a `WithTLSIdentity` identity still takes precedence. `ExampleAddress` shows per-node names. |
| A contact point dialed by a name its certificate carries passed TLS, while every discovered node, the contact node's own pool included, was verified by IP and failed; `Connect` returned the bare `ErrNoConnectionsStarted` and the cause was only in the log. | When no pool opens, `Connect` fails with a `cqlerr.ConnectError` for the "connection pools" stage carrying each pool's last error, which still matches `ErrNoConnectionsStarted`. A handshake whose certificate does not name the IP address it was verified by wraps the `x509.HostnameError` with a message naming `tls.Config.ServerName` and `WithTLSIdentity`. `tls.md` covers the case in its first section, with the three fixes: `ServerName`, a `VerifyName` or `ChainOnly` from `WithTLSIdentity`, or a translator that returns names. |
| `WithTimestampGenerator(func() int64)` made every caller convert to microseconds, and a generator returning nanoseconds or milliseconds wrote timestamps far in the future or past. **Reverses** the first pass's keep of every `ConfigBuilder` setter for this one: the argument was the protocol's unit, but the unit is the driver's concern. | `WithTimestampGenerator(func() time.Time)`; the driver sends `UnixMicro`. nil still sends no client timestamp. |
| `WithDefaultProfile(Profile)` needed `.MustBuild()` or a separate `Build` and error check before the configuration's own `Build`, which validates the profile again. **Reverses** the seventh pass's `WithDefaultProfile(cfg.DefaultProfile().Builder()....MustBuild())`. | `WithDefaultProfile(*ProfileBuilder)` copies the builder, so later calls on it do not reach the configuration, and `ConfigBuilder.Build` validates it, its `cql.ValidationError` naming the profile setter (`WithPageSize`, ...) in `Field`; a zero `ProfileBuilder` fails with `Field` "WithDefaultProfile", and nil restores the default profile. |
| Allowing an authenticator class meant spelling `"org.apache.cassandra.auth.PasswordAuthenticator"`; a typo silently never matched. | `auth.PasswordAuthenticator`, `auth.ScyllaTransitionalAuthenticator` and `auth.ScyllaSaslauthdAuthenticator`, used by the `Password` doc, its tests and `authentication.md`. |
| `examples/streaming` verified a URI identity by turning `InsecureSkipVerify` on and re-verifying the chain in `VerifyConnection`, and set `WithAdvancedShardAwareness(false)`, which `WithConnectionsPerHost` already implies. | It returns `endpoint.TLSIdentity{ChainOnly: true, Verify: ...}` from `WithTLSIdentity`, so the driver verifies the chain, and drops the redundant setter. |
| Five setters (`WithMaxFrameSize`, `WithMaxDecompressedSize`, `WithMaxMetadataDepth`, `WithMaxPagingStates`, `WithMaxPagingStateSize`) set one group of protocol limits that are rarely changed. **Reverses** the first pass's keep of every setter. | `WithLimits(config.Limits{MaxFrameBytes, MaxDecompressedBytes, MaxPagingStateBytes, MaxMetadataDepth, MaxPagingStates})`. A zero field keeps that limit's default, and each call replaces the whole setting. `Build` rejects a negative or too large field with `Field` "WithLimits" and a message naming the field. |
| `TLSFiles.Interval` was a field of the file names, which `LoadTLSFiles` took but ignored (only rejecting a negative one). **Reverses** the first pass's keep of `TLSFiles`: an interval is a property of the reloading source, not of the files. | `NewTLSFileSource(base, files, interval)`; zero means one minute, a negative one is rejected. `TLSFiles` names files only. |
| `Profile.Prefetch` and `exec.Options.Prefetch` were an `int` that only took zero or one, and `WithPrefetch(pages int)` rejected anything else at `Build`. **Reverses** the fourth pass's `WithPrefetch(0)` as the off switch: its argument, no `No*` twin, holds for a bool too, which also says that only two values exist. | `Profile.Prefetch bool`, `ProfileBuilder.WithPrefetch(on bool)` and `exec.Options.Prefetch cql.Option[bool]`. |
| `config.Profile.PageSize` and `WithPageSize` stayed `int32` after the execution pass made the other page sizes `int`. Completes that pass's page size change. | `Profile.PageSize int` and `WithPageSize(rows int)`; a size above `cql.Unpaged` fails validation with `Field` "WithPageSize" instead of wrapping. |
| `WithTLSFallback(fallback bool)` named its parameter after the setter, unlike the other on/off setters. | `WithTLSFallback(on bool)`. |
| `auth.Exchange` said only that a challenge borrows the frame. | The type doc gives the borrowing both ways: a server message passed to `Challenge` or `Success` is valid during the call; a response returned by `Start` or `Challenge` is sent before the exchange is called again and not kept, so an exchange may reuse its buffer but must not change a response before its next call. `Provider.Start` must be safe for concurrent use. |
| `Config.String` left out the connection timeouts, the TLS port and fallback, and whether a translator or client routes were set, the settings asked for first when a session cannot connect. | It prints the connect, DNS, read stall, write stall, heartbeat and metadata timeouts, `tls port`, `tls fallback`, `address translator` (set or not) and the number of client routes; still no credentials, names or TLS material. |
| The 31s default of `WithMetadataTimeout` looked arbitrary. | Its doc says why: one second above the 30s `USING TIMEOUT` that `WithMetadataServerTimeout` derives from it, so on ScyllaDB the server's error, which names the cause, arrives first. |

## Ninth review pass (2026-11-27): errors, retry

| Finding | Change |
|---|---|
| The caller's own context deadline was `KindTimeout` and Transient, as the driver's budget was, so `Kind.Transient` needed a `ctx.Err()` check, `Retryable` special-cased `context.DeadlineExceeded` without `ErrRequestTimeout`, and `metrics.Snapshot.Timeouts` and `AttemptEvent.TimedOut` counted client-side deadlines as server or driver timeouts. **Reverses** the sixth pass's "`ErrRequestTimeout` ... is `KindTimeout`; the caller's own deadline does not match it" (which kept both `KindTimeout`) and the seventh pass's `Timeouts` definition: telling the two apart by sentinel was a second classification beside `Kind`, and an alert on `Timeouts` fired on every client deadline. | `KindOf` reports the caller's deadline (`context.DeadlineExceeded` without `ErrRequestTimeout`) as `KindCanceled`, which is not transient; `KindTimeout` is the server's or the driver's timeout. In a tree holding both, the driver's timeout wins. `Retryable` reads only `Kind` for it. A worker's own `Timeout` now ends its reusable execution context with `ErrRequestTimeout` (it reported a bare `context.DeadlineExceeded`, which the new rule would have called a cancellation), and connection selection reports the context's cause. `metrics.Snapshot.Timeouts` and `AttemptEvent.TimedOut` count only server and driver timeouts; a release note covers the alerting change. |
| An `ExecutionError` unwraps only `Err`, so a request that failed Unavailable and then ran out of its `Timeout` hid the Unavailable from `errors.As`, and an invalid retry decision replaced the attempt's error with a bare `ValidationError`. **Keeps** the sixth pass's `Unwrap` returning `Err` only. | `cqlerr.AttemptAs[T error](err) (T, bool)` searches `err`, then `Attempts` newest first. An unknown `Action`, an invalid `Consistency` or a negative `Delay` fails the request with a `cql.ValidationError` that wraps the attempt's error with `%w`, on the direct, general and stream paths. `KindOf` of such an error is the attempt's, by the existing precedence of server errors over driver errors. |
| `IsCASWriteUnknown` reported only the server's CAS timeout, so an LWT that the driver's timeout, a lost connection or the caller's cancellation ended looked safe to resend. **Keeps** the first pass's `IsCASWriteUnknown`, now documented as the server-reported case with false negatives. | `ExecutionError.StatementKind` records the resolved kind; `cqlerr.ConditionalUnknown(err)` is true for a conditional request whose status is not `NotExecuted`. The errors guide's `MaybeExecuted` row and the server-differences page point to it. |
| A request that could not speculate, being conditional or not idempotent, silently ran without speculation, and a negative speculation delay silently meant "no more", while a negative retry delay fails. **Narrows** the sixth pass's silent speculation gate. | `cql.Result.SpeculationSkipped` (`cql.SpeculationSkip`: `SpeculationSkippedConditional`, `SpeculationSkippedNotIdempotent`) reports the reason on success and in `ExecutionError.Result`. A negative delay fails the execution with a `cql.ValidationError`: before anything is sent when it is the page's first answer, else abandoning the attempts in flight as a context's end does. `Prepared.With` keeps its one-result shape, which chained calls (`p.With(o).Worker()`) and every example use: a view whose own `exec.Options.Speculation` names a policy for a statement that cannot speculate, by the view's `Idempotent`, the statement's and the profile's, records the `cql.ValidationError` once, and every call of the view, `Routing` and `Worker` included, fails with it before sending. A `(view, error)` or `MustWith` form was rejected: it changes every `With` call site for a mistake found on the first call anyway. Speculation from a profile is not checked, since a profile serves statements that cannot speculate by design; those report per call. |
| A retry policy could not tell where the attempt went, which table it touched, or how long the page had taken, and v1's `SimpleSpeculativeExecution` had no counterpart. **Extends** the first pass's keep of `Attempt` and `SpeculationPolicy`. | `retry.Attempt` gains `Target` (`metadata.Target`), `Keyspace`, `Table` and `Elapsed` (since the page's first attempt). `retry.FixedSpeculation{After, Max}` speculates every `After` until `Max` attempts started; the plan's `Delay` field name collides with the `Delay` method, so it is `After`. |
| `cqlerr.Retryable` accepts a rate-limited request and an idempotent write timeout, which `retry.Safe` stops on. | Kept, and documented on both sides (errors and retry-policy guides): `Safe` retries at once on every request, where more work for a rate-limited partition or an overloaded replica only adds load; an application loop can back off first. |
| `Example_errors` and the errors guide retried in a tight loop. | Both sleep before each retry and say why. |
| `exec.SerialOmit` duplicated a profile without `SerialConsistency`, and sent nothing where a conditional request needs a serial level. **Reverses** the first pass's keep of `SerialOverride` and its values for this one value. | Removed; `SerialInherit`, `SerialSerial` and `SerialLocal` remain. |
| `FunctionFailureError` was `KindInvalid`, though the function failed while the server ran the request, not because of the request's form. | `KindServer`, for the type and `CodeFunctionFailure`; its execution status stays `NotExecuted`. |
| `Kind`'s doc did not say which timeout errors reach a caller. | `KindTimeout`'s doc and the errors guide list them: `ErrRequestTimeout`, `ErrTimeoutNoResponse`, the server's read, write and CAS timeouts, and network timeouts. |
| `retry.Speculation.Deadline` duplicated `Speculation.Context.Deadline()`. **Reverses** the eighth pass's `Deadline` field. | Removed; the doc and the speculation guide read the context's deadline. |
| `ExecutionError.Result.Warnings` and `ServerError.Warnings()` both held warnings, with no word on which to read. | The errors guide says `Result.Warnings` is the authoritative copy: it gathers every attempt's and page's warnings, while `ServerError.Warnings()` holds only those of its one response. |

## Ninth review pass (2026-11-27): observability

| Finding | Change |
|---|---|
| An attempt's number restarted on every page, so a multi-page request reported several attempt 1s that nothing told apart, and `routing/attempt.go` called it "within its request". The latency percentiles were recorded as an execution finished, which for `Rows` is when the caller closed them, so they held the application's row processing, and a speculation policy waiting `LatencyP95` waited for the application, not the cluster. **Keeps** the per-page `Attempt` of `CQLTraceRef` and the retry policy, and **reverses** the seventh pass's "latency of the executions that succeeded" for the percentiles. | `Page int` on `observe.AttemptStart`, `routing.AttemptEvent` and `cql.CQLTraceRef`; `Attempt` stays 1-based within the page, and the docs say so. `observe.RequestEvent.Pages` and `metrics.Snapshot.Pages` count pages. The percentiles record each successful page from sending its first attempt to the response that served it, retries and speculation included, on the direct, general and stream paths; `InternalFinish` records nothing. `ExampleJoin` logs the page, `Example_prometheus` and the tracing guide time a request to `RequestEvent.Finished` and say its rows' reading is included. |
| A hook could not see what a request bound, as v1's `ObservedQuery.Values` showed, and copying the values for every request would cost each one an allocation that most hooks never read. | `observe.BoundValues`, a borrowed view over the request's encoded values with `Len`, `At` (the marker's `cql.ColumnSpec` and a `cql.ValueReader`) and `Format(w, redact)`, which prints CQL literals and writes `<redacted>` for the columns `redact` picks. It is `RequestStart.Values`, and each batch statement's `Values`: `RequestStart.BatchStatements` is a `[]observe.BatchStatement` of `Statement` and `Values`. It is valid until the request's end function returns, since the driver releases or reuses the input after it. |
| `cqltrace` reads were ordinary executions: the session's hooks saw them and its metrics counted them, so reading a trace looked like application traffic; and `Fetch` took a bare session ID, dropping the coordinator the `CQLTraceRef` named, which has the session first. **Reverses** the first pass's keep of `Fetch(ctx, s, id)`. | `Fetch(ctx, s, ref cql.CQLTraceRef)` and `Fetcher.Fetch` read from `ref.HostID`, and from any replica once a read there fails; `FetchAll` passes each ref through. The reads run with `NoSessionObserver` and options the session's metrics leave out (`Snapshot`, the percentiles and `HostMetrics`), compiled through an internal link from the root package. |
| `RequestEvent` repeated `ID`, `Statement`, `BatchStatements`, `Parameters` and `Routing` from `RequestStart`, and built a second `[]string` of batch statements for every observed batch. | `RequestEvent` is `{Err, Result, Finished time.Time, Pages}`; what ran is in the `RequestStart` the hook received, so a hook that needs both keeps what it needs in the end function's closure. |
| Only session-wide counters existed, so a slow or failing node showed only in `observe` hooks the application had to aggregate itself. | `Session.HostMetrics(dst []metrics.Host) []metrics.Host` appends one `metrics.Host` per host pool (`HostID`, `Address`, `Attempts`, `Errors`, `Timeouts`, `Connections`, `InFlight`); the counters are atomics on each pool, which an attempt finds through its connection, so counting takes no lock and polling with `hosts[:0]` allocates nothing. |
| An OpenTelemetry span needs `server.address` and `server.port` apart and the session to group by, and a span's page size attribute; hooks had to parse `Address` and had no session or page size. | `AddrPort netip.AddrPort` beside `Address` on `AttemptStart` and `AttemptEvent` (zero for a Unix socket), computed once per connection; `RequestStart.SessionID` (`Session.ID`) and `RequestStart.PageSize` (`cql.Unpaged` for an unpaged request). |
| Tracing a sample of production traffic took setting `exec.Options.CQLTrace` on chosen calls. | `config.Profile.CQLTraceSampler func(routing.Info) bool` and `ProfileBuilder.WithCQLTraceSampler`: asked once per request not already traced, as it is admitted, with a panic logged and the request left untraced. `RequestStart.CQLTrace` reports either source. A func field makes `config.Profile` not comparable with `==`; nothing compared profiles but a test. |
| `exec.PrepareOptions.CQLTrace` traced a PREPARE whose session ID the driver never reported. | Removed; trace a statement's executions instead. |
| `Snapshot.Canceled` and `Timeouts` looked like extra counters beside `ErrorsByKind`, though they repeat two of its entries, and reading one kind from the map took a nil check. **Keeps** the seventh pass's map. | Their doc says they repeat `ErrorsOf(cqlerr.KindCanceled)` and `ErrorsOf(cqlerr.KindTimeout)` and must not be added to it; `Snapshot.ErrorsOf(kind)` reads one kind. An array indexed by kind was rejected: its length would change, breaking the API, whenever a kind is added. |
| Several sessions in one process logged indistinguishable records. | Every record a session logs carries `session`, its ID, which the package doc adds to the stable keys. |
| `Hooks` said only that hooks must be safe for concurrent use, not which goroutine runs them. | The doc says `Request` runs on the caller's goroutine and its end function on the one that ends the request, an attempt's hooks on the goroutine that sends it, and a page's speculative attempts' hooks concurrently, ending possibly after the request. |
| `cql.Result` named the coordinator by host ID only, so logging where a request ran needed the metadata. | `Result.Address`, the host:port of the last completed attempt's connection, beside `HostID`. |

## Ninth review pass (2026-11-27): cql

The ninth pass reviewed the public v2 API with six reviewers and lands in one
change per area. This part covers package `cql`'s types, values and struct
mappings, and `cmd/cqlgen`. Rows marked **reverses** undo a decision of an
earlier pass and give the new argument.

| Finding | Change |
|---|---|
| A UDT value written before `ALTER TYPE ADD` has no cell for the added field, and every UDT field the application leaves unset is NULL, so `UDTCodecOf` failed with `ErrNull` on most real UDT data after a schema change; `AllowMissingFields` did not help, as it concerns metadata. **Reverses** the sixth pass's `ErrNull` for UDT members: a UDT field's NULL is how CQL stores "never set", not a missing column value. | A UDT field decoded by the built-in `Row`/`RowOf` mappings (`UDTCodecOf`, `UDTDecoder`, `StreamUDT`) leaves a NULL or missing field at its zero value; a `Nullable` or pointer field still tells NULL apart. Top-level columns keep `ErrNull`, unless the mapping has `cql.NullAsZero()` (`RowOf`, `StreamRowOf`, `TupleCodecOf`, accepted by `UDTCodecOf`) or the field the `cql:"name,nullzero"` tag. The plan carries the choice per compiled field and skips the cell before the decoder runs, so no decoder changes and the direct ops stay. `TestUDTAddedField` and the integration `TestUDTAlterTypeAdd` cover it. |
| `ParamsOf` bound one field to both markers of `WHERE ck > ? AND ck < ?`, which the server names `ck` after the column, so a range query silently ran with equal bounds. | `Prepare` of a statement whose CQL uses `?` markers rejects a struct mapping that would bind one named field to two markers of one name, with a `CodecError` that suggests `:from` and `:to`. The CQL is scanned once at prepare with the lexer that finds `IF`; a statement with `:name` markers repeats a name on purpose and binds the field each time. `cql.Columns` is unchanged: the check runs beside the compile, which keeps the layout of every type holding `Columns`. Beyond the plan, explicit `Params` with a named `Param` is checked the same way, since it has the same failure; `ParamAt` is not affected. |
| Integer codecs used exact widths, so an `int64` field could not read an `int` column and a schema that widened a column broke every reader. **Reverses** the spec's "Integers use exact widths". | `Int64Codec`/`IntCodec` also decode int, smallint and tinyint, `Int32Codec` smallint and tinyint, and `Int16Codec` tinyint, losslessly; encoding into the narrower column checks the range and fails with a `CodecError`. `Float64Codec` decodes float, and encodes into it rounding to the nearest float32 and rejecting a finite value beyond its range. The streaming integer and float decoders widen alike, and the direct ops read the column's width and store the field's. A narrower codec still rejects a wider column. |
| A UDT struct needed `WithCodec(MustUDTCodecOf[T](...))` in every mapping that used it, and `UDTOptions` was a struct beside the `StructOption`s every other mapping takes. **Reverses** the first pass's keep of `UDTOptions` and `UDTOptionsFor`, the sixth pass's `IgnoreExtraColumns`, and the seventh pass's `IgnoreExtraColumns` setting `AllowExtraDatabaseFields`. | A type with a `CQLCodec() cql.Codec[T]` method, on T or *T, maps with its codec in every struct mapping, as T, *T, []T, map values and every other derived form; reflection uses the codec directly when the package built it and through `reflect` for an application's own codec type, and a `codec=` Go expression on such a field resolves to it under reflection too. `cqlgen -udt=T` generates the method, returning `MustUDTCodecOf[T]()` built once. `UDTOptions` is replaced by options: `UDTName(keyspace, name)`, `UDTNameOf(DataType)` (for `UDTOptionsFor`), `AllowMissingFields()`, and `IgnoreExtraFields()`, which replaces both `IgnoreExtraColumns` and `AllowExtraDatabaseFields`. `UDTCodecOf`, `UDTEncoder`, `UDTDecoder` and `StreamUDT` take `...StructOption` and reject options that do not apply to them. |
| The field table was flat: every V, `Nullable[V]`, `*V`, `[]V`, `BindValue[V]` and a 16×18 product of `map[K]V` were listed, so `[][]int32`, `map[string][]string`, `*Nullable[[]T]`, `Pair` and arrays needed a hand-written codec, and the product cost compile time. **Reverses** the seventh pass's table of built-in map key and value pairs, which `WithCodec` reached only for built-in keys, and its "a type defined from a named type needs a codec" for 16-byte arrays. | A field's codec is derived recursively from leaf codecs over slices, arrays (`[N]T`, exactly N elements; any `[16]byte` maps a uuid or timeuuid like `UUID`), pointers, maps, `Nullable`, `Pair` (a two-member tuple) and, as a field's own type only, `BindValue`. Reflection codes the composite forms with an internal codec over the field's address; leaf fields keep their generic codecs and direct ops. cqlgen applies the same rules and emits nested constructors, such as `cql.GoMapCodec(cql.StringCodec, cql.SliceCodec(cql.Int32Codec))`; its builtins test now covers every leaf and a sample of nested forms against reflection. A `codec=` tag naming a leaf codec applies to the leaves it maps. `WithCodec(c)` maps V wherever a field's type derives from it. |
| `ConvertCodec`'s conversions could not fail, so a type with an invariant had to panic or encode garbage. **Reverses** the seventh pass's `ConvertCodec(base, to, from)` signature, kept from `NamedScalar`. | `ConvertCodec(base, to func(A) (B, error), from func(B) (A, error))`; a conversion error fails the encode or decode as a `CodecError`. Added `ConvertEncoder` and `ConvertDecoder` for one direction. |
| UNSET could be bound only through a `BindValue` literal, so leaving a column unchanged meant a wrapper field per column. | Added `cql.Bind(v)`, `cql.BindNull[T]()` and `cql.BindUnset[T]()`; the `cql:"name,nilunset"` tag and the `cql.NilAsUnset()` `ParamsOf` option bind UNSET for a nil pointer, slice or map instead of NULL or an empty collection. A `nilunset` tag on a field that cannot be nil is an error. |
| `TextCodec` and `BytesCodec` read like the owned `StringCodec` and `ByteSliceCodec`, and `Text.String` hid an expired view's `ErrReleased` behind `"<released>"` in code that meant to extract. **Reverses** the third pass's `Text.String` returning `"<released>"`, and the seventh pass's `BytesCodec` name. | `TextViewCodec` and `BytesViewCodec` name the borrowed views. `Text` has `Format(fmt.State, rune)`, so printing and logging still show the text or `<released>`, and no `String`: `Copy` is the one way to extract it, and it reports the error. |
| `StreamRowOf` had no decoder for `string`, `*T`, maps, `time.Time` or `Decimal`, because every streaming decoder writes into seeded caller storage. **Reverses** the sixth pass's "streaming keeps `cql.Timestamp`". | Added `StreamString`, `StreamPointer`, `StreamGoMap`, `StreamTimestampTime`, `StreamTimeDuration` and `StreamDecimalValue`, which allocate each value and never reuse a buffer between rows, as documented on each; `StreamRowOf` maps `string`, `time.Time`, `time.Duration`, `cql.Decimal` and pointer fields with them. A map field takes `WithStreamDecoder(StreamGoMap(...))`. |
| A `time.Duration` field mapped as an `int64`, a bigint, since `time.Duration` is defined from `int64`. **Reverses** the seventh pass's named-type rule for `time.Duration`. | `TimeDurationCodec` maps a CQL duration to `time.Duration` and is the default for such fields, buffered and streamed (`StreamTimeDuration`); a bigint column is rejected when the plan compiles, and decoding a duration with months or days fails. |
| The zero `time.Time` encoded as the year 1, almost always a field the application forgot to set. | `TimestampTimeCodec` rejects it; a `Nullable[time.Time]` or nil `*time.Time` binds NULL. Rejecting was chosen over encoding an empty value, which CQL tools display inconsistently and which would make the zero value a second NULL. |
| `DateFromTime` did not say which location's date it takes, and the migration guide did not mention millisecond truncation. | `DateFromTime` takes the date in the time's own location; the migration guide says `time.Time` truncates to the millisecond and that a zero time is rejected. |
| `Result.Warnings`, `CQLTraces` and `CustomPayload` returned the result's own slices, so a caller could change another copy's diagnostics. | They return copies. The stream code that marked its own trace refs failed through the returned slice writes them back explicitly. |
| `Value.Signed`, `Float` and `Str` did not say which Go type they return, and `Text` returned a borrowed view where `Str` returned the owned string. **Reverses** the seventh pass's `Value.Str`. | `Int64`, `Float64` and `Text` (an owned string); `TextView` returns the borrowed `Text`. |
| `NullValue(t)` and `UnsetValue(t)` required a type that NULL and UNSET do not encode. | `NullValue()` and `UnsetValue()` have no type and bind to any marker, collection element or member; a decoded NULL keeps its column's type. An unprepared `DynamicParams` statement derives a blob marker for an untyped one, which writes NULL as any type does. |
| CQL date, time, timestamp and duration literals had no parsers, so tests and tools wrote their own. | Added `ParseDate`, `ParseTimeOfDay`, `ParseTimestamp` (date with optional time and zone; a fraction finer than a millisecond is rejected, not truncated) and `ParseDuration` (unit, ISO 8601 and ISO 8601 alternative forms), each the inverse of the type's `String`. |
| `DataType.Fields()` for a tuple returns columns whose names are empty, which a caller could match by name. | Its doc says a tuple member is found by position. |
| The examples spelled statements as `cql.Statement[P, R]{...}` literals, and the spec still used the removed `stream.*` names. | The examples use `cql.Write` and `cql.Select`; the spec names the streaming API as package `cql` exports it (`cql.StreamStatement`, `cql.StreamRowOf`, `cql.WithStreamDecoder`, ...). |
