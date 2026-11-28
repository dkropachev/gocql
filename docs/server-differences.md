# Cassandra and ScyllaDB: response differences

This is the developer reference for how Apache Cassandra and ScyllaDB answer the
same request differently, and where the driver handles each difference. Read it
before changing result decoding, conditional (LWT) handling, paging, error
classification or protocol negotiation. Users get the user-facing summary in
`docs/source/server-differences.md`. Keep both files in step.

Each entry gives what each server sends, the source, where the driver handles
it, and any open gap. Some facts come from server source or server docs and are
not yet checked against a live cluster. They are marked **unverified**. Add an
integration assertion before relying on one, and drop the mark once it is
checked.

## Dialects

A session is opened for one dialect, a value the caller passes to
`config.New` (`config/dialect.go`): `ScyllaV4`, `CassandraV4` or
`CassandraV5`. A dialect sets two things in the settings, at
`ConfigBuilder.Build` and never again (`settings.DialectSpec` maps each
dialect to them):

- **Protocol version** (`settings.Config.Protocol`). Every connection speaks
  it. There is no negotiation and no fallback: a server that refuses it fails
  the connection. What each version puts on the wire is defined once per
  version in `internal/protocol/version_v4.go` and `version_v5.go`, and code
  asks a connection's `protocol.Version`, never compares version numbers.
- **Server** (`settings.Config.Server`). Opening the session checks the
  control connection against it (`cluster.checkServer`) and fails with
  `cqlerr.ServerMismatchError` on the other database. The zero `ServerAny`
  checks nothing; only the driver's internal tests use it.

Behaviour that depends on the server is decided from these values when a
statement is prepared or a decoder compiled, never per request or per row.
The dialect is not a type parameter: no signature names it, so one
`*Session` type serves every dialect and `Session.Dialect` reports which one a
session speaks (derived from the two settings above). API that only one
dialect supports checks the session's settings and returns an error on
another dialect, before anything is sent. The dialect is part of the
configuration, not a `Connect` argument, so it travels with the rest of the
deployment's settings, `Config.String` reports it, and `Build` rejects a
setting the dialect does not support where it was written.

A method called on a type parameter goes through Go's instantiation
dictionary, which measured 2.5 times slower than the version branch it would
replace. The dialect is only read at `Build`, and hot paths read the plain
values it set.

### Dialect-only features

No package holds dialect-only functions: `scylla.BatchStatement`, the last
one, was replaced by `Session.RunConditionalBatch`, and the `scylla` package
went with it (`docs/package-layout.md`, Dialect packages). Every
dialect-only feature is a setting or a server behaviour, so none can be gated
at compile time. A setting that cannot work on another dialect fails there
before anything is sent; one that has nothing to act on there is a
documented no-op. Settings that the session's dialect alone decides
fail with `cqlerr.UnsupportedSettingError` (kind `KindInvalid`), whose
`Setting` names the feature and whose message names the dialect that supports
it and the session's dialect. `settings.ScyllaOnly` and
`settings.ProtocolV5Only` (`internal/settings/server.go`) build it.
`ScyllaOnly` passes a session of `ServerAny`, which only the driver's tests
open; `ProtocolV5Only` checks the protocol alone. A keyspace override also
depends on the statement, so it is checked when the request is built and fails
with `cql.ValidationError`. A feature the server drives, or that only applies
when the server reports it, is a documented no-op on the other dialect.

| Feature | Supported by | On another dialect | Where |
|---|---|---|---|
| `config.ConfigBuilder.WithClientRoutes` | ScyllaV4 | `ConfigBuilder.Build` fails | `settings.Config.Validate` |
| `config.ConfigBuilder.WithMetadataServerTimeout`, positive (`USING TIMEOUT`) | ScyllaV4 | `ConfigBuilder.Build` fails; zero is accepted | `settings.Config.Validate`; `internal/transport/metadata_execution.go` still rejects it per query for a `ServerAny` session |
| `execopt.Options.ShardID` | ScyllaV4 | `Session.CompileOptions` fails; options compiled for another session fail on execution | `checkDialectOptions` in `NewCompiledOptions` and `admit` (`internal/session`) |
| `execopt.Options.NowInSeconds` | CassandraV5 | As `ShardID` | As `ShardID` |
| `execopt.Options.Keyspace` / `execopt.PrepareOptions.Keyspace` other than the session's | CassandraV5 | Fails each attempt or the PREPARE before it is sent, unless the statement names its keyspace (`ValidationError`) | `newAttempt` and `prepareTyped` (`internal/session/execute.go`) |
| Shard awareness: `WithConnectionsPerShard`, `WithMaxExcessPerShard` | ScyllaV4 | Apply per node: a Cassandra node counts as one shard | Pool |
| Shard-aware port: `WithAdvancedShardAwareness`, `WithShardAwareSourcePorts` | ScyllaV4 | No-op: Cassandra has no shard-aware port | Pool |
| Tablet routing, `routing.Options.NoLeaderAware`, `metadata.Keyspace.TabletBased`, `InitialTablets` and `ConsistencyMode` | ScyllaV4 | No-op: Cassandra sends no tablet hints and has no tablet keyspaces | `internal/tablets` |
| LWT routing (conditional and SERIAL statements keep replica order, unshuffled) | Both | Not dialect-only: ScyllaDB marks conditional statements in prepared metadata, and on Cassandra the declared `Statement.Kind` does | `internal/routing/policy.go` |
| `routing.CDCTokenPartitioner`, `config.ConfigBuilder.WithPartitioner` | ScyllaV4 for CDC log tables | No-op: chosen by a table's partitioner class, which no Cassandra table names | Metadata |
| `cqlerr.RateLimitReachedError` | ScyllaV4 | Never returned: Cassandra does not send it | Error parsing |
| `event.KindClientRoute` events | ScyllaV4 | Never delivered | Events |
| Shard and shard-aware port outputs (`endpoint.DialTarget.ShardID`, `endpoint.Peer.ShardAwarePort`, `endpoint.ConvictionEvent.ShardID`, `metadata.HostSnapshot.Shards` and `ShardingIgnoreMSB`, `metadata.Route.ShardID`, `routing.Info.ShardID` when no option sets it, `cqlerr.AttemptFailure.ShardID`, `cqlerr.ErrScyllaSourcePortAlreadyInUse`) | ScyllaV4 | Unset, zero or never returned; `metadata.Route.ShardOf` returns shard 0, since a Cassandra node counts as one shard | Pool and metadata |
| A custom `routing.Policy` route or target with a shard (`metadata.Route.WithShardID`, `metadata.Target.ShardID`) | ScyllaV4 | A shard the node lacks, any but 0 on Cassandra, finds no connection: the plan moves to its next route, and with none left the execution fails with `cqlerr.ErrNoConnections`, as for a ScyllaDB node with fewer shards | `internal/pool/pool.go`, `internal/cluster/execute.go` |

## Conditional statements (LWT)

Every conditional response is a ROWS result whose first column is `[applied]`
(boolean). Where the two servers agree, nothing more is said below.

### Single statement

| Case | Cassandra | ScyllaDB |
|---|---|---|
| Applied | `[applied]` only | `[applied]` and the previous values of the returned columns; NULLs when there was no previous row |
| Not applied | `[applied]` and the current values of the condition columns, with no primary key | `[applied]` and the previous values of the returned columns |
| Not applied, row absent | One row, `[applied]` only, and the metadata holds that column alone | Same columns as the applied case, all NULL |
| Columns, `IF EXISTS` / `IF NOT EXISTS` | Every column of the table | Every column of the table |
| Columns, other conditions | Condition columns, in `IF` order | Primary key and condition columns per the docs, but the docs' examples show condition columns only (**unverified**) |
| Metadata between executions | Changes with the outcome | The same for every execution |

Driver handling:

- `CASRows` (`internal/codec/result.go`) requires `[applied]` first. It decodes
  the rest into `CASResult.Previous` only when previous columns are present.
  `skipNullPrevious` consumes an all-NULL previous row whatever the outcome,
  so `Previous` stays unset and a decoder with non-nullable fields does not
  fail. ScyllaDB sends one when there was no previous row. All-NULL columns
  on an existing row (Cassandra's condition columns only, for example) also
  read as unset.
- A field-mapped previous decoder (`cql.Row`, `RowOf`, `MustRowOf`) compiles
  leniently (`rowCodec.compileLenient`): a column without a field is skipped
  and a field without a column stays zero, so one decoder fits both servers'
  column sets. Other decoders compile strictly, as before.
- The row plan is chosen per response's column set
  (`InternalResultPlanCache.InternalSelectPlan`), so Cassandra's changing
  metadata compiles one plan per shape. ScyllaDB compiles once.
- A conditional PREPARE may come back with no result columns. Then the decoder
  is checked on the first response (`internal/session/prepared_stream.go:54`,
  `internal/session/row_stream.go:483`).

Open gaps:

- `CASResult.Previous` on an applied row is ScyllaDB-only. It is kept as is,
  because normalising it away would discard ScyllaDB data.

### Batch

| Case | Cassandra | ScyllaDB |
|---|---|---|
| Applied | One row, `[applied]` only | One row per statement, in statement order, with previous values |
| Not applied | One row per CQL row read from the partition, presumably in clustering order (**unverified**), each with `[applied] = false` | One row per statement, in statement order |
| Not applied, partition absent | One row, `[applied]` only | One row per statement, every column but `[applied]` NULL |
| Non-conditional members | Not reported separately | One row each, with the batch's `[applied]` and previous values looked up as for any member, from a read that skips the member unless its update reads the row |
| Columns | Primary key, then the union of condition columns in statement order. Every column if any member uses `IF EXISTS` / `IF NOT EXISTS` | `[applied]`, the primary key, then the union of the members' condition columns, in schema order. Every column if any member uses `IF EXISTS` / `IF NOT EXISTS` (`batch_statement::build_cas_result_set_metadata`) |

A batch is applied only when all its conditions hold, so on both servers it was
applied if and only if every returned row has `[applied] = true`. On ScyllaDB
every row carries the same batch-wide flag
(`cas_request::build_cas_result_set`), and the server stops evaluating
conditions at the first that fails (`cas_request::applies_to`), so there is no
per-statement applied flag. What is per statement is the previous values. Row
i holds every result column (the batch-wide set above) taken from the row
ScyllaDB found for statement i (`cas_request::find_old_row`): its clustering
row if the batch's read contains it, and otherwise, unless the statement is
`IF EXISTS` / `IF NOT EXISTS`, the partition's static row if the read
returned one, with the partition key set and the clustering columns NULL. The
read returns a static row only for a table with static columns whose
partition it found (`prefetch_data_builder::accept_partition_end`). Every
column but `[applied]` is NULL when no row was found. One read serves the
whole batch (`cas_request::read_command`): it skips the clustering range of a
member whose update does not read the row and that has no condition or only
conditions on static columns, so whether such a member's own row is found
depends on the rest of the batch and on the partition.
The driver does not parse CQL, so working out which condition failed means
evaluating the conditional members' previous values in the application.

The rows map to statements only on ScyllaDB. On Cassandra they map to rows of
the partition.

Driver handling:

- The same `CASRows` path as a single statement, run through
  `Session.RunConditionalBatch`, which returns `CASBatchRows`: rows that
  iterate like `Rows`, plus `Applied`, which applies the rule above
  (`internal/session/cas_batch.go`). `ExecBatch` rejects a batch known
  to be conditional before sending it (`cqlerr.ErrConditionalExec`), and
  `RunConditionalBatch` one known to have no conditional member, whose VOID
  result has no `[applied]`.
- On ScyllaDB row i is member i, so the caller's row counter is the member
  index; the driver adds no mapping API. The `CASBatchRows` and
  `RunConditionalBatch` docs state the rule and that it does not hold on Cassandra. An all-NULL
  previous row, which ScyllaDB sends when no row was found, decodes as no
  previous row. A found row carries at least its partition key, so it decodes
  as one, with NULL clustering columns when it is the static row.

`tests/integration/lwt_dialect_test.go` asserts the shapes per dialect: the
row count of applied and not-applied batches, `Previous.Set` on applied
single statements, the previous value of a not-applied one, and no previous
row for a missing one. On ScyllaDB (2026.3.6) it also asserts that every row
of a not-applied batch, the ordinary member's included, has `[applied] =
false`, and that `RunConditionalBatch`'s `Applied` agrees with the rows on
both servers. It passes against ScyllaDB 2026.3.6 and Cassandra 5.0.9. It
asserts the ScyllaDB batch row counts exactly, but only that Cassandra's
not-applied batch has rows. No test covers a batch on an absent partition:
those rows rest on the server sources cited below. The remaining
**unverified** marks are the order of Cassandra's not-applied rows and the
columns of single statements with other conditions on ScyllaDB.

### Prepared metadata

ScyllaDB marks conditional statements in prepared metadata through the
`SCYLLA_LWT_ADD_METADATA_MARK` extension, and the driver reads it as the
statement kind (`internal/transport/supported.go`). Cassandra has no such
flag. On both servers the driver infers an undeclared kind from the
statement (`internal/binding/statement.go`): a `cql.CASRows` decoder, or an
INSERT, UPDATE, DELETE or BATCH with an `IF` outside literals and comments,
is conditional, and anything else ordinary. A statement other than an
INSERT, UPDATE, DELETE or BATCH that is declared conditional or decoded with
`cql.CASRows` is rejected before it is sent. `Prepare` checks a `CASRows`
decoder's leading `[applied]` column against ScyllaDB's prepared metadata,
and fails when ScyllaDB reports the statement is not a lightweight
transaction; Cassandra omits that metadata, so the check waits for EXECUTE.
`Exec` and `ExecBatch` reject a conditional request before sending it
(`cqlerr.ErrConditionalExec`).
A statement executed with SERIAL or LOCAL_SERIAL consistency is a Paxos read,
not a conditional statement: `Exec` does not reject it up front, and fails on
its result columns like any other row-returning statement.

## Paging

ScyllaDB cuts a page at a built-in 1 MB size limit as well as at the requested
row count, so a page can hold fewer rows than the page size. The last page can
also be empty. Cassandra pages are normally full. Both servers say whether more
pages remain through the paging state, not through the row count.

Driver handling: every many-rows path loops on `state.More()`, never on row
count (`internal/session/each.go:118`, `internal/session/result.go:313`,
`internal/session/row_stream.go:398`). Keep it that way. A loop that stops on
an empty or short page drops rows on ScyllaDB. `RunPage` callers get the empty
page as it is, and the user docs tell them to check `More()`.

## Errors

| Case | Cassandra | ScyllaDB | Driver |
|---|---|---|---|
| LWT outcome unknown | `CAS_WRITE_UNKNOWN` (0x1700) on protocol v5. On v4, `WriteTimeout` with write type `CAS` | `WriteTimeout` with write type `CAS`. ScyllaDB speaks v4 at most | Both map to `MaybeExecuted` (`internal/protocol/errors.go:426`, `writeExecution`) and `cqlerr.KindTimeout`, and `cqlerr.IsCASWriteUnknown` matches both |
| CDC write failure | `CDC_WRITE_FAILURE` (0x1600) | Not sent | Parsed |
| Rate limit | Not sent | `RATE_LIMIT_ERROR` via `SCYLLA_RATE_LIMIT_ERROR`, otherwise `Config_error` | `RejectedByCoordinator` maps to `NotExecuted`, otherwise `MaybeExecuted` |
| Failure reason map | v5 `Read_failure` / `Write_failure` | Added to v4 via `SCYLLA_FAILURE_REASON_MAP` | Parsed |
| Message text | Differs | Differs | Never branch on message text |

## Protocol and topology

These are internal: callers never see them. They are listed so a change to
negotiation or metadata does not drop one.

| Difference | Driver |
|---|---|
| Partitioner: ScyllaDB only Murmur3; Cassandra Murmur3 by default, `RandomPartitioner` or `ByteOrderedPartitioner` if configured | Tokens are `int64` (`metadata.HostSnapshot.Tokens`, `routing.TokenPartitioner`); a host token that does not parse fails opening the session (`internal/endpoint/snapshot.go`, `TestRandomPartitionerClusterIsRejected`) |
| Highest protocol version: Cassandra v5, ScyllaDB v4 plus extensions | The dialect fixes the version (`CassandraV5`, `ScyllaV4`); nothing is negotiated |
| Result metadata ID: v5 on Cassandra, v4 via `SCYLLA_USE_METADATA_ID` on ScyllaDB | `internal/transport/conn.go`, `internal/protocol/framer.go` |
| Shard information in SUPPORTED (`SCYLLA_SHARD`, `SCYLLA_NR_SHARDS`, `SCYLLA_SHARD_AWARE_PORT` and others): ScyllaDB only | `internal/transport/supported.go` |
| Tablet routing hint in the custom payload (`tablets-routing-v1`): ScyllaDB only | `internal/tablets/hint_parser.go` |
| `CLIENT_ROUTES_CHANGE` event: ScyllaDB only | Client routes |
| Peer table: `system.peers_v2` on Cassandra 4.0+, `system.peers` only on ScyllaDB | `internal/transport/dial.go` |
| Schema tables: each server has its own extra tables and options | Metadata parsing must tolerate unknown columns |

## Sources

- ScyllaDB LWT result sets:
  https://docs.scylladb.com/manual/stable/features/lwt.html
- ScyllaDB LWT differences from Cassandra:
  https://opensource.docs.scylladb.com/stable/kb/lwt-differences.html
- ScyllaDB paging: https://github.com/scylladb/scylladb/blob/master/docs/dev/paged-queries.md
- ScyllaDB protocol extensions:
  https://github.com/scylladb/scylladb/blob/master/docs/dev/protocol-extensions.md
- ScyllaDB CAS result sets: `cas_request::read_command`, `applies_to`,
  `find_old_row` and `build_cas_result_set`,
  `batch_statement::build_cas_result_set_metadata`, and the static-row entry
  of `prefetch_data_builder::accept_partition_end`:
  https://github.com/scylladb/scylladb/blob/3df4785b3e439072c5cb11318f3726bdad47c01a/cql3/statements/cas_request.cc#L60-L80
  https://github.com/scylladb/scylladb/blob/3df4785b3e439072c5cb11318f3726bdad47c01a/cql3/statements/cas_request.cc#L100-L112
  https://github.com/scylladb/scylladb/blob/3df4785b3e439072c5cb11318f3726bdad47c01a/cql3/statements/cas_request.cc#L124-L155
  https://github.com/scylladb/scylladb/blob/3df4785b3e439072c5cb11318f3726bdad47c01a/cql3/statements/cas_request.cc#L157-L215
  https://github.com/scylladb/scylladb/blob/3df4785b3e439072c5cb11318f3726bdad47c01a/cql3/statements/batch_statement.cc#L192-L220
  https://github.com/scylladb/scylladb/blob/3df4785b3e439072c5cb11318f3726bdad47c01a/cql3/update_parameters.cc#L156-L178
- Cassandra CAS result sets: `ModificationStatement.buildCasResultSet` and
  `buildCasFailureResultSet`, and `BatchStatement.makeCasRequest`, in
  https://github.com/apache/cassandra (trunk)
