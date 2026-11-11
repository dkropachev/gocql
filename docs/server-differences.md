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
  `skipNullPrevious` consumes an all-NULL previous row on an applied ScyllaDB
  response, so `Previous` stays unset and a decoder with non-nullable fields
  does not fail.
- The row plan is chosen per response's column set
  (`InternalResultPlanCache.InternalSelectPlan`), so Cassandra's changing
  metadata compiles one plan per shape. ScyllaDB compiles once.
- A conditional PREPARE may come back with no result columns. Then the decoder
  is checked on the first response (`internal/session/prepared_stream.go:54`,
  `internal/session/row_stream.go:483`).

Open gaps:

- Row mapping compiles with `extra=false` (`internal/codec/rows_codec.go:245`):
  a returned column without an accessor fails. Struct mappings (`RowOf`,
  `MustRowOf`) skip missing fields, but `cql.Row(...)` rejects them too. A
  previous-row decoder that names exactly one server's column set can fail on
  the other. The planned fix is for `CASRows` to compile the previous plan
  leniently: extra columns skipped, missing fields left zero.
- `skipNullPrevious` runs only when `[applied]` is true. A not-applied
  ScyllaDB response for an absent row (for example `UPDATE ... IF v = 1` on a
  missing row) carries an all-NULL previous row, and the driver decodes it into
  `Previous`. A non-nullable field then fails to decode. The fix is to run
  `skipNullPrevious` whatever `[applied]` says. That costs the same prefix scan
  the applied path already pays, and only on conditional results.
- `CASResult.Previous` on an applied row is ScyllaDB-only. It is kept as is,
  because normalising it away would discard ScyllaDB data.

### Batch

| Case | Cassandra | ScyllaDB |
|---|---|---|
| Applied | One row, `[applied]` only | One row per conditional statement, in statement order, with previous values |
| Not applied | One row per CQL row read from the partition, presumably in clustering order (**unverified**), each with `[applied] = false` | One row per conditional statement, in statement order |
| Not applied, partition absent | One row, `[applied]` only | Not documented (**unverified**) |
| Non-conditional members | Not reported separately | Contribute no row |
| Columns | Primary key, then the union of condition columns in statement order. Every column if any member uses `IF EXISTS` / `IF NOT EXISTS` | Union of the condition columns of all conditional statements |

A batch is applied only when all its conditions hold, so on both servers it was
applied if and only if every returned row has `[applied] = true`. ScyllaDB does
not document whether a row's `[applied]` reports the batch or its own
statement, and the rule above holds either way.

The rows map to statements only on ScyllaDB. On Cassandra they map to rows of
the partition.

Driver handling: the same `CASRows` path as a single statement, run through
`Session.RunBatch`. `ExecBatch` rejects result columns after the response
arrives (see the deviations in `docs/api-surface.md`).

Open gaps: the integration test checks only that some row has applied
(`tests/integration/struct_mapping_test.go:170`). Per-server assertions on the
row count, `Previous.Set` on applied rows and the not-applied column set are
missing.

### Prepared metadata

ScyllaDB marks conditional statements in prepared metadata through the
`SCYLLA_LWT_ADD_METADATA_MARK` extension, and the driver reads it as the
statement kind (`internal/endpoint/scylla.go`). Cassandra has no such flag, so
the kind is whatever `Statement.Kind` declares. The driver never parses CQL
to infer it (`internal/cqltype/statement_kind.go`). `Exec` and `ExecBatch`
reject a known conditional request before sending it
(`cqlerr.ErrConditionalExec`). On Cassandra with an undeclared kind, the
request is sent and its result columns are rejected after the response.

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
| LWT outcome unknown | `CAS_WRITE_UNKNOWN` (0x1700) on protocol v5. On v4, `WriteTimeout` with write type `CAS` | `WriteTimeout` with write type `CAS`. ScyllaDB speaks v4 at most | Both map to `MaybeExecuted` (`internal/protocol/errors.go:256`, `writeExecution`) |
| CDC write failure | `CDC_WRITE_FAILURE` (0x1600) | Not sent | Parsed |
| Rate limit | Not sent | `RATE_LIMIT_ERROR` via `SCYLLA_RATE_LIMIT_ERROR`, otherwise `Config_error` | `RejectedByCoordinator` maps to `NotExecuted`, otherwise `MaybeExecuted` |
| Failure reason map | v5 `Read_failure` / `Write_failure` | Added to v4 via `SCYLLA_FAILURE_REASON_MAP` | Parsed |
| Message text | Differs | Differs | Never branch on message text |

## Protocol and topology

These are internal: callers never see them. They are listed so a change to
negotiation or metadata does not drop one.

| Difference | Driver |
|---|---|
| Highest protocol version: Cassandra v5, ScyllaDB v4 plus extensions | Version negotiation |
| Result metadata ID: v5 on Cassandra, v4 via `SCYLLA_USE_METADATA_ID` on ScyllaDB | `internal/transport/conn.go`, `internal/protocol/framer.go` |
| Shard information in SUPPORTED (`SCYLLA_SHARD`, `SCYLLA_NR_SHARDS`, `SCYLLA_SHARD_AWARE_PORT` and others): ScyllaDB only | `internal/endpoint/scylla.go` |
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
- Cassandra CAS result sets: `ModificationStatement.buildCasResultSet` and
  `buildCasFailureResultSet`, and `BatchStatement.makeCasRequest`, in
  https://github.com/apache/cassandra (trunk)
