# gocql v2

gocql provides a typed Go driver for ScyllaDB and Apache Cassandra. Statements
pair explicit parameter encoders with result decoders. Worker-owned queries reuse an owned parameter snapshot across retries and paging.

```sh
go get github.com/scylladb/gocql/v2@v2.0.0
```

The root package `github.com/scylladb/gocql/v2` (`gocql`) connects and runs
statements. The data vocabulary (statements, types, codecs, values,
consistency levels and server errors) lives in `github.com/scylladb/gocql/v2/cql`.
Schema metadata is in `metadata`, host selection in `routing`, authentication
in `auth`, and addressing and dialing in `endpoint`. The repository has one Go
module; compression and benchmarks are included in it.

## Typed execution

```go
import (
    gocql "github.com/scylladb/gocql/v2"
    "github.com/scylladb/gocql/v2/cql"
)

config, err := gocql.NewConfig("127.0.0.1:9042").Build()
if err != nil { return err }
session, err := gocql.Connect(ctx, config)
if err != nil { return err }
defer session.Close()

statement := cql.Statement[int32, string]{
    CQL: "SELECT name FROM app.users WHERE id = ?",
    Params: cql.OneParam(cql.Int32),
    Rows: cql.OneColumn(cql.StringCodec),
}
prepared, err := session.Prepare(ctx, statement)
if err != nil { return err }
query, err := prepared.Query(gocql.Options{})
if err != nil { return err }
defer query.CloseAndWait(ctx)
var name string
found, err := query.One(ctx, int32(42), &name)
if err != nil { return err }
fmt.Println(found, name)
```

`Exec` handles statements with no result columns. Read rows with `One` for a
single row and `Each` to visit every row: both reuse the query's scratch space
and leave nothing to close. `Run` returns `Rows`, iterated with `NextInto` and
closed by the caller; `RunPage` resumes from a saved `PageState` for explicit
paging, and `RunUnpaged` requests one unpaged response. A nil `Params` or
`Rows` means none when the type is `cql.EmptyParams` or `cql.EmptyRow`.
`cql.StringCodec` decodes text into an owned `string`; `cql.TextCodec` decodes
a `cql.Text` view of the response without copying. `Params`/`Row` compose typed struct mappings, while `DynamicRows` and
`DynamicParams` support metadata-driven applications. Custom codecs compile
against immutable `DataType`/`Columns` metadata; execution does not infer codecs
from arbitrary Go values.

## Ownership and streaming

`prepared.Query(options)` creates a reusable query for one worker; any number
of queries can share one preparation. Each `Exec`, `Run`,
`RunPage` or `RunUnpaged` call copies its parameters during admission. Prepared
statements and compiled plans remain shareable. Overlapping executions return
`ErrBusy`; `query.Wait(ctx)` joins accepted writes and callbacks before reuse.
`Close` cancels the active execution and permanently stops the query;
`CloseAndWait(ctx)` closes it and waits for that drain.

For point reads and writes from many goroutines, call the prepared statement
directly. `prepared.Exec(ctx, input, options)`, `prepared.One(ctx, input, &dst,
options)` and `prepared.Each(ctx, input, &dst, fn, options)`, with
`SessionOptions` from `BindOptions` (or the zero value), are safe for
concurrent use and reuse queries the statement keeps, so they cost about the
same as a query per worker. They return only when the execution has finished:
values that borrow response storage are valid until `One` returns or `fn`
returns. Don't keep `Query` values in a `sync.Pool`: a query the pool drops
stays open until it is closed.

Buffered rows borrow receive pages. Views expire at the next advancement or
Close unless a retained `PageLease` pins the page. `Clone` explicitly detaches
values. `NextInto` writes typed fields into caller structs while payloads still
borrow their pages.

`prepared.StreamQuery(decoder, options)` decodes into seeded caller storage. It buffers each page on
normal multiplexed connections by default. `StreamOptions{Incremental: true}`
selects bounded incremental receive with an exclusive connection reservation. Read the
[streaming guide](docs/source/statements/streaming.md) and run the complete
[standalone example](examples/streaming/main.go):

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

The example creates and drops its own keyspace, validates every result, seeds
nested blob buffers, checks backing identity across pages, and exercises pool
query admission and transport drain. It accepts protocol, compression, authentication
and verified TLS options; see `go run ./examples/streaming -h`.

The following table describes explicit **incremental** mode. Page-buffered
queries support the normal protocol/compression combinations and retain memory
proportional to concurrent response pages, within configured frame and
decompression limits. Idle receive caching is bounded by 16 MiB total, 9 MiB
per object and 64 objects per session.

| Streaming mode | Behavior |
| --- | --- |
| Protocol 3/4, raw or TLS, no compression | Incremental receive into caller storage |
| Protocol 5, raw or TLS, no compression | Checked segments with bounded staging |
| Protocol 5, raw or TLS, LZ4 | Bounded segment decompression |
| Protocol 3/4, LZ4 or Snappy frame compression | Explicit `StreamCapabilityError` before streaming submission |

Normal buffered execution supports negotiated LZ4 and Snappy subject to native
protocol rules; protocol 5 compression uses LZ4. Configure
`config.Compression = gocql.LZ4Compression` without a compressor import.

## Configuration and extensions

`Connect` snapshots validated configuration before starting workers. Configure
TLS with `crypto/tls`, authentication with `auth.Password` or an `auth.Provider`, and
routing/retry/speculation through `WithDefaultProfile` or a per-query `*Profile`.
Per-execution settings are plain `gocql.Options` values, for example
`gocql.Options{PageSize: cql.Some[int32](100)}`; the zero `Options{}` uses the
session default. Query constructors validate, resolve and encode them for their
session once, so query executions do no per-call work. One-shot executions
(`prepared.Exec/One/Each`, `query.ExecBatch`, `query.RunBatch`) take `SessionOptions` from
`BindOptions(session, options)`, which does the same work up front. Session
options carry their session's default profile and keyspace and are not checked
against the session that runs them: use them only on the session they were
bound to.
`endpoint.Dialer`, `endpoint.DNSResolver`,
`endpoint.Translator`, `ReconnectPolicy`, `routing.TokenPartitioner`,
`routing.Policy`, observers and immutable
metadata snapshots provide typed extension points. Host/shard pool sizing,
advanced shard awareness, cancellation and orphan budgets are independently configured.

## Development

```sh
go test -race -timeout=5m ./...
go test -tags 'integration gocql_debug' -run '^$' ./...
go test -run '^$' -bench SingleConnection -benchtime=1x ./tests/bench/e2e
```

Replay benchmarks execute ten actual SELECT/INSERT cases using typed-control
recordings; [their recipe](tests/bench/e2e/README.md) explains regeneration. Native
integration requires [owned fixtures](tests/fixtures/README.md). Full native
acceptance and comprehensive performance gates are tracked in
[plan.md](plan.md) and the [execution ledger](feature-specs/execution/STATUS.md).
Earlier source-bound passes do not establish final release verification.

See the [documentation](docs/source/index.md),
[API contract](feature-specs/gocql-v2-api.md), [contribution guide](CONTRIBUTING.md)
and [license](LICENSE).
