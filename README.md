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
    Params: cql.OneParam(cql.Int32Codec),
    Rows: cql.OneColumn(cql.StringCodec),
}
prepared, err := session.Prepare(ctx, statement)
if err != nil { return err }
var name string
found, _, err := prepared.One(ctx, int32(42), &name)
if err != nil { return err }
fmt.Println(found, name)
```

`Exec` handles statements with no result columns. Read rows with `One` for a
single row and `Each` to visit every row: both leave nothing to close. `Exec`
and `Each` return `(Result, error)` and `One` returns `(bool, Result, error)`;
the `Result` reports the coordinator, attempts, latency, row count and any
warnings, trace IDs or custom payload. A nil `Params` or
`Rows` means none when the type is `cql.EmptyParams` or `cql.EmptyRow`.
`cql.NewTimeUUID` and its range helpers return `cql.TimeUUID`, which supports
canonical string/JSON/text formatting and timestamp extraction. Use
`cql.TimeUUIDFromUUID` or `cql.ParseTimeUUID` for checked external input.
`cql.StringCodec` decodes text into an owned `string`; `cql.TextCodec` decodes
a `cql.Text` view of the response without copying. `Params`/`Row` compose typed struct mappings, while `DynamicRows` and
`DynamicParams` support metadata-driven applications. Custom codecs compile
against immutable `DataType`/`Columns` metadata; execution does not infer codecs
from arbitrary Go values. See [custom codec contracts](docs/source/custom-codecs.md).

## Ownership and streaming

`prepared.Exec(ctx, input)`, `prepared.One(ctx, input, &dst)` and
`prepared.Each(ctx, input, &dst, fn)` run the statement once. They are safe for
concurrent use and run on queries the statement keeps for them, so they
allocate no per-call execution state. Each takes at most one `CompiledOptions`
from `session.CompileOptions(options)`; none uses the session default. They return
only when the execution has finished. `One` detaches borrowed fields before
returning, so its result remains valid.
`Each` lends borrowed fields only until `fn` returns; clone any values retained
by the callback. `Each` decodes every row into the same `dst`; with
`cql.ReuseBuffers`, or a `RowInto` reset that keeps slices, `[]byte` fields
reuse their capacity, so a slice kept from one row aliases the next row's data:
keep `bytes.Clone(row.Field)`. Return `gocql.StopIteration` to stop `Each` successfully; other
callback errors are returned and recorded as failures.

`prepared.Query(options)` creates a reusable query owned by one worker; any
number of queries can share one preparation. It resolves its options once and
skips the one-shot calls' pool hand-off, which saves about 2 to 4 percent of
client CPU at high concurrency, and it adds the executions whose handles
outlive the call: `Run` returns `Rows`, iterated with `NextInto` and closed by
the caller; and `RunPage` resumes from a saved `PageState` for explicit paging.
The page size `cql.Unpaged` (`Options{PageSize: cql.Some(cql.Unpaged)}`)
requests the whole result in one response.

`session.UnpreparedExec`, `session.UnpreparedOne`, and `session.UnpreparedEach`
handle occasional CQL text executions without leaving a query to close; prepare
a statement that runs repeatedly. Each call copies its parameters during admission. A call while
a `Rows`, `Page` or stream of the query is open, or while another goroutine's
call runs, returns `ErrBusy`; a call after an execution that already returned
waits until that execution's accepted writes drain. `query.Close()`
permanently stops the query and any open handle and returns at once; a call
running in another goroutine is cancelled and returns an error matching
`context.Canceled`. `query.Wait(ctx)`
waits for the drain. A query dropped without `Close` is closed once the
garbage collector finds it unreachable, but close queries explicitly: collection
timing is unspecified.

Buffered rows borrow receive pages. Views expire at the next advancement or
Close unless a retained `PageLease` pins the page. `Clone` explicitly detaches
values. `NextInto` writes typed fields into caller structs while payloads still
borrow their pages.

`session.PrepareStream(ctx, cql.StreamStatement[P, R]{CQL: text, Params: params, Rows: decoder})`
prepares directly for caller-storage decoding. Share its immutable handle and
create a query per worker with `prepared.Query(options)`. The query decodes into
seeded caller storage. It buffers each page on
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
`gocql.NewConfig(hosts...).WithCompression(gocql.LZ4Compression)` without a
compressor import.

## Configuration and extensions

`NewConfig` and `NewProfile` setters store the latest value, including invalid
values. `Build` validates the final settings and reports the first error;
correcting a setting and building again clears that error. Nil optional
providers remove overrides, and zero optional timeouts disable them.

`Connect` snapshots validated configuration before starting workers. Configure
TLS with `crypto/tls`, authentication with `auth.Password` or an `auth.Provider`, and
routing/retry/speculation through `WithDefaultProfile` or a per-query `*Profile`.
Per-execution settings are plain `gocql.Options` values, for example
`gocql.Options{PageSize: cql.Some[int32](100)}`; the zero `Options{}` uses the
session default. Query constructors validate, resolve and encode them for their
session once, so query executions do no per-call work. One-shot executions
(`prepared.Exec/One/Each`, `session.ExecBatch`, `session.RunBatch`) take
`CompiledOptions` from `session.CompileOptions(options)`, which does the same work up
front. Compiled options carry their session's default profile and keyspace; on
another session they are resolved again for that session on each execution.
`endpoint.Dialer`, `endpoint.DNSResolver`,
`endpoint.Translator`, `ReconnectPolicy`, `routing.TokenPartitioner`,
`routing.Policy`, observers and immutable
metadata snapshots provide typed extension points. Host/shard pool sizing,
advanced shard awareness, cancellation and orphan budgets are independently configured.

## Development

```sh
go test -race -timeout=5m ./...
go test -tags integration -run '^$' ./...
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
