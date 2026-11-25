# Executing statements

`Statement[P,R]` pairs CQL with `ParamEncoder[P]` and `RowDecoder[R]`. A nil
`Params` or `Rows` means none for `cql.EmptyParams` or `cql.EmptyRow`. Prepare
once and share immutable prepared handles. To execute a prepared statement
once from any goroutine, call `prepared.Exec(ctx, input)`,
`prepared.One(ctx, input, &dst)`, `prepared.Each(ctx, input, &dst, fn)` or
`prepared.All(ctx, input, &dst)`. `Exec` and `Each` return `(Result, error)`,
`One` returns `(bool, Result, error)`, and `All` returns an iterator for a
range loop that yields each row and, once, any error. They run on queries the statement keeps, and take at
most one `exec.CompiledOptions` for that call: compile `exec.Options` for the session once with
`session.CompileOptions(options)` and reuse the result, or pass none for the
session default. They return once the execution has finished. `One` detaches borrowed fields
before returning, so the row remains usable. `Each` lends borrowed fields only
during the callback; copy or clone fields that escape it. Every row is decoded
into the same `dst`, and a nil `dst` decodes each into a new value instead; with `cql.ReuseBuffers`, or a `RowInto` reset that keeps
slices, `[]byte` fields reuse their capacity, so a slice kept from one row
aliases the next row's data: keep `bytes.Clone(row.Field)`. Return
`gocql.StopIteration` from the callback to finish successfully after enough rows.
A single-error wrapper around StopIteration has the same effect; joined errors
and other callback errors are returned and reported to observers as failures.
`Result.FetchedRows` counts rows fetched in response pages, which may exceed the number
of callback invocations after early stopping.

`prepared.Run(ctx, input)` returns `Rows` for pull iteration and
`prepared.RunPage(ctx, input, state, pageSize)` one `Page` for explicit paging.
Each handle holds one of the statement's pooled queries until it is closed.

For manual cursor lifetimes, explicit paging, or a worker that runs one
statement repeatedly and benefits from skipping the pool hand-off, create one `Worker` per goroutine
with `prepared.Worker()`, or `prepared.Worker(options)` to fix its options; it has the same `Exec`, `One`, `Each`, `All`,
`Run` and `RunPage`.
The `session.Unprepared*` calls run a statement without preparing it and take the
parameter columns from `Statement.BindMarkers`; with `cql.DynamicParams` and
no markers, the markers take the types of the values. To decode into caller storage,
use `session.PrepareStream` with a `cql.StreamStatement[P,R]` and call
`prepared.Worker(streamOptions)`; this checks the streaming decoder directly
without requiring a buffered row mapping. It buffers pages by default; set
`exec.StreamOptions.Incremental` for bounded incremental receive.
Workers reject overlap (an open handle, or another goroutine's call) with
`cqlerr.ErrBusy`, wait for an execution that already returned to drain, and own
captured parameters through drain. `worker.Close()` stops a worker at once, and
`worker.Wait(ctx)` waits for its drain.

`exec.Options` overlays the selected profile using explicit `Option[T]` values for
settings where zero is meaningful. It carries consistency, paging, tracing,
custom payload, idempotence and host/shard targeting. `Statement.Kind` classifies
ordinary and conditional statements; conditional requests send no client
timestamp, keep a fixed replica order and cannot speculate. A statement left
`cql.UnknownStatement` is conditional when its rows decode through
`cql.CASRows` or its CQL is an INSERT, UPDATE, DELETE or BATCH with an `IF`
condition, and ordinary otherwise; one declared `cql.OrdinaryStatement` that
is conditional by that test is rejected before it is sent, as is a SELECT
or other statement that cannot be conditional declared
`cql.ConditionalStatement` or decoded with `cql.CASRows`.
Server-confirmed conditional metadata takes precedence. `Statement.Idempotent`
marks a statement safe to run more than once, for speculation and
`retry.Safe`; prepared statements, their workers and batch members carry it,
and a set `exec.Options.Idempotent` overrides it for one execution.

```{toctree}
:maxdepth: 1

paging
streaming
batches
lightweight-transactions
request-timeouts
```

For occasional statements, `Session.UnpreparedExec`, `UnpreparedOne`, and
`UnpreparedEach` execute typed CQL text and own their cleanup. Parameters must
be described by `Statement.BindMarkers`: a statement with parameters and no
markers fails before it is sent, unless it binds `cql.DynamicParams`. They take at most one plain `exec.Options`,
validated during the call. Frequent statements should be prepared
once and shared. Create workers once per goroutine, rather than once per
HTTP request, and close each one. A worker dropped without `Close` is closed
when the garbage collector finds it unreachable, which may take a while.

For execution options:

- Omit them to use the session defaults.
- Compile recurring overrides once with `session.CompileOptions` and reuse the result.
- Use a request context deadline for a per-request time limit.

Compiled options snapshot explicit overrides and the originating session's defaults.
On another session, the original overrides are resolved against that session's
defaults on every call. Set an explicit profile when you need identical profile
settings across sessions. Policies and observers remain shared objects and must
support concurrent use.

Compiling options snapshots custom payloads and profiles and precomputes request
parameters. Keep recurring compiled options alongside prepared statements rather
than rebuilding them per call. A worker's options remain fixed for its lifetime.
For an occasional request-specific timestamp or payload, compile a fresh options
value and pass it to a one-shot call. Normal default timestamps already vary
per execution; an explicit `USING TIMESTAMP ?` bind marker is another option
for statements whose timestamp comes from application data. Context deadlines
provide request-specific time limits independently of compiled options.
