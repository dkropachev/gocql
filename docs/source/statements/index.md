# Executing statements

`Statement[P,R]` pairs CQL with `ParamEncoder[P]` and `RowDecoder[R]`. A nil
`Params` or `Rows` means none for `cql.EmptyParams` or `cql.EmptyRow`. Prepare
once and share immutable prepared handles. To execute a prepared statement
once from any goroutine, call `prepared.Exec(ctx, input)`,
`prepared.One(ctx, input, &dst)` or `prepared.Each(ctx, input, &dst, fn)`.
`Exec` and `Each` return `(Result, error)`, `One` returns
`(bool, Result, error)`. They run on queries the statement keeps, and take at
most one `BoundOptions` for that call: bind `Options` to the session once with
`session.BindOptions(options)` and reuse the result, or pass none for the
session default. They return once the execution has finished. `One` detaches borrowed fields
before returning, so the row remains usable. `Each` lends borrowed fields only
during the callback; copy or clone fields that escape it. Return
`gocql.StopIteration` from the callback to finish successfully after enough rows.
A single-error wrapper around StopIteration has the same effect; joined errors
and other callback errors are returned and reported to observers as failures.
`Result.Rows` counts rows fetched in response pages, which may exceed the number
of callback invocations after early stopping.

For manual cursor lifetimes, explicit paging, or a worker that runs one
statement repeatedly and benefits from skipping the pool hand-off, create one query per worker
with `prepared.Query(options)`; it has the same `Exec`, `One` and `Each` plus
`Run`, `RunPage` and `RunUnpaged`.
`session.UnpreparedQuery(statement, options)` runs a statement without preparing it and takes the parameter columns from `Statement.BindMarkers`. `prepared.StreamQuery(decoder, options)` uses a streaming decoder with page buffering by
default; set `StreamOptions.Incremental` for bounded incremental receive. For
stream-only statements, use `session.PrepareStream` with a
`cql.StreamStatement[P,R]` and call `prepared.Query(streamOptions)`; this checks
the streaming decoder directly without requiring a buffered row mapping.
Queries reject overlap (an open handle, or another goroutine's call) with
`ErrBusy`, wait for an execution that already returned to drain, and own
captured parameters through drain. `query.Close(ctx)` stops a query and waits
for its drain.

`Options` overlays the selected profile using explicit `Option[T]` values for
settings where zero is meaningful. It carries consistency, paging, tracing,
custom payload, idempotence and host/shard targeting. `Kind` classifies known
ordinary/conditional statements; unknown or conditional requests cannot
speculate. Server-confirmed conditional metadata takes precedence.

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
be described by `Statement.BindMarkers`. Frequent statements should be prepared
once and shared. Create reusable queries once per worker, rather than once per
HTTP request; each remains registered until `Close`, even after execution ends.

For execution options:

- Omit them to use the session defaults.
- Bind recurring overrides once with `session.BindOptions` and reuse the result.
- Use a request context deadline for a per-request time limit.

Bound options snapshot explicit overrides and the originating session's defaults.
On another session, the original overrides are resolved against that session's
defaults on every call. Set an explicit profile when you need identical profile
settings across sessions. Policies and observers remain shared objects and must
support concurrent use.

Binding options snapshots custom payloads and profiles and precomputes request
parameters. Keep recurring bound options alongside prepared statements rather
than rebuilding them per call. A query's options remain fixed for its lifetime.
For an occasional request-specific timestamp or payload, bind a fresh options
value and pass it to a one-shot call. Normal default timestamps already vary
per execution; an explicit `USING TIMESTAMP ?` bind marker is another option
for statements whose timestamp comes from application data. Context deadlines
provide request-specific time limits independently of bound options.
