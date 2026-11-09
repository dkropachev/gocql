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
session default. They return once the execution has finished.

For rows that outlive the call, or to skip the one-shot calls' pool hand-off
in a worker that runs one statement repeatedly, create one query per worker
with `prepared.Query(options)`; it has the same `Exec`, `One` and `Each` plus
`Run`, `RunPage` and `RunUnpaged`.
`session.UnpreparedQuery(statement, options)` runs a statement without preparing it and takes the parameter columns from `Statement.BindMarkers`. `prepared.StreamQuery(decoder, options)` uses a streaming decoder with page buffering by
default; set `StreamOptions.Incremental` for bounded incremental receive.
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
