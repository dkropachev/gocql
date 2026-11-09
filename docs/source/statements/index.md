# Executing statements

`Statement[P,R]` pairs CQL with `ParamEncoder[P]` and `RowDecoder[R]`. A nil
`Params` or `Rows` means none for `cql.EmptyParams` or `cql.EmptyRow`. Prepare
once and share immutable prepared handles. Create one query per worker with
`prepared.Query(options)`, then pass parameters directly to its `Exec`,
`One`, `Each`, `Run`, `RunPage` or `RunUnpaged` methods.
`session.Query(statement, options)` runs a statement without preparing it and takes the parameter columns from `Statement.BindMarkers`. `prepared.StreamQuery(decoder, options)` uses a streaming decoder with page buffering by
default; set `StreamOptions.Incremental` for bounded incremental receive.
Queries reject overlap with `ErrBusy` and own captured parameters through drain.

To execute a prepared statement once from any goroutine, call
`prepared.Exec(ctx, input, options)`, `prepared.One(ctx, input, &dst, options)`
or `prepared.Each(ctx, input, &dst, fn, options)`. They reuse queries the
statement keeps, so point reads and writes cost about what a query per worker
does, and the options apply to that call only. These calls take
`SessionOptions`: bind `Options` to the session once with
`BindOptions(session, options)` and reuse the result, or pass the zero value
for the session default. They return once the execution
has finished; for rows that outlive the call, use a `Query` and `Run`.

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
