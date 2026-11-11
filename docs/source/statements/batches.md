# Batches

Append each typed member with `prepared.AddToBatch(&batch, parameters)` (or
`query.AddToBatch`), then send the batch with
`session.ExecBatch(ctx, kind, &batch)`. Members of several statements of one
session may share a batch:

```go
var batch gocql.Batch
for _, r := range rows {
	if err := insert.AddToBatch(&batch, r); err != nil {
		return err
	}
}
if _, err := session.ExecBatch(ctx, cql.UnloggedBatch, &batch); err != nil {
	return err
}
batch.Reset()
```

For an occasional statement, `session.UnpreparedAddToBatch(&batch, statement,
parameters)` adds it without preparing it. Parameters are described by
`Statement.BindMarkers`, as for `session.UnpreparedExec`, and are compiled on
every call, so prepare a statement you add often. An unprepared member gives
the batch no routing token: the batch routes by its first prepared member.

`AddToBatch` encodes the parameters straight into storage the batch owns, so
later input mutation cannot change the batch. It uses none of the query's
execution state: `Query.AddToBatch` works while the query runs or has an open
`Rows`, and `Prepared.AddToBatch` takes no pooled query. `ExecBatch` and `RunBatch` take at most one
`CompiledOptions` from `session.CompileOptions`. A batch response is a single
page, so paging options are ignored and the options you compiled for
statements work for batches too. A batch
may be sent again unchanged. `Reset` empties it and keeps its storage, so a
reused batch appends without allocating; if a cancelled execution may still be
writing the batch, `Reset` leaves that storage to it. A `Batch` is not safe for
concurrent use.
`LoggedBatch`, `UnloggedBatch` and `CounterBatch` select native batch kind.
A counter batch contains only counter updates. Prepared metadata, routing,
parameter schemas and observation remain typed. Batches do not speculate.

`ExecBatch` rejects a batch known to be conditional with
`cqlerr.ErrConditionalExec` before sending it. Conditional batch results use
`session.RunBatch(ctx, kind, &batch, decoder)`, which returns `Rows` decoded
with `decoder`, like `prepared.Run`. Close the rows when done: until then they
hold one of the session's pooled queries. Once they are closed, sending the
batch again with the same decoder reuses their storage. See
[lightweight transactions](lightweight-transactions.md). The rows of a conditional batch
differ between Cassandra and ScyllaDB. See
[Cassandra and ScyllaDB differences](../server-differences.md).
