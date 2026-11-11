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

`AddToBatch` encodes the parameters straight into storage the batch owns, so
later input mutation cannot change the batch. It uses none of the query's
execution state: `Query.AddToBatch` works while the query runs or has an open
`Rows`, and `Prepared.AddToBatch` takes no pooled query. `ExecBatch` and `RunBatch` take at most one
`CompiledOptions` from `session.CompileOptions`. A batch
may be sent again unchanged. `Reset` empties it and keeps its storage, so a
reused batch appends without allocating; if a cancelled execution may still be
writing the batch, `Reset` leaves that storage to it. A `Batch` is not safe for
concurrent use.
`LoggedBatch`, `UnloggedBatch` and `CounterBatch` select native batch kind.
A counter batch contains only counter updates. Prepared metadata, routing,
parameter schemas and observation remain typed. Batches do not speculate.

Conditional batch results use `session.RunBatch(ctx, kind, &batch, decoder)`,
which decodes the rows with `decoder` and returns a page to close. See
[lightweight transactions](lightweight-transactions.md). The rows of a conditional batch
differ between Cassandra and ScyllaDB. See
[Cassandra and ScyllaDB differences](../server-differences.md).
