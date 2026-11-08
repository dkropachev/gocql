# Batches

Append each typed member with `query.AddToBatch(&batch, parameters)`, then send the
batch with `query.ExecBatch(ctx, kind, &batch, options)`. Any query of the
session can send the batch, and members of several queries may share one batch:

```go
var batch gocql.Batch
for _, r := range rows {
	if err := insert.AddToBatch(&batch, r); err != nil {
		return err
	}
}
if _, err := insert.ExecBatch(ctx, cql.UnloggedBatch, &batch, gocql.SessionOptions{}); err != nil {
	return err
}
batch.Reset()
```

`AddToBatch` encodes the parameters into storage the batch owns, so the query is
free for reuse at once and later input mutation cannot change the batch. A batch
may be sent again unchanged. `Reset` empties it and keeps its storage, so a
reused batch appends without allocating; if a cancelled execution may still be
writing the batch, `Reset` leaves that storage to it. A `Batch` is not safe for
concurrent use.
`LoggedBatch`, `UnloggedBatch` and `CounterBatch` select native batch kind.
A counter batch contains only counter updates. Prepared metadata, routing,
parameter schemas and observation remain typed. Batches do not speculate.

Conditional batch results use `query.RunBatch`, which decodes the rows with the
query's row decoder. See
[lightweight transactions](lightweight-transactions.md).
