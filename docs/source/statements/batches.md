# Batches

Append each typed member with `prepared.AddToBatch(&batch, parameters)` (or
`query.AddToBatch`), then send the batch with
`session.ExecBatch(ctx, &batch)`. Members of several statements of one
session may share a batch, and may write tables of different keyspaces. A
member of another session is rejected with a `*cql.ValidationError`, as is
the statement after `gocql.MaxBatchStatements`:

```go
batch := gocql.Batch{Kind: cql.UnloggedBatch}
for _, r := range rows {
	if err := insert.AddToBatch(&batch, r); err != nil {
		return err
	}
}
if _, err := session.ExecBatch(ctx, &batch); err != nil {
	return err
}
batch.Reset()
```

For an occasional statement, `session.UnpreparedAddToBatch(&batch, statement,
parameters)` adds it without preparing it. Parameters are described by
`Statement.BindMarkers`, as for `session.UnpreparedExec`, and are compiled on
every call, so prepare a statement you add often. An unprepared member gives
the batch no routing token: the batch routes by its first prepared member,
and takes that member's keyspace and table for routing.

`AddToBatch` encodes the parameters straight into storage the batch owns, so
later input mutation cannot change the batch. It uses none of the query's
execution state: `Query.AddToBatch` works while the query runs or has an open
`Rows`, and `Prepared.AddToBatch` takes no pooled query. `ExecBatch` and `RunConditionalBatch` take at most one
`exec.CompiledOptions` from `session.CompileOptions`. A batch response is a single
page, so paging options are ignored and the options you compiled for
statements work for batches too. Compile them once and reuse them, for a
one-off batch as well:

```go
idempotent, err := session.CompileOptions(exec.Options{Idempotent: true})
// ...
_, err = session.ExecBatch(ctx, &batch, idempotent)
```

`batch.Len()` and `batch.Size()` report the statements and the bytes of
encoded values the batch holds; the server's `batch_size_warn_threshold` and
`batch_size_fail_threshold` limit the mutations they make, so check `Size` to
split a large batch before sending it. A batch
may be sent again unchanged. `Reset` empties it and keeps its storage and its
`Kind`, so a reused batch appends without allocating; if a cancelled execution may still be
writing the batch, `Reset` leaves that storage to it. A `Batch` is not safe for
concurrent use.
The batch's `Kind` field selects the native batch kind: `cql.LoggedBatch` (the
zero value), `cql.UnloggedBatch` or `cql.CounterBatch`. The kind follows from
what the batch holds, so it is set once when the batch is built rather than on
each send: a counter batch contains only counter updates, and an unlogged
batch suits members that share a partition. Prepared metadata, routing,
parameter schemas and observation remain typed. A batch speculates only when it
is idempotent and every member is known to be ordinary.

An unlogged or counter batch spanning several partitions is not atomic: the
coordinator writes each partition separately, and Cassandra can reject the
batch as Unavailable or Overloaded after it sent the earlier partitions'
writes. Such a failure therefore reports `cqlerr.MaybeExecuted`, and
`retry.Safe` does not replay the batch unless it is idempotent; see
[retries](../retry-policy.md).

`ExecBatch` rejects a batch known to be conditional with
`cqlerr.ErrConditionalExec` before sending it. Conditional batch results use
`session.RunConditionalBatch(ctx, &batch, decoder)`, which takes a
`cql.CASRows` decoder and returns `CASBatchRows`. Iterate them with
`NextInto`, like the `Rows` of `prepared.Run`, or call `Applied`, which
reports whether the batch applied without you checking every row. Close the
rows when done: until then they hold one of the session's pooled queries.
Once they are closed, sending the batch again with the same decoder reuses
their storage. It rejects a batch whose members are all known not to be
conditional, before sending it. See
[lightweight transactions](lightweight-transactions.md).
The rows of a conditional batch differ between Cassandra and ScyllaDB. See
[Cassandra and ScyllaDB differences](../server-differences.md).
