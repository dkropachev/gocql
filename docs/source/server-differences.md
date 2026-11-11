# Cassandra and ScyllaDB differences

The driver works with both Apache Cassandra and ScyllaDB. Most responses are the
same on both, and the driver absorbs most of the differences. The ones below
change what your code sees, so write code that works for both.

## Lightweight transactions

Decode conditional results with `cql.CASRows(decoder)` into
`cql.CASResult[R]`. `Applied` holds the `[applied]` column. `Previous` holds
the row values the server returned, when it returned any.

What the server returns for a single conditional statement:

| Outcome | Cassandra | ScyllaDB |
|---|---|---|
| Applied | `Applied` only. `Previous` is unset | The values from before the write in `Previous`. `Previous` is unset when the row did not exist |
| Not applied | The current values of the condition columns in `Previous`. Every column for `IF EXISTS` / `IF NOT EXISTS` | The values from before the write in `Previous` |
| Not applied, row absent | `Applied` only. `Previous` is unset | Every returned column is NULL. The driver decodes them into `Previous`, so a field that cannot hold NULL fails to decode |

Write portable code this way:

- Decide on `Applied`. Treat `Previous` as extra information: on an applied
  statement only ScyllaDB fills it.
- Decode `Previous` with a struct mapping (`cql.MustRowOf[T]()`) whose struct
  has a field for every column the statement can return. The two servers return
  different column sets, and a struct mapping leaves fields without a returned
  column at their zero value. A hand-written `cql.Row(...)` must match the
  returned columns exactly, so it can fail on one server and not the other.

## Conditional batches

`session.RunBatch` returns the rows of a conditional batch:

| Outcome | Cassandra | ScyllaDB |
|---|---|---|
| Applied | One row, with `Applied` true | One row per conditional statement, in statement order |
| Not applied | One row per table row the conditions read, with `Applied` false | One row per conditional statement, in statement order |

A batch is applied only when all its conditions hold. The portable test is: the
batch was applied if and only if every row reports `Applied`. Matching rows to
statements by position works on ScyllaDB only.

## Paging

A page can hold fewer rows than the page size, or no rows, while more pages
remain. ScyllaDB also cuts pages at a size limit. `Each`, `All`, `Run` and
streaming already handle this. With `RunPage`, decide whether to continue with
`page.State().More()`, never with the number of rows on the page.

## Unknown LWT outcome

When a conditional write times out, the server cannot say whether it was
applied. Cassandra reports this as `CAS_WRITE_UNKNOWN` on protocol v5, and as a
write timeout of type `CAS` on v4. ScyllaDB reports a write timeout of type
`CAS`. In every case `cqlerr.ExecutionStatusOf(err)` returns `MaybeExecuted`.
Read the row back before retrying a non-idempotent conditional write, and use
`cqlerr.ExecutionStatusOf(err)` instead of matching a specific error, so the
check works on both servers.

## Server-specific errors

ScyllaDB can reject a request with a rate-limit error
(`cqlerr.KindOf(err)` reports rate limited). Cassandra never sends one.
Error message text differs between the servers, so match errors with
`errors.Is`, `errors.As` and `cqlerr`, never with their messages. See
[errors](errors.md).
