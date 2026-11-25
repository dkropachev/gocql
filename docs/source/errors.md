# Errors

The driver's errors live in `github.com/scylladb/gocql/v2/cqlerr`. Inspect them
with `errors.Is` and `errors.As`; every error the driver wraps stays reachable.

## Did the request change data?

`cqlerr.ExecutionStatusOf(err)` answers whether replaying a failed request is
safe:

| Status | Meaning |
| --- | --- |
| `NotExecuted` | Nothing was applied: the request never left the driver, or the server rejected it first (Unavailable, Overloaded, Syntax, Invalid, Unauthorized, ...). Retrying cannot duplicate it. An unlogged or counter batch rejected as Unavailable or Overloaded is the exception: the server may have written some of its partitions, so its attempt reports `MaybeExecuted`. |
| `MaybeExecuted` | Unknown: a timeout, a lost connection, or a response that failed to decode. Retry only idempotent requests. An LWT whose outcome is unknown reports this on both Cassandra and ScyllaDB, and `cqlerr.IsCASWriteUnknown(err)` identifies it. See [server differences](server-differences.md). |
| `PartiallyExecuted` | Some replicas applied a write without reaching the requested consistency, an unlogged or counter batch applied some of its statements, or a schema change succeeded but its follow-up failed. Treat it as executed for work that is not idempotent: read back what it changed before sending it again. A logged batch that failed after its batch log was written reports `MaybeExecuted` instead: the server replays the batch log until every statement is applied. |
| `Executed` | The request succeeded: `ExecutionStatusOf(nil)` returns it. |

A request that ran several attempts reports the most severe status of all of
them, including attempts still in flight when the context ended. A bare
context error from the driver means the request was never sent. Compare a
status with `==` or a `switch`: the constants' numeric order is not their
severity.

## Can trying again help?

`cqlerr.KindOf(err)` groups errors as timeout, unavailable, overloaded, rate
limited, invalid, unauthorized, connection, canceled, closed, protocol or
server. `Kind.Transient` reports whether a later attempt can succeed. It does
not say whether a retry is safe; combine it with the execution status.

`KindTimeout` covers both the driver's own budget and the caller's expired
context. When the profile's `Timeout` or `exec.Options.Timeout` (or
`PageTimeout`) runs out, the error matches `cqlerr.ErrRequestTimeout`; it
also matches `context.DeadlineExceeded`, as the caller's deadline does.

`cqlerr.Retryable(err, idempotent)` puts the two questions together: the error
is transient, it is not the caller's own cancellation or deadline, and the
request was not executed or is idempotent. A retry loop needs nothing else:

<!-- snippet
var insert *gocql.Prepared[int32, cql.EmptyRow]
var row int32
-->
```go
for attempt := 0; cqlerr.Retryable(err, true) && attempt < 3; attempt++ {
	_, err = insert.Exec(ctx, row)
}
```

When an error wraps several others, a cancellation decides first, then a
deadline, then a server error, then the driver's own errors. A `Connect` that
failed because a host rejected the credentials therefore reports
`KindUnauthorized`, not `KindConnection`. Server errors and the driver's error
types also report their own kind with a `Kind` method.

## Server errors

Every server error implements `cqlerr.ServerError`, which is sealed: only
the driver's error types, and types that embed one, implement it. Its `Code`, `Message`
and `Warnings` report the error code, the server's message and the warnings
sent with it. Each is a pointer to a type named after its code:
`*WriteTimeoutError`, `*UnavailableError`, `*RateLimitReachedError`, ..., and
`*OtherServerError` for codes without fields of their own (Overloaded, Syntax,
Invalid, ...). Match a code with `errors.Is(err, cqlerr.CodeOverloaded)`, or
read the fields with `errors.As`:

```go
if timeout, ok := errors.AsType[*cqlerr.WriteTimeoutError](err); ok {
	log.Printf("%d of %d replicas acknowledged", timeout.Received, timeout.BlockFor)
}
```

Every server error also reports its `Kind`, the one `cqlerr.KindOf` gives it.
A read or write failure on protocol 5 maps each failed replica to a
`cqlerr.FailureReason` in its `ErrorMap`, such as
`FailureReasonReadTooManyTombstones`; a code the driver does not name, such
as one of ScyllaDB's own, prints as a number.

Messages start with the code's name, e.g.
`write timeout: Operation timed out ...`.

## Attempts

A failed request returns `*cqlerr.ExecutionError`. `Err` is why it stopped: the
last attempt's error, the caller's cancellation, or a failure before any
attempt. `Attempts` lists every failed attempt with its host address, shard,
consistency, execution status, error and, when the request ran with
`exec.Options.CQLTrace`, the `CQLTraceID` of the server's trace session. The
error wraps `Err` only, so `errors.Is` and `errors.As` see why the request
stopped, the error `KindOf` classifies; read an earlier attempt's error from
`Attempts`. `cqlerr.IsCASWriteUnknown` checks every attempt, so it still
reports an LWT timeout that the caller's cancellation ended.

<!-- snippet
var insert *gocql.Prepared[int32, cql.EmptyRow]
var row int32
-->
```go
_, err := insert.Exec(ctx, row)
if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); ok {
	log.Printf("stopped: %v (%s)", failure.Err, failure.ExecutionStatus())
	for _, a := range failure.Attempts {
		shard := "-"
		if a.Target.ShardID.Set {
			shard = strconv.FormatUint(uint64(a.Target.ShardID.Value), 10)
		}
		log.Printf("attempt %d: host %s (%s) shard %s at %s: %s: %v",
			a.Attempt, a.Target.HostID, a.Address, shard, a.Consistency, a.ExecutionStatus, a.Err)
	}
}
```

The message names the last attempt's address (its host ID when the address is
unknown) and shard, its error, the execution status and up to three earlier
attempts. It never holds a bound value. By default it does not name the
statement either: CQL text can carry literals the application wrote into it,
and error messages often reach logs that are not cleared for application data.
Opt in per session with `config.New(...).WithErrorStatementText(maxBytes)`:
`failure.Statement` then holds the statement's CQL text, and the message adds
it. Text longer than `maxBytes` is cut at a character boundary to at most
`maxBytes` bytes and `...` is appended:

```text
gocql: request: #1 10.0.0.1:9042/shard3: invalid query: unconfigured table t; statement "SELECT v FROM ks.t WHERE k = ?"
```

A prepared statement reports the CQL text it was prepared from. A batch
reports its statement count in `failure.BatchSize` and its first
statement in `failure.Statement`, rendered as
`batch of 3 statements, first "INSERT ..."`. Bound values are left out in
every mode; only literals written into the CQL text itself appear.

`failure.Result` holds what the execution gathered before it failed, such as
its attempt count and latency.

To test code that handles these errors, such as a `retry.Policy` or a
fallback path, build one with `cqlerr.NewExecutionError(err, status,
attempts...)`, and a server error with its type or
`cqlerr.NewOtherServerError(code, message)`. A successful call names its coordinator host
in `Result.HostID`; an `observe.Hooks` attempt hook sees the shard of every attempt in
`AttemptEvent.Target`.

## Connecting

When no host can be reached, `Connect` returns an error matching
`cqlerr.ErrNoConnectionsStarted`. `errors.As` into `*cqlerr.ConnectError` lists
each host tried and its cause, such as a `*net.OpError`. An invalid cluster
config returns a `*cql.ValidationError` naming the field.

## Decoding

A row that does not decode fails with a `*cql.CodecError` naming the column.
NULL in a column whose destination cannot hold it, such as an `int64` field,
matches `cql.ErrNull`; map that column to a `cql.Nullable[T]` field instead.
