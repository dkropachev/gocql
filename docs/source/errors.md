# Errors

The driver's errors live in `github.com/scylladb/gocql/v2/cqlerr`. Inspect them
with `errors.Is` and `errors.As`; every error the driver wraps stays reachable.

## Did the request change data?

`cqlerr.ExecutionStatusOf(err)` answers whether replaying a failed request is
safe:

| Status | Meaning |
| --- | --- |
| `NotExecuted` | Nothing was applied: the request never left the driver, or the server rejected it first (Unavailable, Overloaded, Syntax, Invalid, Unauthorized, ...). Retrying cannot duplicate it. An unlogged or counter batch rejected as Unavailable or Overloaded is the exception: the server may have written some of its partitions, so its attempt reports `MaybeExecuted`. |
| `MaybeExecuted` | Unknown: a timeout, a lost connection, or a response that failed to decode. Retry only idempotent requests. An LWT whose outcome is unknown reports this on both Cassandra and ScyllaDB, and `cqlerr.ConditionalUnknown(err)` identifies it, whatever ended it. A SERIAL read is not an LWT and is not reported. See [server differences](server-differences.md). |
| `PartiallyExecuted` | Some replicas applied a write without reaching the requested consistency, an unlogged or counter batch applied some of its statements, or a schema change succeeded but its follow-up failed (a `*cqlerr.SchemaAgreementError` whose `Applied` is set; see below). Treat it as executed for work that is not idempotent: read back what it changed before sending it again. A logged batch that failed after its batch log was written reports `MaybeExecuted` instead: the server replays the batch log until every statement is applied. |
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

`KindTimeout` means the server or the driver gave up waiting. The errors of
that kind a caller sees are:

- `cqlerr.ErrRequestTimeout`: the profile's `Timeout` or `execopt.Options.Timeout`
  (or `PageTimeout`) ran out. It also matches `context.DeadlineExceeded`.
- `cqlerr.ErrTimeoutNoResponse`: an attempt got no response from its
  connection in time.
- The server's `*cqlerr.ReadTimeoutError`, `*cqlerr.WriteTimeoutError` and
  `*cqlerr.CASWriteUnknownError`.
- A network error that reports `Timeout()`, such as a dial timeout.

The caller's own context ending, by cancellation or by its deadline, is
`KindCanceled`, which is not transient: no retry under that context can
succeed. Its error matches `context.Canceled` or `context.DeadlineExceeded`
and never `cqlerr.ErrRequestTimeout`. `metrics.Snapshot.Timeouts` and
`routing.AttemptEvent.TimedOut` count `KindTimeout` only, `Canceled` the
rest.

`cqlerr.Retryable(err)` puts the two questions together: the error is
transient, which rules out the caller's own cancellation or deadline, and the
request was not executed, or is idempotent and not an LWT that may have
applied (`ConditionalUnknown`). The idempotence is the one the driver
resolved for the request (`execopt.Options.Idempotent`, else the
statement's or the profile's), kept in `ExecutionError.Idempotent`, so the
caller does not restate it. It reads the error a call returns; a retry
policy's `retry.Attempt.Err` holds no `ExecutionError`, so a policy calls
`Attempt.Retryable()` instead. A retry loop needs nothing else, but note
that the profile's retry policy has already retried each request:

<!-- snippet
var insert *gocql.Prepared[int32, cql.EmptyRow]
var row int32
-->
```go
for attempt := 0; cqlerr.Retryable(err) && attempt < 3; attempt++ {
	time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	_, err = insert.Exec(ctx, row)
}
```

`Retryable` also accepts errors the default `retry.Safe` stops on, such as a
rate-limited request or an idempotent write timeout: the driver retries at
once, an application loop can back off first, as above (see
[retry policy](retry-policy.md)).

When an error wraps several others, a cancellation decides first, then the
driver's timeout, then the caller's deadline, then a server error, then the
driver's own errors. A `Connect` that
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
`execopt.Options.CQLTrace`, the `CQLTraceID` of the server's trace session. The
error wraps `Err` only, so `errors.Is` and `errors.As` see why the request
stopped, the error `KindOf` classifies; read an earlier attempt's error from
`Attempts`, or find one by type with `cqlerr.AttemptAs`, which searches `err`
and then the attempts, newest first: a request that failed Unavailable and
then ran out of its `Timeout` stops with `ErrRequestTimeout`, and
`cqlerr.AttemptAs[*cqlerr.UnavailableError](err)` still finds the
Unavailable. `cqlerr.AttemptIs(err, target)` is its `errors.Is` counterpart:
`cqlerr.AttemptIs(err, cqlerr.CodeUnavailable)` holds too. A retry decision the driver cannot carry out fails the request
with a `*cql.ValidationError` that wraps the attempt's error the same way.

`failure.StatementKind` is the request's statement kind as the driver
resolved it; a SERIAL or LOCAL_SERIAL read is `OrdinaryStatement`, though
the driver routes it as it routes an LWT. `cqlerr.ConditionalUnknown(err)`
reports a conditional request whose status is not `NotExecuted`: an LWT
that may have applied, whatever ended it, the server's CAS timeout, the
driver's timeout, a lost connection or the caller's cancellation. To tell
which, look for the server's report in every attempt with
`cqlerr.AttemptAs[*cqlerr.CASWriteUnknownError](err)` (Cassandra, protocol
5) or `cqlerr.AttemptAs[*cqlerr.WriteTimeoutError](err)` with `WriteType`
`cqlerr.WriteTypeCAS` (protocol 4 and ScyllaDB).

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
its attempt count and latency. Its `Warnings` are the authoritative copy of
the server's warnings: they gather every attempt's and page's, while
`ServerError.Warnings()` holds only those sent with that one error response.

## Schema agreement

`Session.AwaitSchemaAgreement`, and a schema change statement that waits for
agreement (the default; see `config.ConfigBuilder.WithSchemaChangeWait`),
fail with a `*cqlerr.SchemaAgreementError`. `Versions` holds the schema
version each host reported in the last check and `Unreachable` the hosts it
could not read; `Err` is why: `cqlerr.ErrRequestTimeout` (`KindTimeout`)
when the agreement timeout of `WithSchemaAgreement` ran out, the caller's
context error, a `*cqlerr.NoHostsError`, or a check's or the metadata
refresh's error. A host the host filter accepts counts while it is down, so
one that does not come back holds agreement until the timeout. For a schema
change statement `Applied` is set and the request reports
`PartiallyExecuted`: the server applied the change, so do not send it again
as if it had not.

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
