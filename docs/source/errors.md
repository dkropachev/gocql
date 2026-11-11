# Errors

The driver's errors live in `github.com/scylladb/gocql/v2/cqlerr`. Inspect them
with `errors.Is` and `errors.As`; every error the driver wraps stays reachable.

## Did the request change data?

`cqlerr.ExecutionStatusOf(err)` answers whether replaying a failed request is
safe:

| Status | Meaning |
| --- | --- |
| `NotExecuted` | Nothing was applied: the request never left the driver, or the server rejected it first (Unavailable, Overloaded, Syntax, Invalid, Unauthorized, ...). Retrying cannot duplicate it. |
| `MaybeExecuted` | Unknown: a timeout, a lost connection, or a response that failed to decode. Retry only idempotent requests. An LWT whose outcome is unknown reports this on both Cassandra and ScyllaDB. See [server differences](server-differences.md). |
| `PartiallyExecuted` | Some replicas applied a write without reaching the requested consistency, or a schema change succeeded but its follow-up failed. |

A request that ran several attempts reports the most severe status of all of
them, including attempts still in flight when the context ended. A bare
context error from the driver means the request was never sent.

## Can trying again help?

`cqlerr.KindOf(err)` groups errors as timeout, unavailable, overloaded, rate
limited, invalid, unauthorized, connection, canceled, closed, protocol or
server. `Kind.Transient` reports whether a later attempt can succeed. It does
not say whether a retry is safe; combine it with the execution status.

## Server errors

Every server error implements `cqlerr.RequestError` and is a pointer to a type
named after its code: `*RequestErrWriteTimeout`, `*RequestErrUnavailable`, and
`*RequestErrServer` for codes without fields of their own. Match a code with
`errors.Is(err, cqlerr.ErrServerOverloaded)`, or read the fields with
`errors.As`. Messages start with the code's name, e.g.
`write timeout: Operation timed out ...`.

## Attempts

A failed request returns `*cqlerr.ExecutionError`. `Err` is why it stopped: the
last attempt's error, the caller's cancellation, or a failure before any
attempt. `Attempts` lists every failed attempt with its host address, shard,
consistency, execution status and error; `errors.As` searches all of them, so a
server error from an earlier attempt is found even after a timeout.

```go
_, err := insert.Exec(ctx, row)
if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); ok {
	log.Printf("stopped: %v (%s)", failure.Err, failure.ExecutionStatus())
	for _, a := range failure.Attempts {
		shard := "-"
		if a.Shard.Set {
			shard = strconv.FormatUint(uint64(a.Shard.Value), 10)
		}
		log.Printf("attempt %d: host %s (%s) shard %s at %s: %s: %v",
			a.Number, a.HostID, a.Address, shard, a.Consistency, a.Status, a.Err)
	}
}
```

`failure.Result` holds what the execution gathered before it failed, such as
its attempt count and latency. A successful call names its coordinator host
in `Result.HostID`; an `Observer` sees the shard of every attempt in
`AttemptEvent.Target`.

## Connecting

When no host can be reached, `Connect` returns an error matching
`cqlerr.ErrNoConnectionsStarted`. `errors.As` into `*cqlerr.ConnectError` lists
each host tried and its cause, such as a `*net.OpError`. An invalid cluster
config returns a `*cqlerr.ValidationError` naming the field.
