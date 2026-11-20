# Retry policy

`Profile.Retry` is a `retry.Policy`: `Decide(retry.Attempt) retry.Decision`.
The default profile uses `retry.Safe{MaxAttempts: 3}`; set another with
`ProfileBuilder.WithRetry`. `retry.Attempt` carries the failed attempt's error,
1-based number, idempotence (`exec.Options.Idempotent`), consistency, whether
it is conditional, and `ExecutionStatus`, which reports whether the attempt
may have changed data (see [Errors](errors.md)). A `retry.Decision` selects
`retry.Stop` (the zero value), `retry.Same` (the same host), `retry.Next` (the
next host of the plan) or `retry.IgnoreWrite` (treat the failed write as
successful), with an optional `Consistency` for the next attempt and a
`Delay` before it; `Result.AchievedConsistency` reports the consistency that
succeeded. `Decide` runs on the request goroutine and must not block.

`exec.Options.Retry == nil` inherits the selected profile's retry policy. Use
`exec.Options{Retry: retry.Never{}}` to stop after the first failed attempt without
replacing other profile settings. `Profile.Retry == nil` disables retries in a
complete profile; `retry.Never{}` can express that choice there too.

`retry.Safe` bounds attempts (`MaxAttempts`, the first included; zero means
three) and retries only when replay is safe. Among server errors, it moves to
the next host on Unavailable, Overloaded and Bootstrapping, which it treats as
not executed; retries in place a read timeout that had enough replicas but no
data; and moves an idempotent request on after a coordinator server error
(`cqlerr.CodeServer`). Every other server error stops, idempotent or not:
write timeouts, read timeouts with data or too few replicas, read and write
failures, and deterministic errors. A failure that is not a server error, such
as a lost connection, is replayed on the next host only for an idempotent
request or an attempt whose status is `cqlerr.NotExecuted`. The caller's
cancellation or deadline never retries. A custom policy can build on `cqlerr.KindOf` and the
attempt's `ExecutionStatus`; `retry.PolicyFunc` adapts a function:

```go
policy := retry.PolicyFunc(func(a retry.Attempt) retry.Decision {
	if a.Number < 5 && cqlerr.KindOf(a.Err) == cqlerr.KindOverloaded {
		return retry.Decision{Action: retry.Next, Delay: 100 * time.Millisecond}
	}
	return retry.Safe{}.Decide(a)
})
```

Retries reuse encoded binding inputs and compiled result plans. They do not
invoke application encoders again. Streaming may retry only before any
application output begins and never replays a delivered row. Each retry runs
within the execution's deadline (see
[deadlines](statements/request-timeouts.md)).

Request observers see one terminal request event and one per started attempt.
Callbacks are synchronous and must be prompt; disabled observation avoids event
construction.
