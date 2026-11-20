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
three) and retries only when replay is safe: it moves to the next host when
the server rejected the request before applying it (Unavailable, Overloaded,
Bootstrapping), retries in place a read timeout that had enough replicas but
no data, moves an idempotent request on after a coordinator server error, and
replays other failures only for idempotent requests or attempts whose status
is `cqlerr.NotExecuted`. Deterministic errors and the caller's cancellation or
deadline never retry. A custom policy can build on `cqlerr.KindOf` and the
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
